# Copyright (C) 2019  Giuseppe Fabio Nicotra <artix2 at gmail dot com>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published
# by the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

require 'fileutils'

$redis_proxy_test_libdir ||= File.expand_path(File.dirname(__FILE__))
load File.join($redis_proxy_test_libdir, 'crc16.rb')

class RedisCluster

    include RedisProxyTestLogger

    RedisClusterHashSlots = 16384
    DefaultNodeTimeout = 200000000
    MigrateDefaultPipeline = 10
    MigrateDefaultTimeout = 60000

    attr_reader :port, :dirs, :instances, :nodes, :node_by_id, :node_by_port,
                :node_for_slot, :masters
    attr_accessor :verbose

    def initialize(masters_count: 3, replicas: 1,
                   node_timeout: DefaultNodeTimeout, verbose: true, passw: nil)
        @masters_count = masters_count
        @replicas_count = replicas
        @node_timeout = node_timeout
        @verbose = verbose
        @num_instances = @masters_count + (@replicas_count * @masters_count)
        @ports = find_available_ports(18000, @num_instances)
        @passw = passw
        if @ports.length < @num_instances
            raise "Could not find available ports from 18000 for the cluster!"
        end
        @start_port = @ports[0]
        @tmppath = File.join(RedisProxyTestCase::TMPDIR,
                             "redis-cluster-test-#{urand2hex(6)}")
        @redis_paths = find_redis!
        %w(server cli).each{|progname|
            progname = "redis-#{progname}"
            if !@redis_paths[progname]
                err = colorized "Cannot find #{progname} on your machine!",
                                :red
                STDERR.puts err
                exit 1
            end
        }
        @redis_server = @redis_paths['redis-server']
        @redis_cli = @redis_paths['redis-cli']
    end

    def build_dirs
        @instances = @ports.map{|port|
            path = File.join(@tmppath, port.to_s)
            FileUtils.mkdir_p(path)
            cfgfile = File.join(path, 'redis.conf')
            conf = config_for port, path
            File.open(cfgfile, 'w'){|f| f.write(conf)}
            {
                port: port,
                path: path,
                conf: cfgfile
            }
        }
    end

    def start
        build_dirs if !@instances || !File.exists?(@tmppath)
        if @verbose
            log("Starting #{@instances.length} cluster nodes...", :gray)
        end
        @instances.each{|instance|
            next if is_instance_running?(instance)
            start_instance instance
        }
        $test_clusters ||= []
        $test_clusters |= [self]
        @instances
    end

    def stop
        return if !@instances
        if @verbose
            log("Stopping #{@instances.length} cluster nodes...", :gray)
        end
        @instances.each{|instance|
            next if !is_instance_running?(instance)
            stop_instance instance
        }
    end

    def restart(reset: false)
        stop
        reset! if reset
        start
    end

    def reset!
        stop if is_instance_running?(@instances.first)
        return if !@instances || !File.exists?(@instances[0][:path])
        @instances.each{|instance|
            path = instance[:path]
            %w(dump.rdb nodes.conf).each{|file|
                file = File.join path, file
                FileUtils.rm file if File.exists?(file)
            }
        }
    end

    def destroy!
        stop
        FileUtils.rm_r(@tmppath) if File.exists?(@tmppath)
    end

    def start_instance(instance, quiet: false, fatal: true, timeout: 10)
        port = instance[:port]
        script = %Q(cd #{instance[:path]} && #{@redis_server} ./redis.conf)
        print("Starting cluster node #{port}...".gray) if @verbose
        `#{script}`
        if !$?.success?
            raise "Failed to start node #{port}"
        end
        now = Time.now.to_i
        while !is_instance_running?(port)
            if (Time.now.to_i - now) > timeout
                err = "Instance #{port} could not be started!"
                STDERR.puts "\n#{err}".red
                if fatal
                    destroy!
                    raise err
                else
                    return
                end
            end
            if !quiet && @verbose
                print '.'
                STDOUT.flush
            end
        end
        puts "\n" if !quiet && @verbose
    end

    def stop_instance(instance, save: false, quiet: false, timeout: 10,
                      fatal: true)
        port = instance[:port]
        save_action = (save ? 'save' : 'nosave')
        cmd = "#{redis_cli_cmd(port)} shutdown #{save_action}"
        print("Stopping cluster node #{port}...".gray) if @verbose
        `#{cmd}`
        while is_instance_running?(port)
            if (Time.now.to_i - now) > timeout
                err = "Instance #{port} could not be stopped!"
                STDERR.puts "\n#{err}".red
                if fatal
                    destroy!
                    raise err
                end
            end
            if !quiet && @verbose
                print '.'
                STDOUT.flush
            end
        end
        puts "\n" if !quiet && @verbose
    end

    def stop_random_instance(only_masters: false, stop_replicas: false,
                             quiet: false, fatal: true)
        _nodes = (only_masters ? @masters : @nodes)
        idx = (rand() * 1000000) % _nodes.length
        node = _nodes[idx]
        if stop_replicas && node[:replicate] == nil
            node_replicas(node).each{|replica|
                stop_instance(replica, quiet: quiet, fatal: fatal)
            }
        end
        stop_instance(node, quiet: quiet, fatal: fatal)
        node
    end

    def stop_random_master(stop_replicas: false, quiet: false)
        stop_random_instance(only_masters: true, stop_replicas: false,
                             quiet: quiet, fatal: false)
    end

    def is_instance_running?(instance)
        if instance.is_a? Hash
            port = instance[:port]
        elsif instance.is_a? Integer
            port = instance
        end
        `#{redis_cli_cmd(port)} ping 2>/dev/null`.strip.downcase == 'pong'
    end

    def create_cluster
        if @nodes && @nodes.length > 0
            restart reset: true
        else
            restart if !@instances || !is_instance_running?(@instances.first)
        end
        redis_cli_version = `#{@redis_cli} -v`.strip
        match = redis_cli_version.match(/\d+\.\d+\.\d+/)
        if !match
            err = colorize("Cannot determine redis-cli version")
            STDERR.puts err
            destroy!
            exit 1
        end
        log "Creating cluster with #{@instances.length} instances...", :gray
        redis_cli_version = match[0]
        if !@passw
            authopt = ''
        else
            authopt = " -a #{@passw}"
        end
        if redis_cli_version.split('.')[0].to_i >= 5
            cmd = "#{@redis_cli}#{authopt} --cluster create " +
            @instances.map{|instance|
                "127.0.0.1:#{instance[:port]}"
            }.join(' ') + " --cluster-replicas #{@replicas_count}"
            ok = shell_exec cmd, auto_answer: :yes, return_value: :status
            if !ok
                err = "Failed to start cluster!".red
                STDERR.puts err
                destroy!
                exit 1
            end
        else
            #TODO: use redis-trib
            raise "Not supported: redis-trib!"
        end
        res = update_cluster
        log "Cluster created with #{@masters.length} masters:"
        log @masters.map{|n| " - #{n[:port]}"}.join("\n")
        res
    end

    def update_cluster(instance = nil)
        instance ||= @instances.first
        @nodes = []
        @node_by_id = {}
        @node_by_port = {}
        @node_for_slot = []
        @masters = []
        friends = []
        @nodes << get_node_info(instance, friends)
        friends.each{|friend|
            @nodes << get_node_info(friend)
        }
        @nodes.each{|node|
            @node_by_id[node[:id]] = node
            @node_by_port[node[:port]] = node
            @masters << node if !node[:replicate]
            node[:slots].keys.each{|slot|
                slot = slot.to_i if !slot.is_a?(Numeric)
                @node_for_slot[slot] = node
            }
        }
    end

    def node_replicas(node)
        return [] if node[:replicate] || !node[:id]
        replicas = node[:replicas]
        return replicas if replicas
        node[:replicas] = @nodes.select{|n|
            n[:replicate] == node[:id]
        }
    end

    def node_for_key(key)
        slot = RedisCluster::slot_for_key(key)
        @node_for_slot[slot]
    end

    def get_node_info(instance, friends = nil)
        reply = redis_command instance, "cluster nodes"
        if !$?.success?
            err = "Failed to execute 'cluster nodes' on "+
                  "127.0.0.1:#{@instances[0][:port]}".red
            STDERR.puts err
            STDERR.puts reply.yellow
            destroy!
            exit 1
        end
        node = nil
        lines = reply.split(/\n+/)
        lines.each{|l|
            l = l.strip.split
            name,addr,flags,master_id,ping_sent,ping_recv,
            config_epoch,link_status = l[0..6]
            slots = l[8..-1]
            info = {
                name: name,
                addr: addr,
                flags: flags.split(","),
                replicate: master_id,
                ping_sent: ping_sent.to_i,
                ping_recv: ping_recv.to_i,
                link_status: link_status,
            }
            info[:replicate] = nil if master_id == "-"
            if addr
                addr, ibus = addr.split('@')
                ip, port = addr.split(':')
                info[:ip] = ip
                info[:port] = port.to_i
                info[:internal_bus_port] = (ibus ? ibus.to_i : nil)
            end
            info[:myself] = info[:flags].include?("myself")
            next if !info[:myself] && !friends
            info[:slots] ||= {}
            info[:migrating] ||= {}
            info[:importing] ||= {}
            if info[:myself]
                slots.each{|s|
                    if s[0..0] == '['
                        if s.index("->-") # Migrating
                            slot,dst = s[1..-1].split("->-")
                            info[:migrating][slot.to_i] = dst
                        elsif s.index("-<-") # Importing
                            slot,src = s[1..-1].split("-<-")
                            info[:importing][slot.to_i] = src
                        end
                    elsif s.index("-")
                        start,stop = s.split("-")
                        ((start.to_i)..(stop.to_i)).each{|slot| 
                            info[:slots][slot] = true
                        }
                    else
                        info[:slots][(s.to_i)] = true
                    end
                } if slots
                node = info
            elsif friends
                friends << info
            end
        }
        instance = @instances.find{|inst|
            inst[:port] != nil && inst[:port] == node[:port]
        }
        node[:path] = instance[:path] if instance
        node
    end

    def redis_cli_cmd(port)
        rcli = "#{@redis_cli} -p #{port}"
        if @passw
            rcli << " -a #{@passw}"
        end
        rcli
    end

    def redis_command(instance, command)
        if instance.is_a? Integer
            port = instance
        else
            port = instance[:port]
        end
        `#{redis_cli_cmd(port)} #{command}`
    end

    def config_for(port, path)
        cfg =
        "port #{port}\n" +
        "cluster-enabled yes\n" +
        "cluster-config-file \"nodes.conf\"\n" +
        "logfile ./redis.log\n" +
        "cluster-node-timeout #{@node_timeout}\n" +
        #"unixsocket \"#{File.join(path, 'redis.sock')}\"\n" +
        "daemonize yes\n" +
        "dir \"#{path}\"\n"
        if @passw && !@passw.strip.empty?
            cfg << "requirepass #{@passw}\n"
            cfg << "masterauth #{@passw}\n"
        end
        cfg
    end

    def move_slot(slot, target, pipeline: nil, timeout: nil, update: true,
                  results: nil)
        pipeline ||= MigrateDefaultPipeline
        timeout ||= MigrateDefaultTimeout
        if (source = @node_for_slot[slot.to_i])
            if !target
                @masters.each{|master|
                    next if master == source
                    target = master
                    break
                }
                if !target
                    results[:error] = "Failed to find a target node" if results
                    return false
                end
            end
            if !source[:name] || !target[:name]
                results[:error] = "Missing name in source or target" if results
                return false
            end
            res = redis_command target,
                "cluster setslot #{slot} importing #{source[:name]}"
            if res.strip.index('(error)') == 0
                results[:error] = "SETSLOT (importing): #{res.strip}" if results
                return false
            end
            res = redis_command source,
                "cluster setslot #{slot} migrating #{target[:name]}"
            if res.strip.index('(error)') == 0
                results[:error] = "SETSLOT (migrating): #{res.strip}" if results
                return false
            end
            migrated = 0
            while true
                res = redis_command source,
                    "cluster getkeysinslot #{slot} #{pipeline}"
                if res.strip.index('(error)') == 0
                    results[:error] = "GETKEYSINSLOT: #{res.strip}" if results
                    return false
                end
                keys = res.split
                break if keys.length == 0
                res = redis_command source,
                    "migrate #{target[:ip]} #{target[:port]} \"\" 0 " +
                    "#{timeout} keys #{keys.join(' ')}"
                if res.strip.index('(error)') == 0
                    results[:error] = "MIGRATE: #{res.strip}" if results
                    return false
                end
                migrated += keys.length
            end
            @masters.each{|master|
                res = redis_command master,
                        "cluster setslot #{slot} node #{target[:name]}"
                if res.strip.index('(error)') == 0
                    results[:error] = "SETSLOT (owner): #{res.strip}" if results
                    return false
                end
            }
            if update
                update_cluster
            end
            true
        else
            if results
                results[:error] = "Failed to find source for slot: #{slot}"
            end
            false
        end
    end

    def RedisCluster::slot_for_key(key)
        s = key.index "{"
        if s
            e = key.index "}",s+1
            if e && e != s+1
                key = key[s+1..e-1]
            end
        end
        RedisClusterCRC16.crc16(key) % RedisClusterHashSlots
    end

end
