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

require 'bundler/setup'
require 'redis'
require 'hiredis'
require 'time'

$redis_proxy_test_libdir ||= File.expand_path(File.dirname(__FILE__))
$redis_proxy_path ||= File.dirname(File.dirname($redis_proxy_test_libdir))

class RedisClusterProxy

    include RedisProxyTestLogger

    attr_reader :cluster, :entry_point, :pid, :logfile

    def initialize(cluster, port: 7777, **opts)
        @cluster = cluster
        @port = port
        @cluster.start if !@cluster.instances
        @entry_point = @cluster.instances.first
        @options = opts
        @cmdpath = File.join($redis_proxy_path, 'src/redis-cluster-proxy')
        if !File.exists?(@cmdpath) && !File.symlink?(@cmdpath)
            @cluster.destroy!
            STDERR.puts ("Could not find redis-cluster-proxy in:\n'" + 
                         @cmdpath + "'\nCompile redis-cluster-proxy "+
                         "before making tests!").red
            exit 1
        end
        @redis_paths = find_redis!
        @redis_cli = @redis_paths['redis-cli']
        ts = Time.now.strftime('%Y%m%d-%H%M%S')
        @logfile = File.join(RedisProxyTestCase::LOGDIR,
                             "redis-cluster-proxy-#{ts}.log")
    end

    def start
        if !@cluster.is_instance_running?(@entry_point)
            STDERR.puts "Cluster is not running!".red
            return false
        end
        if !@cluster.nodes || @cluster.nodes.length == 0
            @cluster.create_cluster
            if !@cluster.nodes || @cluster.nodes.length == 0
                STDERR.puts "Cluster is not a cluster!".red
                return false
            end
        end
        cmdopts = ''
        if @options.length > 0
            cmdopts = ' ' + @options.to_a.map{|o|
                opt, val = o
                opt = opt.to_s.gsub('_', '-')
                if opt.length == 1
                    opt = "-#{opt}"
                else
                    opt = "--#{opt}"
                end
                if val == true
                    opt
                else
                    "#{opt} #{val}"
                end
            }.join(' ')
        end
        entry_port = @entry_point[:port]
        cmd = "#{@cmdpath} -p #{@port}#{cmdopts} " + 
              "127.0.0.1:#{entry_port}"
        log "Starting proxy to 127.0.0.1:#{entry_port}...", :gray
        STDOUT.flush
        @pid = Process.spawn cmd, out: @logfile, err: @logfile
        $test_proxies ||= []
        $test_proxies |= [self]
        loop do
            `#{@redis_cli} -p #{entry_port} ping`
            break if $?.success?
            sleep(1)
        end
        threads = (@options[:threads] || 8).to_i
        sleep(threads / 2)
        log "Proxy started with PID #{@pid}", :gray
        @pid
    end

    def stop
        if @pid
            log "Stopping proxy to 127.0.0.1:#{@entry_point[:port]} " +
                "with PID #{@pid}", :gray
            Process.kill('TERM', @pid) 
            $test_proxies ||= []
            $test_proxies -= [self]
            @pid = nil
        end
    end

    def redis
        @redis ||= Redis.new(port: @port)
    end

    def redis_command(command, *args)
        begin
            redis.send(command.to_sym, *args)
        rescue Redis::CommandError => cmderr
            cmderr
        end
    end

    def method_missing(name, *args, &block)
        if block
            redis.send(name, *args, &block)
        else
            redis.send(name, *args)
        end
    end

end
