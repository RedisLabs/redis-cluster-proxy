require 'redis'
require 'hiredis'

redis_cli = nil

setup {
    #RedisProxyTestCase::GenericSetup.call
    use_valgrind = $options[:valgrind] == true
    loglevel = $options[:log_level] || 'debug'
    dump_queues = $options[:dump_queues]
    dump_queries = $options[:dump_queries]
    @aux_cluster = RedisCluster.new
    @aux_cluster.restart
    @aux_proxy = RedisClusterProxy.new @aux_cluster,
                                       log_level: loglevel,
                                       dump_queries: dump_queries,
                                       dump_queues: dump_queues,
                                       valgrind: use_valgrind
    @aux_proxy.start
    $aux_cluster, $aux_proxy = @aux_cluster, @aux_proxy
    $source_node, $target_node = @aux_cluster.masters[0, 2]
    assert($source_node != nil, "Failed to find a source master node")
    assert($target_node != nil, "Failed to find a target master node")
    $slots = $source_node[:slots]
    assert($slots != nil, "Source slots is nil")
    $slots = $slots.keys.dup
    assert($slots.length > 0, "Source slots is empty")
    redis_cli = $main_cluster.instance_eval{@redis_cli}
}

cleanup {
    @aux_proxy.stop
    @aux_proxy = nil
    @aux_cluster.stop
    @aux_cluster = nil
    $aux_cluster = nil
    $aux_proxy = nil
}

$options ||= {}
$numkeys = 60
$numclients = $options[:clients] || 10
$datalen = [1]#, 4096]
$move_slot_every = 10
$hash_key_every = 3

if $options[:max_keys] && $numkeys > $options[:max_keys]
    $numkeys = $options[:max_keys]
    $numlists = $numkeys / 10
end

update_key_mutex = Mutex.new
update_moved_mutex = Mutex.new
living_threads_mutex = Mutex.new
$living_threads = -1

def key_for_num(n)
    if (n % $hash_key_every) == 0
        slot = (n / $move_slot_every)# - 1
        return "k:#{n}" if slot < 0 || slot >= CRC16_SLOT_TABLE.length
        hash = CRC16_SLOT_TABLE[slot]
        "k:{#{hash}}:#{n}"
    else
        "k:#{n}"
    end
end

def check_cluster(node, log: nil)
    srcaddr = "#{node[:ip]}:#{node[:port]}"
    redis_cli = $aux_cluster.instance_eval{@redis_cli}
    check = `#{redis_cli} --cluster check #{srcaddr}`
    ok = $?.success?
    if !ok
        if log
            check = check.split(/\n+/).select{|l|
                l['[WARN'] || l['[ERR']
            }.join("\n")
            log << check
        end
    end
    ok
end

$datalen.each_with_index{|len, lidx|

    test "SET #{$numkeys} keys (size=#{len}b, clients=#{$numclients})" do
        spawn_clients($numclients, proxy: $aux_proxy){|client, idx|
            (0...$numkeys).each{|n|
                log_test_update "key #{n + 1}/#{$numkeys}"
                val = n.to_s * len
                key = key_for_num n
                reply = redis_command client, :set, key, val
                assert_not_redis_err(reply)
            }
            log_same_line ''
        }
    end

    test "MOVE SLOTS, GET #{$numkeys} keys (s=#{len}b, c=#{$numclients})" do
        $biggest_key = 0
        $moved = 0
        spawn_clients($numclients + 1, proxy: $aux_proxy){|client, idx|
            if idx == 0
                loop do
                    living = nil
                    living_threads_mutex.synchronize{
                        living = $living_threads
                    }
                    break if living == 0
                    next if living < 0
                    key_n = nil
                    update_key_mutex.synchronize{
                        key_n = $biggest_key
                    }
                    break if (key_n + 1) >= $numkeys
                    #do_move = (n && n > 0 && (n % $move_slot_every) == 0)
                    do_move = (key_n && (key_n % $move_slot_every) == 0)
                    break if $slots.length.zero?
                    next if !do_move
                    slot = $slots.shift
                    break if !slot
                    update_moved_mutex.synchronize{
                        log_same_line ''
                        log_test_update "moving slot #{slot}..."
                        res = {}
                        moved = $aux_cluster.move_slot slot, $target_node,
                                                       results: res,
                                                       update: false

                        assert(moved,
                            "Failed to move slot #{slot}: #{res[:error]}")
                        $moved += 1
                        check = ''
                        ok = check_cluster $source_node, log: check
                        if !ok
                            retries = 50
                            check = ''
                            while !(ok = check_cluster($source_node, log: check))
                                break if retries <= 0
                                sleep 0.2
                                retries -= 1
                                check = ''
                            end
                        end
                        assert(ok, "Cluster is broken:\n#{check}")
                        log_same_line ''
                    }
                end
            else
                living_threads_mutex.synchronize{
                    $living_threads = 0 if $living_threads < 0
                    $living_threads += 1
                }
                begin
                    (0...$numkeys).each{|n|
                        biggest = n
                        update_key_mutex.synchronize{
                            $biggest_key = n if n > $biggest_key
                            biggest = $biggest_key
                        }
                        break if (biggest + 1) >= $numkeys
                        moved = 0
                        update_moved_mutex.synchronize{
                            moved = $moved
                        }
                        #log_test_update "key #{n+1}<#{biggest + 1}/#{$numkeys} " +
                        #                "(moved slots: #{moved})"
                        log_test_update "key #{biggest + 1}/#{$numkeys} " +
                                        "(moved slots: #{moved})"
                        val = n.to_s * len
                        key = key_for_num n
                        reply = redis_command client, :get, key
                        assert_not_redis_err(reply)
                        assert_equal(reply.to_s, val)
                    }
                rescue Exception => e
                    living_threads_mutex.synchronize{$living_threads-=1}
                    raise e
                end
                living_threads_mutex.synchronize{$living_threads-=1}
                log_same_line ''
            end
        }
    end

}
