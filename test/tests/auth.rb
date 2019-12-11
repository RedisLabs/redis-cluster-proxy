require 'redis'
require 'hiredis'

redis_cli = nil

$authpassw = '123'

setup {
    use_valgrind = $options[:valgrind] == true
    loglevel = $options[:log_level] || 'debug'
    dump_queues = $options[:dump_queues]
    dump_queries = $options[:dump_queries]
    @aux_cluster = RedisCluster.new passw: $authpassw
    @aux_cluster.restart
    @aux_proxy = RedisClusterProxy.new @aux_cluster,
                                       log_level: loglevel,
                                       dump_queries: dump_queries,
                                       dump_queues: dump_queues,
                                       valgrind: use_valgrind,
                                       auth: $authpassw,
                                       verbose: true
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
$numkeys = 50
$numclients = $options[:clients] || 10
$move_slot_every = 10
$hash_key_every = 3

if $options[:max_keys] && $numkeys > $options[:max_keys]
    $numkeys = $options[:max_keys]
    $numlists = $numkeys / 10
end



test "SET #{$numkeys} keys (clients=#{$numclients})" do
    spawn_clients($numclients, proxy: $aux_proxy){|client, idx|
        (0...$numkeys).each{|n|
            log_test_update "key #{n + 1}/#{$numkeys}"
            val = n.to_s
            key = "k:#{n}"
            reply = redis_command client, :set, key, val
            assert_not_redis_err(reply)
        }
        log_same_line ''
    }
end

test "GET #{$numkeys} keys (clients=#{$numclients}, multiplex=off)" do
    spawn_clients($numclients, proxy: $aux_proxy){|client, idx|
        expected = ['OK']
        keys = (0...$numkeys).map{|n|
            key = "k:#{n}"
            val = n.to_s
            expected << val
            key
        }
        begin
            reply = client.pipelined{
                client.proxy 'multiplexing', 'off'
                keys.each{|k|
                    client.get k
                }
            }
        rescue Redis::CommandError => cmderr
            reply = cmderr
        end
        assert_not_redis_err(reply)
        assert_equal(expected, reply)
        log_same_line ''
    }
end
