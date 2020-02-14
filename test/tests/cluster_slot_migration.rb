require 'redis'
require 'hiredis'

setup {
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
    @source_node, @target_node = @aux_cluster.masters[0, 2]
    assert(@source_node != nil, "Failed to find a source master node")
    assert(@target_node != nil, "Failed to find a target master node")
    @slot = 1 # TODO
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
$numclients = $options[:clients] || 10
$numkeys = $options[:keys] || 60

def key_for_slot(slot, n)
    hash = CRC16_SLOT_TABLE[slot]
    "k:{#{hash}}:#{n}"
end

def validate_getting_keys()
    test "Read existing keys from slot #{@slot} (clients=#{$numclients})" do
        spawn_clients($numclients, proxy: $aux_proxy){|client, idx|
            (0...$numkeys).each{|n|
                log_test_update "key #{n + 1}/#{$numkeys}"
                val = n.to_s
                key = key_for_slot @slot, n
                reply = redis_command client, :get, key
                assert_not_redis_err(reply)
                assert_equal(reply.to_s, val)
            }
            log_same_line ''
        }
    end

    test "Read bogus keys from slot #{@slot} (clients=#{$numclients})" do
        spawn_clients($numclients, proxy: $aux_proxy){|client, idx|
            (0...$numkeys).each{|n|
                log_test_update "key #{n + 1}/#{$numkeys}"
                key = key_for_slot @slot, (n+$numkeys)
                reply = redis_command client, :get, key
                assert_not_redis_err(reply)
                assert_equal(reply.to_s, "")
            }
            log_same_line ''
        }
    end
end

test "SET #{$numkeys} keys (clients=#{$numclients})" do
    spawn_clients($numclients, proxy: $aux_proxy){|client, idx|
        (0...$numkeys).each{|n|
            log_test_update "key #{n + 1}/#{$numkeys}"
            val = n.to_s
            key = key_for_slot @slot, n
            reply = redis_command client, :set, key, val
            assert_not_redis_err(reply)
        }
        log_same_line ''
    }
end

validate_getting_keys()

test "Set slot #{@slot} to be IMPORTING on target node" do
    reply = @aux_cluster.redis_command @target_node,
        "cluster setslot #{@slot} importing #{@source_node[:name]}"
    assert_not_redis_err(reply)
end

validate_getting_keys()

test "Set slot #{@slot} to be MIGRATING on source node" do
    reply = @aux_cluster.redis_command @source_node,
        "cluster setslot #{@slot} migrating #{@target_node[:name]}"
    assert_not_redis_err(reply)
end

validate_getting_keys()

test "Set slot #{@slot} to be owned by target node" do
    reply = @aux_cluster.redis_command @source_node,
        "cluster setslot #{@slot} node #{@target_node[:name]}"
    assert_not_redis_err(reply)
end

validate_getting_keys()