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
}

cleanup {
    @aux_proxy.stop
    @aux_proxy = nil
    @aux_cluster.stop
    @aux_cluster = nil
}

$numkeys = 50
$numclients = $options[:clients] || 10
$node_down_every = 8
#$node_down_for = 4

if $options[:max_keys] && $numkeys > $options[:max_keys]
    $numkeys = $options[:max_keys]
end

require 'thread'

def is_down_err(reply)
    return false if !reply
    reply = reply.to_s
    reply = reply.downcase
    (
        !reply['cluster node disconnected'].nil? ||
        !reply['could not connect to node'].nil? ||
        !reply['error writing to cluster'].nil?
    )
end

down_node_mutex = Mutex.new
down_node = nil
down_node_thread = nil
down_at = nil

test "SET #{$numkeys} keys to test 'Node down'" do
    spawn_clients(1, proxy: @aux_proxy){|client, idx|
        (0...$numkeys).each{|n|
            val = n.to_s
            reply = redis_command client, :set, "k:#{n}", val
            assert_not_redis_err(reply)
        }
    }
end

test "GET #{$numkeys} keys (Node down every #{$node_down_every})" do
    spawn_clients($numclients, proxy: @aux_proxy){|client, idx|
        (0...$numkeys).each{|n|
            log_test_update "key #{n + 1}/#{$numkeys}"
            key = "k:#{n}"
            node_for_key = @aux_cluster.node_for_key(key)
            val = n.to_s
            if !client.connected?
                client._client.connect
            end
            if ((n + 1) % $node_down_every) == 0
                down_node_mutex.synchronize{
                    if !down_node
                        down_node = @aux_cluster.stop_random_master(quiet: true)
                        down_node_thread = idx
                        down_at = n
                    else
=begin
                        if down_node_thread == idx &&
                           n >= (down_at + $node_down_for)
                            @aux_cluster.start_instance down_node,
                                                    quiet:true,
                                                    fatal: false,
                                                    timeout: 5
                            if @aux_cluster.is_instance_running?(down_node)
                                down_node = nil
                                down_node_thread = nil
                                down_at = nil
                            else
                                raise "Failed to restart node"
                            end
                        end
=end
                    end
                }  
            end
            is_down = false
            is_down_node = false
            down_node_mutex.synchronize{
                is_down_node = (down_node != nil &&
                                (node_for_key[:port] == down_node[:port]))
                is_down = (is_down_node &&
                           !@aux_cluster.is_instance_running?(down_node))
            }
            reply = redis_command client, :get, key
            if is_down
                err = "Node for key '#{key}' (:#{node_for_key[:port]} should "+
                      "be down, but got reply: '#{reply}'"
                assert_redis_err(reply)
                assert(is_down_err(reply), err)
            else
                if is_down_node && !is_down_err(reply)
                    assert_not_redis_err(reply)
                    assert_equal(reply, val)
                end
            end
        }
    }
end
