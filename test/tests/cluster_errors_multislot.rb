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
$living_threads = 0

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

test "Enable cross-slot queries" do
    spawn_clients(1, proxy: $aux_proxy){|client, idx|
        reply = client.proxy 'config', 'set', 'enable-cross-slot', '1'
        assert_not_redis_err(reply)
        reply = client.proxy 'config', 'get', 'enable-cross-slot'
        assert_not_redis_err(reply)
        assert_class(reply, Array)
        assert_equal(reply.length, 2, "PROXY CONFIG GET enable-cross-slot " +
                                      "reply array should contian 2 items, " +
                                      "but it has #{reply.length} item(s)")
        assert_equal(reply[1].to_i, 1)
    }
end

test "Move slot" do
    $slot = $source_node[:slots].keys[0]
    $slots = [$target_node[:slots].keys.last,
              @aux_cluster.masters.last[:slots].keys.last]

    log_same_line ''
    log_test_update "moving slot #{$slot}..."
    res = {}
    moved = @aux_cluster.move_slot $slot, $target_node,
                                   results: res,
                                   update: false

    assert(moved,
        "Failed to move slot #{$slot}: #{res[:error]}")
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
end

test "MGET on moved slot (clients=#{$numclients})" do
    spawn_clients($numclients, proxy: $aux_proxy){|client, idx|
        slots = [$slots[0], $slot, $slots[1]]
        keys = slots.map{|s|
            CRC16_SLOT_TABLE[s.to_i]
        }
        reply = client.mget *keys
        assert_not_redis_err(reply)
        assert_class(reply, Array)
        reply.each{|r| assert_nil(r)}
    }
end
