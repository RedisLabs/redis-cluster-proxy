setup {
    max_keys = 1000
    RedisProxyTestCase::GenericSetup.call
    $all_keys = []
    get_keys = proc{
        $main_cluster.masters.each{|node|
            r = Redis.new port: node[:port]
            begin
                reply = r.keys '*'
            rescue Redis::CommandError => cmderr
                reply = cmderr
            end
            assert_not_redis_err(reply)
            $all_keys |= reply
        }
    }
    get_keys.call
    if $all_keys.length > max_keys
        $all_keys = []
        $main_cluster.masters.each{|node|
            r = Redis.new port: node[:port]
            begin
                reply = r.flushdb
            rescue Redis::CommandError => cmderr
                reply = cmderr
            end
            assert_not_redis_err(reply)
        }
        get_keys.call
    end
    if $all_keys.length.zero?
        log "Populating cluster with #{max_keys} keys"
        r = Redis.new port: $main_proxy.port
        max_keys.times.each{|i|
            k = "k:#{i}"
            begin
                reply = r.set k, i.to_s
            rescue Redis::CommandError => cmderr
                reply = cmderr
            end
            assert_not_redis_err(reply)
            begin
                reply = r.exists k
            rescue Redis::CommandError => cmderr
                reply = cmderr
            end
            assert_not_redis_err(reply)
            assert(reply, "Failed to create key '#{k}'")
        }
    end
    get_keys.call
    $all_keys.sort!
}

$options ||= {}
$numclients = $options[:clients] || 10

test "SCAN keys" do
    spawn_clients($numclients){|client, idx|
        cursor = nil
        mykeys = []
        while !cursor || cursor.to_i != 0
            cursor ||= 0
            reply = redis_command client, :scan, cursor, match: '*', count: 5
            assert_not_redis_err(reply)
            cursor, keys = reply
            assert_not_nil cursor, 'Got nil cursor'
            assert_not_nil keys, 'Got nil keys'
            match = cursor.strip.match /^[0-9]+$/
            assert_not_nil match, "Invalid cursor: '#{cursor}'"
            assert_class keys, Array
            mykeys |= keys
            node_idx = 0
            if cursor.length > 4 && cursor.to_i != 0
                node_idx = cursor[-4..-1].to_i
                log_test_update "node #{node_idx + 1}"
            end
        end
        mykeys.sort!
        assert_equal(mykeys.length, $all_keys.length,
            "Not all keys scanned: expected #{$all_keys.length}, " +
            "got #{mykeys.length}")
        mykeys.each_with_index{|k, i|
            assert_equal(k, $all_keys[i],
                "Keys at index #{i} differ: #{k} != #{$all_keys[i]}")
        }
        log_same_line ''
    }
end
