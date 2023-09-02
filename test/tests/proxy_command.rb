setup &RedisProxyTestCase::GenericSetup

test "PROXY CONFIG GET" do
    conf = {
        'threads' => 8,
        #'max-clients' => 10000
    }
    conf.each{|opt, val|
        reply = $main_proxy.proxy('config', 'get', opt.to_s)
        assert_not_redis_err(reply)
        assert(reply.is_a?(Array), "Expected array reply, got #{reply.class}")
        assert(reply.length >= 2,
               "Expected min. 2 elements in reply, got: #{reply.length}")
        assert_equal(reply[1].to_i, val.to_i)
    }
end

test "LOG TO PROXY" do
    msg = "*********** TEST LOG ***********"
    reply = log_to_proxy $main_proxy, msg
    assert_not_redis_err reply
    log = File.read $main_proxy.logfile
    assert_not_nil(log[msg], "Could not find logged message in proxy's log")
end

test "PROXY CLUSTER INFO" do
    reply = $main_proxy.proxy('cluster', 'info')
    assert_not_redis_err(reply)
    assert(reply.is_a?(Array), "Expected array reply, got #{reply.class}")
    assert(reply[0..4] == ["status", "updated", "connection", "shared", "nodes"], "Got unexpected reply: #{reply}")
    reply[5].each{ |node|
        _, name, _, ip, _, port, _, slots, _, replicas, _, connected = node
        assert(ip == "127.0.0.1", "unexpected ip #{ip}")
        assert([18000, 18001, 18002, 18003, 18004, 18005].include?(port), "unexpected port #{port}")
        assert([5461, 5462].include?(slots), "unexpected slots #{slots}")
        assert(replicas == 1, "replicas of #{replicas} != 1")
        assert(connected == 1, "connected of #{connected} != 1")
    }
end

test "PROXY CLUSTER UPDATE" do
    info_before = $main_proxy.proxy('cluster', 'info')
    assert_not_redis_err(info_before)

    update_reply = $main_proxy.proxy('cluster', 'update')
    assert_not_redis_err(update_reply)

    info_after = $main_proxy.proxy('cluster', 'info')
    assert_not_redis_err(info_after)

    assert(info_before == info_after, "cluster info before update != cluster info after update.\n #{info_before} != #{info_after}")

end