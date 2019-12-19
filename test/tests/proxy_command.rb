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
