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
