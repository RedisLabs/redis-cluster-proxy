setup &RedisProxyTestCase::GenericSetup

test "SET a..z" do
    ('a'..'z').each{|char|
        reply = @proxy.set("k:#{char}", char)
        assert_not_redis_err(reply)
    }
end

test "GET a..z" do
    ('a'..'z').each{|char|
        reply = @proxy.get("k:#{char}")
        assert_not_redis_err(reply)
        assert_equal(reply, char)
    }
end
