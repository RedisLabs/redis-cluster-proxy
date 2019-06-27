require 'redis'
require 'hiredis'

setup &RedisProxyTestCase::GenericSetup

$numkeys = 15
$numclients = 10
$datalen = [1, 4096]
$keycount = [3, 6]

$datalen.each{|len|

    $keycount.each{|keycount|

        test "MSET (keycount=#{keycount}, size = #{len}b) #{$numkeys} keys" do
            spawn_clients($numclients){|client, idx|
                keycount_args = []
                (0...$numkeys).each{|n|
                    log_test_update "key #{n + 1}/#{$numkeys}"
                    val = n.to_s * len
                    keycount_args << ["k:#{n}", val]
                    if ((n + 1) % keycount) == 0
                        begin
                            client.mset *keycount_args
                        rescue Redis::CommandError => cmderr
                            reply = cmderr
                        end
                        assert_not_redis_err(reply)
                        keycount_args.clear
                    end
                }
                log_same_line('')
            }
        end

        test "MGET (keycount=#{keycount}, size = #{len}b) #{$numkeys} keys" do
            spawn_clients($numclients){|client, idx|
                keycount_args = []
                expected = []
                (0...$numkeys).each{|n|
                    log_test_update "key #{n + 1}/#{$numkeys}"
                    keycount_args << "k:#{n}"
                    val = n.to_s * len
                    expected << val
                    if ((n + 1) % keycount) == 0
                        begin
                            reply = client.mget *keycount_args
                        rescue Redis::CommandError => cmderr
                            reply = cmderr
                        end
                        errmsg = "MGET #{keycount_args.inspect}: expected " +
                                 "#{expected.inspect}, " +
                                 "got #{reply.inspect}"
                        assert_not_redis_err(reply)
                        assert_equal(reply, expected, errmsg)
                        keycount_args.clear
                        expected.clear
                    end
                }
                log_same_line('')
            }
        end
    }
}

test "DBSIZE" do
    dbsize = 0
    $main_cluster.masters.each{|node|
        reply = $main_cluster.redis_command node, 'dbsize'
        reply = reply.strip
        is_err = reply.index('ERR') == 0
        assert(!is_err,
                "Node #{node[:port]} returned an error on DBSIZE: #{reply}")
        dbsize += reply.to_i
    }
    assert(dbsize > 0, "Cluster has no keys, cannot test DBSIZE!")
    expected = dbsize
    spawn_clients($numclients){|client, idx|
        reply = client.dbsize
        assert_not_redis_err(reply)
        dbsize = reply.to_i 
        errmsg = "DBSIZE #{dbsize} != from expected #{expected}"
        assert_equal(dbsize, expected, errmsg)
    }
end

test "DEL 15 keys" do
    spawn_clients($numclients){|client, idx|
        keys = [] 
        (0...$numkeys).each{|n|
            log_test_update "key #{n + 1}/#{$numkeys}"
            val = n.to_s
            key = "key2del:#{idx}:#{n}"
            keys << key
            reply = client.set key, val
            assert_not_redis_err reply
            reply = client.get key
            assert_not_redis_err reply
            assert_equal(val, reply)
        }
        reply = client.del *keys
        assert_not_redis_err reply
        assert_equal(reply.to_i, keys.length)
    }
end

=begin
test "EXISTS 15 keys" do
    spawn_clients($numclients){|client, idx|
        keys = [] 
        (0...$numkeys).each{|n|
            log_test_update "key #{n + 1}/#{$numkeys}"
            val = n.to_s
            key = "keyexist:#{idx}:#{n}"
            keys << key
            reply = client.set key, val
            assert_not_redis_err reply
            reply = client.get key
            assert_not_redis_err reply
            assert_equal(val, reply)
        }
        client._client.write ['exists'] + keys
        reply = client.read
        #assert_not_redis_err reply
        assert_equal(reply.to_i, keys.length)
    }
end
=end
