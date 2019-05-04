require 'redis'
require 'hiredis'

setup &RedisProxyTestCase::GenericSetup

$numkeys = 5000
$numlists = $numkeys / 10
$numclients = 10
$datalen = [1, 4096]

$datalen.each{|len|

    test "SET #{$numkeys} keys (#{len} byte(s))" do
        spawn_clients($numclients){|client, idx|
            (0...$numkeys).each{|n|
                val = n.to_s * len
                reply = client.set "k:#{n}", val
                assert_not_redis_err(reply)
            }
        }
    end

    test "GET #{$numkeys} keys (#{len} byte(s))" do
        spawn_clients($numclients){|client, idx|
            (0...$numkeys).each{|n|
                val = n.to_s * len
                reply = client.get "k:#{n}"
                assert_not_redis_err(reply)
                assert_equal(reply.to_s, val)
            }
        }
    end

    test "RPUSH #{$numkeys} keys (#{len} byte(s))" do
        spawn_clients(1){|client, idx|
            (0...$numlists).each{|n|
                (0...10).each{|vn| 
                    val = vn.to_s * len
                    val = "#{n}:#{vn}"
                    reply = client.rpush "mylist:#{n}:#{len}", val
                    assert_not_redis_err(reply)
                }
            }
        }
    end

    test "LRANGE #{$numkeys} keys (#{len} byte(s))" do
        spawn_clients($numclients){|client, idx|
            (0...$numlists).each{|n|
                values = (0...10).map{|vn| 
                    val = vn.to_s * len
                    val = "#{n}:#{vn}"
                }
                reply = client.lrange "mylist:#{n}:#{len}", 0, -1
                assert_not_redis_err(reply)
                assert_equal(reply, values)
            }
        }
    end

    test "SADD #{$numkeys} keys (#{len} byte(s))" do
        spawn_clients(1){|client, idx|
            (0...$numlists).each{|n|
                (0...10).each{|vn| 
                    val = vn.to_s * len
                    val = "#{n}:#{vn}"
                    reply = client.sadd "myset:#{n}:#{len}", val
                    assert_not_redis_err(reply)
                }
            }
        }
    end

    test "SMEMBERS #{$numkeys} keys (#{len} byte(s))" do
        spawn_clients($numclients){|client, idx|
            (0...$numlists).each{|n|
                values = (0...10).map{|vn| 
                    val = vn.to_s * len
                    val = "#{n}:#{vn}"
                }
                reply = client.smembers "myset:#{n}:#{len}"
                assert_not_redis_err(reply)
                assert_equal(reply.sort, values.sort)
            }
        }
    end

}
