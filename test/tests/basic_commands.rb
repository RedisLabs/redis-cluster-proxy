require 'redis'
require 'hiredis'

setup &RedisProxyTestCase::GenericSetup

$numkeys = 5000
$numlists = $numkeys / 10
$numclients = 10
$datalen = [1, 4096]

$datalen.each{|len|

    test "SET #{$numkeys} keys (size=#{len}b, clients=#{$numclients})" do
        spawn_clients($numclients){|client, idx|
            (0...$numkeys).each{|n|
                log_test_update "key #{n + 1}/#{$numkeys}"
                val = n.to_s * len
                reply = redis_command client, :set, "k:#{n}", val
                assert_not_redis_err(reply)
            }
            log_same_line ''
        }
    end

    test "GET #{$numkeys} keys (size=#{len}b, clients=#{$numclients})" do
        spawn_clients($numclients){|client, idx|
            (0...$numkeys).each{|n|
                log_test_update "key #{n + 1}/#{$numkeys}"
                val = n.to_s * len
                reply = redis_command client, :get, "k:#{n}"
                assert_not_redis_err(reply)
                assert_equal(reply.to_s, val)
            }
            log_same_line ''
        }
    end

    test "RPUSH #{$numlists} keys (size=#{len}b, clients=#{$numclients})" do
        spawn_clients(1){|client, idx|
            (0...$numlists).each{|n|
                log_test_update "key #{n + 1}/#{$numlists}"
                (0...10).each{|vn| 
                    val = vn.to_s * len
                    val = "#{n}:#{vn}"
                    reply = redis_command client, :rpush,
                                          "mylist:#{n}:#{len}", val
                    assert_not_redis_err(reply)
                }
            }
            log_same_line ''
        }
    end

    test "LRANGE #{$numlists} keys (size=#{len}b, clients=#{$numclients})" do
        spawn_clients($numclients){|client, idx|
            (0...$numlists).each{|n|
                log_test_update "key #{n + 1}/#{$numlists}"
                values = (0...10).map{|vn| 
                    val = vn.to_s * len
                    val = "#{n}:#{vn}"
                }
                reply = redis_command client, :lrange,
                                      "mylist:#{n}:#{len}", 0, -1
                assert_not_redis_err(reply)
                assert_equal(reply, values)
            }
            log_same_line ''
        }
    end

    test "SADD #{$numlists} keys (size=#{len}b, clients=#{$numclients})" do
        spawn_clients(1){|client, idx|
            (0...$numlists).each{|n|
                log_test_update "key #{n + 1}/#{$numlists}"
                (0...10).each{|vn| 
                    val = vn.to_s * len
                    val = "#{n}:#{vn}"
                    reply = redis_command client, :sadd,
                                          "myset:#{n}:#{len}", val
                    assert_not_redis_err(reply)
                }
            }
            log_same_line ''
        }
    end

    test "SMEMBERS #{$numlists} keys (size=#{len}b, clients=#{$numclients})" do
        spawn_clients($numclients){|client, idx|
            (0...$numlists).each{|n|
                log_test_update "key #{n + 1}/#{$numlists}"
                values = (0...10).map{|vn| 
                    val = vn.to_s * len
                    val = "#{n}:#{vn}"
                }
                reply = redis_command client, :smembers, "myset:#{n}:#{len}"
                assert_not_redis_err(reply)
                assert_equal(reply.sort, values.sort)
            }
            log_same_line ''
        }
    end

    test "ZADD #{$numlists} keys (size=#{len}b, clients=#{$numclients})" do
        spawn_clients(1){|client, idx|
            (0...$numlists).each{|n|
                log_test_update "key #{n + 1}/#{$numlists}"
                (0...10).each{|vn| 
                    val = vn.to_s * len
                    val = "#{n}:#{vn}"
                    reply = redis_command client, :zadd,
                                          "myzset:#{n}:#{len}", vn, val
                    assert_not_redis_err(reply)
                }
            }
            log_same_line ''
        }
    end

    test "ZRANGE #{$numlists} keys (size=#{len}b, clients=#{$numclients})" do
        spawn_clients($numclients){|client, idx|
            (0...$numlists).each{|n|
                log_test_update "key #{n + 1}/#{$numlists}"
                values = (0...10).map{|vn| 
                    val = vn.to_s * len
                    val = "#{n}:#{vn}"
                }
                reply = redis_command client, :zrange, "myzset:#{n}:#{len}",0,-1
                assert_not_redis_err(reply)
                assert_equal(reply, values)
            }
            log_same_line ''
        }
    end

}
