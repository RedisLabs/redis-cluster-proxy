require 'redis'
require 'hiredis'

setup &RedisProxyTestCase::GenericSetup

$options ||= {}
$numkeys = 500
$numlists = 3
$numclients = $options[:clients] || 10
$datalen = [1]
$values_per_list = 2
used_slot = 0
tag = CRC16_SLOT_TABLE[used_slot]

if $options[:max_keys] && $numkeys > $options[:max_keys]
    $numkeys = $options[:max_keys]
    $numlists = $numkeys / 10
end

$datalen.each{|len|

    $lists = []

    test "ZADD #{$numlists} keys (size=#{len}b, clients=#{$numclients})" do
        spawn_clients(1){|client, idx|
            (0...$numlists).each{|n|
                log_test_update "key #{n + 1}/#{$numlists}"
                list = "zset:{#{tag}}:#{n}"
                $lists << list if idx.zero?
                $values_per_list.times.each{|i|
                    vn = n + i
                    val = vn.to_s * len
                    reply = redis_command client, :zadd, list, vn, val
                    assert_not_redis_err(reply)
                }
            }
            log_same_line ''
        }
    end

    test "ZUNIONSTORE #{$numlists} (size=#{len}b, clients=#{$numclients})" do
        spawn_clients($numclients){|client, idx|
            dest_key = "dest:zset:{#{tag}}:#{idx}"
            log_test_update "key #{idx + 1}/#{$numclients}"
            begin
                reply = redis_command client, :zunionstore, dest_key, $lists,
                                      aggregate: 'max'
            rescue Redis::CommandError => cmderr
                reply = cmderr
            end
            assert_not_redis_err(reply)
            assert_equal(reply.to_i, $lists.length + 1)
            log_same_line ''
        }
    end

    test "ZRANGE #{$numlists} keys (size=#{len}b, clients=#{$numclients})" do
        spawn_clients($numclients){|client, idx|
            dest_key = "dest:zset:{#{tag}}:#{idx}"
            begin
                reply = redis_command client, :zrange, dest_key,0,-1
            rescue Redis::CommandError => cmderr
                reply = cmderr
            end
            assert_not_redis_err(reply)
            expected = (0..$numlists).map{|n|
                n.to_s * len
            }
            assert_equal reply, expected
            log_same_line ''
        }
    end

}
