require 'redis'
require 'hiredis'

setup &RedisProxyTestCase::GenericSetup

$numkeys = 100
$numclients = 10
$datalen = [1, 4096]

$datalen.each{|len|

    test "MULTI..SET..EXEC (size = #{len}b) #{$numkeys} keys" do
        spawn_clients($numclients){|client, idx|
            pipeline_args = []
            (0...$numkeys).each{|n|
                log_test_update "key #{n + 1}/#{$numkeys}"
                val = n.to_s * len
                count = (rand * 1000).to_i % 4
                count = 1 if count == 0
                pipeline_args = []
                count.times{|i|
                    pipeline_args << ["k:#{n}", val]
                }
                begin
                    reply = client.multi {
                        pipeline_args.each{|a|
                            client.set *a
                        }
                    }
                rescue Redis::CommandError => cmderr
                    reply = cmderr
                end
                assert_not_redis_err(reply)
            }
            log_same_line('')
        }
    end

    test "MULTI..GET..EXEC (size = #{len}b) #{$numkeys} keys" do
        spawn_clients($numclients){|client, idx|
            pipeline_args = []
            (0...$numkeys).each{|n|
                log_test_update "key #{n + 1}/#{$numkeys}"
                val = n.to_s * len
                count = (rand * 1000).to_i % 4
                count = 1 if count == 0
                pipeline_args = []
                expected = []
                count.times{|i|
                    pipeline_args << "k:#{n}"
                    expected << val
                }
                begin
                    reply = client.multi {
                        pipeline_args.each{|a|
                            client.get *a
                        }
                    }
                rescue Redis::CommandError => cmderr
                    reply = cmderr
                end
                assert_not_redis_err(reply)
                assert(reply, expected)
            }
            log_same_line('')
        }
    end

}
