require 'redis'
require 'hiredis'

setup &RedisProxyTestCase::GenericSetup

$numkeys = 5000
$numlists = $numkeys / 10
$numclients = 10
$datalen = [1]#[1, 4096]
$pipeline = 2

$datalen.each{|len|

=begin
    test "SET #{$numkeys} keys (#{len} byte(s))" do
        spawn_clients(1){|client, idx|
            (0...$numkeys).each{|n|
                val = n.to_s * len
                reply = client.set "k:#{n}", val
                assert_not_redis_err(reply)
            }
        }
    end
=end

    test "SET (pipeline=#{$pipeline}) #{$numkeys} keys (#{len} byte(s))" do
        spawn_clients($numclients){|client, idx|
            pipeline_args = []
            (0...$numkeys).each{|n|
                log_test_update "key #{n + 1}/#{$numkeys}"
                val = n.to_s * len
                pipeline_args << ["k:#{n}", val]
                if ((n + 1) % $pipeline) == 0
                    reply = client.pipelined {
                        pipeline_args.each{|a|
                            client.set *a
                        }
                    }
                    assert_not_redis_err(reply)
                    pipeline_args.clear
                end
            }
            log_same_line('')
        }
    end

    test "GET (pipeline=#{$pipeline}) #{$numkeys} keys (#{len} byte(s))" do
        spawn_clients($numclients){|client, idx|
            pipeline_args = []
            expected = []
            (0...$numkeys).each{|n|
                log_test_update "key #{n + 1}/#{$numkeys}"
                pipeline_args << "k:#{n}"
                val = n.to_s * len
                expected << val
                if ((n + 1) % $pipeline) == 0
                    reply = client.pipelined {
                        pipeline_args.each{|a|
                            client.get(a)
                        }
                    }
                    errmsg = "GET #{pipeline_args.inspect}: expected " +
                             "#{expected.inspect}, " +
                             "got #{reply.inspect}"
                    assert_not_redis_err(reply)
                    assert_equal(reply, expected, errmsg)
                    pipeline_args.clear
                    expected.clear
                end
            }
            log_same_line('')
        }
    end

}
