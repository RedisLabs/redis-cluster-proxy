require 'redis'
require 'hiredis'

setup &RedisProxyTestCase::GenericSetup

$numkeys = 1500
$numclients = $options[:clients] || 10
$datalen = [1, 4096]
$pipeline = [2, 4]

if $options[:max_keys] && $numkeys > $options[:max_keys]
    $numkeys = $options[:max_keys]
end

$datalen.each{|len|

    $pipeline.each{|pipeline|

        test "SET (pipeline=#{pipeline}, size = #{len}b) #{$numkeys} keys" do
            spawn_clients($numclients){|client, idx|
                pipeline_args = []
                (0...$numkeys).each{|n|
                    log_test_update "key #{n + 1}/#{$numkeys}"
                    val = n.to_s * len
                    pipeline_args << ["k:#{n}", val]
                    if ((n + 1) % pipeline) == 0
                        begin
                            reply = client.pipelined {
                                pipeline_args.each{|a|
                                    client.set *a
                                }
                            }
                        rescue Redis::CommandError => cmderr
                            reply = cmderr
                        end
                        assert_not_redis_err(reply)
                        pipeline_args.clear
                    end
                }
                log_same_line('')
            }
        end

        test "GET (pipeline=#{pipeline}, size = #{len}b) #{$numkeys} keys" do
            spawn_clients($numclients){|client, idx|
                pipeline_args = []
                expected = []
                (0...$numkeys).each{|n|
                    log_test_update "key #{n + 1}/#{$numkeys}"
                    pipeline_args << "k:#{n}"
                    val = n.to_s * len
                    expected << val
                    if ((n + 1) % pipeline) == 0
                        begin
                            reply = client.pipelined {
                                pipeline_args.each{|a|
                                    client.get(a)
                                }
                            }
                        rescue Redis::CommandError => cmderr
                            reply = cmderr
                        end
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
}
