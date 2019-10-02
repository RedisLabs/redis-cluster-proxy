require 'redis'
require 'hiredis'

setup &RedisProxyTestCase::GenericSetup

$numkeys = 5000
$numlists = $numkeys / 10
$numclients = 10
$datalen = [1]#[1, 4096]
$pipeline = 2
$disconnect_every = 8

$datalen.each{|len|

    test "SET #{$numkeys} keys (#{len} byte(s))" do
        spawn_clients(1){|client, idx|
            (0...$numkeys).each{|n|
                val = n.to_s * len
                reply = redis_command client, :set, "k:#{n}", val
                assert_not_redis_err(reply)
            }
        }
    end

    test "GET #{$numkeys} keys (Disconnect every #{$disconnect_every})" do
        spawn_clients($numclients){|client, idx|
            (0...$numkeys).each{|n|
                log_test_update "key #{n + 1}/#{$numkeys}"
                key = "k:#{n}"
                val = n.to_s * len
		if !client.connected?
		    client._client.connect
		end
                if ((n + 1) % $disconnect_every) == 0
                    client._client.write(['get', key])
                    client.disconnect!
	            next
                end
                reply = redis_command client, :get, key
                assert_not_redis_err(reply)
                assert_equal(reply, val)
            }
        }
    end

}
