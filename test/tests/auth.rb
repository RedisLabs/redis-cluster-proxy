require 'redis'
require 'hiredis'

$authpassw = '123'
$acl_username = 'myuser'
$acl_userpassw = '1234'

setup {
    use_valgrind = $options[:valgrind] == true
    loglevel = $options[:log_level] || 'debug'
    dump_queues = $options[:dump_queues]
    dump_queries = $options[:dump_queries]
    @aux_cluster = RedisCluster.new passw: $authpassw
    @aux_cluster.restart
    @aux_proxy = RedisClusterProxy.new @aux_cluster,
                                       log_level: loglevel,
                                       dump_queries: dump_queries,
                                       dump_queues: dump_queues,
                                       valgrind: use_valgrind,
                                       auth: $authpassw,
                                       verbose: true
    @aux_proxy.start
    $aux_cluster, $aux_proxy = @aux_cluster, @aux_proxy
    node = @aux_cluster.masters[0]
    r = Redis.new port: node[:port]
    reply = redis_command r, 'auth', $authpassw
    assert_not_redis_err(reply)
    begin
        @acl_command = r.command 'info', 'acl'
    rescue Redis::CommandError => cmderr
        @acl_command = nil
    end
    @acl_command = @acl_command.first if @acl_command.is_a? Array
    if @acl_command
        @aux_cluster.nodes.each{|n|
            r = Redis.new port: n[:port]
            reply = redis_command r, 'auth', $authpassw
            assert_not_redis_err(reply)
            reply = redis_command r, 'acl', 'setuser', $acl_username, 'on',
                                  ">#{$acl_userpassw}", '~*','+get'
            assert_not_redis_err(reply)
        }
    end
}

cleanup {
    @aux_proxy.stop
    @aux_proxy = nil
    @aux_cluster.stop
    @aux_cluster = nil
    $aux_cluster = nil
    $aux_proxy = nil
}

$options ||= {}
$numkeys = 50
$numclients = $options[:clients] || 10
$move_slot_every = 10
$hash_key_every = 3

if $options[:max_keys] && $numkeys > $options[:max_keys]
    $numkeys = $options[:max_keys]
    $numlists = $numkeys / 10
end



test "SET #{$numkeys} keys (clients=#{$numclients})" do
    spawn_clients($numclients, proxy: $aux_proxy){|client, idx|
        (0...$numkeys).each{|n|
            log_test_update "key #{n + 1}/#{$numkeys}"
            val = n.to_s
            key = "k:#{n}"
            reply = redis_command client, :set, key, val
            assert_not_redis_err(reply)
        }
        log_same_line ''
    }
end

test "GET #{$numkeys} keys (clients=#{$numclients}, multiplex=off)" do
    spawn_clients($numclients, proxy: $aux_proxy){|client, idx|
        expected = ['OK']
        keys = (0...$numkeys).map{|n|
            key = "k:#{n}"
            val = n.to_s
            expected << val
            key
        }
        begin
            reply = client.pipelined{
                client.proxy 'multiplexing', 'off'
                keys.each{|k|
                    client.get k
                }
            }
        rescue Redis::CommandError => cmderr
            reply = cmderr
        end
        assert_not_redis_err(reply)
        assert_equal(expected, reply)
        log_same_line ''
    }
end

test 'AUTH as restricted user' do
    if !@acl_command
        server = @aux_cluster.instance_eval{@redis_server}
        message = "Missing support for ACL in redis-server, found in:\n"
        message << "   #{server}\n"
        message << "   ACL requires redis-server >= 6.0\n"
        message << "   You can use the env. variable REDIS_HOME to specify\n"
        message << "   another path for redis-server\n"
        skip_test message
    end

    spawn_clients($numclients, proxy: $aux_proxy){|client, idx|
        use_restricted_user = ((idx % 2) == 0)
        if use_restricted_user
            begin
                reply = client.call 'auth', $acl_username, $acl_userpassw
            rescue Redis::CommandError => cmderr
                reply = cmderr
            end
            assert_not_redis_err(reply)
            reply = redis_command client, 'proxy', 'multiplexing', 'status'
            assert_not_redis_err(reply)
            assert_not_nil(reply)
            assert_equal(reply.downcase, 'off')
        end
        keys = (0...$numkeys).map{|n|
            key = "k:#{n}"
            val = n.to_s
            reply = redis_command client, 'get', key
            assert_not_redis_err(reply)
            assert_equal(reply, val)
            reply = redis_command client, 'set', key, val
            if use_restricted_user
                assert_redis_err(reply)
                reply = reply.to_s
                assert(reply['NOPERM'],
                       "Expected NOPERM error for restricted client, " +
                       "got '#{reply}'")
            else
                assert_not_redis_err(reply)
            end
        }
    }
end
