setup {
    use_valgrind = $options[:valgrind] == true
    loglevel = $options[:log_level] || 'debug'
    dump_queues = $options[:dump_queues]
    dump_queries = $options[:dump_queries]
    @socketfile = File.join RedisProxyTestCase::TMPDIR,
        "proxy-#{urand2hex(4)}.sock"
    @aux_cluster = RedisCluster.new
    @aux_cluster.restart
    @aux_proxy = RedisClusterProxy.new @aux_cluster,
                                       log_level: loglevel,
                                       dump_queries: dump_queries,
                                       dump_queues: dump_queues,
                                       valgrind: use_valgrind,
                                       verbose: true,
                                       unixsocket: @socketfile
    @aux_proxy.start
    $aux_cluster, $aux_proxy = @aux_cluster, @aux_proxy
}

cleanup {
    @aux_proxy.stop
    @aux_proxy = nil
    @aux_cluster.stop
    @aux_cluster = nil
    $aux_cluster = nil
    $aux_proxy = nil
    FileUtils.rm @socketfile if File.exists? @socketfile
}


test "SET a..z" do
    redis = Redis.new path: @socketfile
    ('a'..'z').each{|char|
        begin
            reply = redis.set("k:#{char}", char)
        rescue Redis::CommandError => cmderr
            reply = cmderr
        end
        assert_not_redis_err(reply)
    }
end

test "GET a..z" do
    redis = Redis.new path: @socketfile
    ('a'..'z').each{|char|
        begin
            reply = redis.get("k:#{char}")
        rescue Redis::CommandError => cmderr
            reply = cmderr
        end
        assert_not_redis_err(reply)
        assert_equal(reply, char)
    }
end
