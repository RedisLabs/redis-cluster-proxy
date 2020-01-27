setup &RedisProxyTestCase::GenericSetup

def redis_raw_query(redis, qry, raw_reply: false, split: nil, wait: 0.1,
                    verbose: false)
    if !redis._client.connected?
        redis._client.connect
    end
    raise "Could not connect!" if !redis._client.connected?
    sock = redis._client.connection.instance_eval{@sock}
    raise "Missing sock!" if !sock
    pipeline_c = 1
    commands = redis_query_from_raw qry, verbose: verbose
    pipeline_c = commands.length if commands
    if !split
        sock.write qry
    else
        split = [split] if !split.is_a? Array
        queries = []
        truncated = 0
        split.each{|idx|
            idx = idx.to_i
            idx -= truncated
            next if idx == 0
            break if idx >= qry.length
            queries << qry[0...idx]
            truncated += queries.last.length
            qry = qry[idx..-1]
        }
        queries << qry if qry.length > 0
        #puts queries.map{|q| q.inspect}.join("\n") if verbose
        queries.each{|q|
            puts q.inspect if verbose
            sock.write q
            sleep wait if wait
        }
    end
    if !raw_reply
        replies = []
        if pipeline_c > 1
            while pipeline_c > 0
                replies << redis._client.read
                pipeline_c -= 1
            end
            return replies
        else
            return redis._client.read
        end
    else
        reply = ''
        loop do
            begin
                reply << sock.read(1)#s.gets
            rescue Errno::EAGAIN
            end
            break if s.nread <= 0
        end
        reply = nil if reply.empty?
        reply
    end
end

def redis_format_command(*command)
    "*#{command.length}\r\n" +
    command.map{|w|
        w = w.to_s
        "$#{w.length}\r\n#{w}\r\n"
    }.join
end

def redis_query_from_raw(rawquery, verbose: false)
    commands = []
    cur = nil
    while rawquery.length > 0
        cur_bulk_len = nil
        if (match = rawquery.match(/^\*(\d+)\r\n/))
            commands << cur[:query] if cur
            cur = {
                bulks_count: match[1].to_i
            }
            return nil if cur[:bulks_count].zero?
            rawquery = rawquery[match.end(0)..-1]
            break if !rawquery || rawquery.empty?
            cur_bulk_len = nil
        end
        if !cur
            puts "Invalid query: missing '*'" if verbose
            return nil
        end
        puts "Bulks count: #{cur[:bulks_count]}" if verbose
        while cur[:bulks_count] > 0
            break if !rawquery || rawquery.strip.empty?
            if !cur_bulk_len
                match = rawquery.match(/^\$(\d+)\r\n/)
                if !match
                    puts "Invalid query: missing '$'" if verbose
                    puts rawquery[0,5].inspect + '...' if verbose
                    return nil
                end
                cur_bulk_len = match[1].to_i
                puts "Current bulk length: #{cur_bulk_len}" if verbose
                rawquery = rawquery[match.end(0)..-1]
                break if !rawquery || rawquery.empty?
            else
                arg = rawquery[0, cur_bulk_len]
                cur[:query] ||= []
                cur[:query] << arg
                rawquery = rawquery[cur_bulk_len..-1]
                break if !rawquery || rawquery.empty?
                if !rawquery[/^\r\n/]
                    if verbose
                        puts "Invalid query: missing \\r\\n after argument: " +
                              arg.inspect
                    end
                    return nil
                end
                rawquery = rawquery[2..-1]
                cur_bulk_len = nil
                cur[:bulks_count] -= 1
                break if !rawquery || rawquery.empty?
            end
            #rawquery = rawquery[match.end(0)..-1]
            break if !rawquery || rawquery.empty?
        end
        break if !rawquery || rawquery.empty?
    end
    commands << cur[:query] if cur && cur[:query]
    commands
end

$options ||= {}
$numkeys = 4
$numclients = $options[:clients] || 1
$datalen = [1, 64]

if $options[:max_keys] && $numkeys > $options[:max_keys]
    $numkeys = $options[:max_keys]
    $numlists = $numkeys / 10
end

nk = $numkeys
nc = $numclients
$datalen.each{|len|

    test "SET #{nk} keys (s=#{len}b, c=#{nc}), split buffer" do
        spawn_clients($numclients){|client, idx|
            (0...$numkeys).each{|n|
                if n == $numkeys - 1
                    val = '*' * len
                else
                    val = n.to_s * len
                    val += "\r\n!\r\n" if n == $numkeys - 2
                end
                query = redis_format_command :set, "k:#{n}", val
                qlen = query.length
                step = len > 1 ? 8 : 1
                (1..(qlen - 1)).step(step){|i|
                    begin
                        log_test_update "key #{n + 1}/#{$numkeys} " +
                                        "#{i}-#{qlen - i} (buflen: #{qlen})"
                        reply = redis_raw_query client, query , split: [i]
                    rescue Redis::CommandError => err
                        reply = err
                    end
                    assert_not_redis_err(reply)
                }
            }
            log_same_line ''
        }
    end

    test "GET #{nk} keys (s=#{len}b, c=#{nc}), split buffer" do
        spawn_clients($numclients){|client, idx|
            (0...$numkeys).each{|n|
                if n == $numkeys - 1
                    val = '*' * len
                else
                    val = n.to_s * len
                    val += "\r\n!\r\n" if n == $numkeys - 2
                end
                query = redis_format_command :get, "k:#{n}"
                qlen = query.length
                step = len > 1 ? 8 : 1
                (1..(qlen - 1)).step(step){|i|
                    begin
                        log_test_update "key #{n + 1}/#{$numkeys} " +
                                        "#{i}-#{qlen - i} (buflen: #{qlen})"
                        reply = redis_raw_query client, query , split: [i]
                    rescue Redis::CommandError => err
                        reply = err
                    end
                    assert_not_redis_err(reply)
                    assert_equal reply.to_s, val
                }
            }
            log_same_line ''
        }
    end

    test "GET #{nk} keys (s=#{len}b, c=#{nc}, pipeline), split buffer" do
        spawn_clients($numclients){|client, idx|
            (0...$numkeys).each{|n|
                log_test_update "key #{n + 1}/#{$numkeys}"
                if n == $numkeys - 1
                    val = '*' * len
                else
                    val = n.to_s * len
                    val += "\r\n!\r\n" if n == $numkeys - 2
                end
                query = redis_format_command :get, "k:#{n}"
                query *= 2
                qlen = query.length
                step = len > 1 ? 8 : 1
                (1..(qlen - 1)).step(step){|i|
                    begin
                        log_test_update "key #{n + 1}/#{$numkeys} " +
                                        "#{i}-#{qlen - i} (buflen: #{qlen})"
                        reply = redis_raw_query client, query , split: [i]
                    rescue Redis::CommandError => err
                        reply = err
                    end
                    assert_not_redis_err(reply)
                    assert_equal reply, [val, val]
                }
            }
            log_same_line ''
        }
    end
}

test "INVALID QUERIES" do
    spawn_clients(1){|client, idx|
        query = redis_format_command :get, 'mykey'
        qlen = query.length
        (1..(qlen - 2)).each{|i|
            log_test_update "removing byte at #{i}"
            q = query
            q = q[0...i] + q[(i+1)..-1]
            reply = redis_raw_query client, q
            assert_redis_err(reply)
            assert_not_nil(reply.to_s.downcase['protocol error'],
                "Expected 'Protocol error', got #{reply.to_s}")
            disconnected = false
            begin
                reply = redis_raw_query client, redis_format_command(:ping)
            rescue Redis::ConnectionError => e
                disconnected = true
            rescue Errno::EPIPE => e
                disconnected = true
            end
            if !disconnected
                disconnected = (
                    reply.is_a?(Redis::ConnectionError) ||
                    reply.is_a?(Errno::EPIPE)
                )
            end
            assert(disconnected, "Expected client to be disconnected")
            client._client.connect
        }
    }
end
