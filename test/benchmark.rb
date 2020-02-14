#!/usr/bin/env ruby

# Copyright (C) 2019  Giuseppe Fabio Nicotra <artix2 at gmail dot com>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published
# by the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

require 'rubygems'
require 'bundler/setup'

$redis_proxy_test_dir = File.expand_path(File.dirname(__FILE__))
load File.join($redis_proxy_test_dir, 'lib/test.rb')

include RedisProxyTestLogger

opts =RedisProxyTestUtils::OptionParser.new help_banner_arguments: '[TESTS]' do

    option   '',   '--custom-query', 'Use custom query insetad of test names'
    option   '',   '--proxy-threads NUM', 'Number of proxy threads'
    option   '',   '--proxy-log-level LEVEL', 'Proxy log level'
    option   '',   '--enable-cross-slot', 'Enable crosslot queries on proxy'
    option  '',   '--valgrind', 'Enable Valgrind'
    option   '',   '--benchmark-threads NUM', 'Number of benchmark threads'
    option   '',   '--benchmark-pipeline NUM', 'Pipeline queries'
    option   '',   '--benchmark-clients NUM',
             'Benchmark clients (use `max` to use `maxclients` from proxy)'
    option   '',   '--benchmark-opts OPTS', 'Other redis-benchmark options'
    option '-r',   '--repeat NUM', 'Repeat tests multiple times'
    option '-w',   '--wait SEC',
            'Wait time in seconds between each benchmark (default: 0, ' +
            'can be < 1, e.g. 0.5)'
    option '-d',   '--display OPTS',
           'Display benchmark results: none|short|normal (default: normal)'
    option '-o',   '--output PATH', 'Output results to a file'
    option   '',   '--csv', 'Set output format to CSV'

end

opts.auto_help!
opts.parse!
$options = opts.options
$display = :normal
if (display_opts = $options[:display])
    if display_opts == 'none'
        $display = false
    else
        $display = :"#{display_opts}"
    end
end
if $options[:wait]
    $options[:wait] = $options[:wait].to_f
end
if !$options[:custom_query]
    $tests = ARGV
    if $tests.length == 0
        $tests = %w(get)
    end
else
    $tests = ARGV.join(' ')
    if $tests.strip.empty?
        STDERR.puts "Missing custom query".red
        exit 1
    end
end
$bm_err_log = "/tmp/proxy-redis-benchmark.#{urand2hex(4)}.err"

def setup(proxy_threads: nil)
    if !$main_cluster
        @cluster = RedisCluster.new
        @cluster.restart
        $main_cluster = @cluster
    end
    if !$main_proxy
        @proxy = RedisClusterProxy.new @cluster, verbose: true,
                                       threads: proxy_threads,
                                       valgrind: $options[:valgrind],
                                       log_level: $options[:proxy_log_level] ||
                                                  :error
        @proxy.start
        $main_proxy = @proxy
    end
    if $options[:benchmark_clients] == 'max'
        puts "Getting max clients..."
        reply = @proxy.redis_command :proxy, :config, :get, :maxclients
        if reply.is_a? Redis::CommandError
            puts reply.to_s
            final_cleanup
            exit
        end
        c = reply[1].to_i
        if c > 0
            $options[:benchmark_clients] = c
            puts "Max clients: #{c}".blue
            if c > 100
                tw_reuse = nil
                is_linux = !(`uname -s`.strip.downcase['linux'].nil?)
                linux_proc_f = '/proc/sys/net/ipv4/tcp_tw_reuse'
                if is_linux && File.exists?(linux_proc_f)
                    begin
                        tw_reuse = (File.read(linux_proc_f).strip.to_i == 1)
                    rescue Exception => e
                    end
                end
                if tw_reuse == false
                    puts("WARN: SO_REUSEADDR is not enabled on your system".
                        yellow)
                    if is_linux
                        puts "      try to set 1 into #{linux_proc_f}".yellow
                    end
                elsif tw_reuse == nil
                    puts("WARN: SO_REUSEADDR may be disabled on your system".
                        yellow)
                end
                if !tw_reuse
                    puts("    Try to enable it in order to avoid TIME_WAIT ".
                         yellow +
                         "sockets".yellow)
                end
            end
        end
        client = @proxy.redis.instance_eval{@client}
        client.disconnect
    end
    if $options[:enable_cross_slot]
        reply = @proxy.redis_command :proxy, :config,
            :set, 'enable-cross-slot', '1'
        if reply.is_a? Redis::CommandError
            puts "Could not set 'enable-cross-slot'".red
            puts reply.to_s.red
            final_cleanup
            exit
        end
        reply = @proxy.redis_command 'proxy', 'config', 'get',
                                     'enable-cross-slot'
        if reply.is_a? Redis::CommandError
            puts "Could not verify 'enable-cross-slot'".red
            puts reply.to_s.red
            final_cleanup
            exit
        end
        if !reply.is_a? Array
            puts "Could not verify 'enable-cross-slot'".red
            puts "Unexpected reply class: #{reply.class}".red
            final_cleanup
            exit
        end
        if reply[1] != 1
            puts "Failed to enable cross-slot".red
            final_cleanup
            exit
        end
        client = @proxy.redis.instance_eval{@client}
        client.disconnect
    end
end

def final_cleanup
    if $test_proxies
        $test_proxies.each{|proxy|
            proxy.stop
        }
    end
    if $test_clusters
        $test_clusters.each{|cluster|
            cluster.destroy!
        }
    end
    $main_cluster = nil
    $main_proxy = nil
end

def redis_benchmark(port, tests, **kwargs)
    cmd ="#{$redis_benchmark} -p #{port}"
    opts =  kwargs.map{|arg,val|
        next if val.nil?
        arg = arg.to_s
        if arg.length == 1
            arg = "-#{arg}"
        else
            arg = "--#{arg.gsub(/_+/, '-')}"
        end
        if val != true
            arg = "#{arg} #{val}"
        end
        arg
    }.compact.join(' ')
    cmd << " #{opts}" if opts.length > 0
    if (bmopts = $options[:benchmark_opts])
        cmd << " #{bmopts}"
    end
    if tests.is_a?(Array) && tests.length > 0
        cmd += " -t #{tests.join(',')}"
    elsif tests.is_a?(String) && tests.length > 0
        cmd += " #{tests}"
    end
    cmd << " 2>#{$bm_err_log}"
    puts cmd
    `#{cmd}`
end

def parse_benchmark(output)
    output = output.gsub(/\r+/, "\n")
    lines = output.split(/\n+/)
    tests = []
    current_test = nil
    lines.each{|l|
        l.strip!
        if (match = l.match(/^==+\s*(.+)\s*==+$/))
            if current_test
                tests << current_test
            end
            name = match[1].strip
            name.sub! /\s*==+$/, ''
            current_test = {
                name: name
            }
        elsif current_test
            if (match = l.match(/^([\d\.]+)%\s*<=\s*([\d\.]+)\s+milliseconds$/))
                percent = match[1].to_f
                ms = match[2].to_f
                current_test[:min_latency] ||= ms
                if percent == 100
                    current_test[:max_latency] = ms
                else
                    next
                end
            elsif (match =
                   l.match(/(\d+) requests completed in ([\d\.]+) seconds/))
                current_test[:num_requests] = match[1].to_i
                current_test[:duration] = match[2].to_f
            elsif (match = l.match(/(\d+) parallel client/))
                current_test[:num_clients] = match[1].to_i
            elsif (match = l.match(/([\d\.]+) requests per second/))
                current_test[:rps] = match[1].to_f
            end
        end
    }
    tests << current_test if current_test
    tests
end

Signal.trap("TERM") do
    final_cleanup
end

Signal.trap("INT") do
    final_cleanup
end

$redis_benchmark = find_redis!["redis-benchmark"]
if !$redis_benchmark
    STDERR.puts "Could not find 'redis-benchmark' in your path"
    exit 1
end

proxy_threads = [nil]
if (nthreads = $options[:proxy_threads])
    proxy_threads = nthreads.split(',').map{|n|
        n = n.strip.to_i
        n = 1 if n == 0
        n
    }
end

benchmark_threads = [nil]
if (bthreads = $options[:benchmark_threads])
    benchmark_threads = bthreads.split(',').map{|n|
        n = n.strip.to_i
        n = 1 if n == 0
        n
    }
end

total_tests = 0
started = Time.now.to_f
benchmarks = []
repeat = $options[:repeat] || 1
repeat = repeat.to_i
repeat = 1 if repeat.zero?
proxy_threads.each{|pthreads|
    failed = false
    setup(proxy_threads: pthreads)
    benchmark_threads.each{|bthreads|
        begin
            log "PROXY THREADS: #{pthreads}",nil,:bold if pthreads
            log "BENCHMARK THREADS: #{bthreads}",nil,:bold if bthreads
            repeat.times{|repeat_i|
                out = redis_benchmark($main_proxy.port, $tests, r: 10,
                                      threads: bthreads,
                                      c: $options[:benchmark_clients],
                                      P: $options[:benchmark_pipeline])
                if !$?.success?
                    if File.exists? $bm_err_log
                        out = "#{File.read($bm_err_log)}\n#{out}"
                    end
                    raise out
                end
                if $display == :normal
                    puts RedisProxyTestLogger::colorized(out, :cyan)
                end
                total_tests += $tests.length
                tests = parse_benchmark(out)
                if $display == :short
                    short_out = tests.map{|t|
                        "#{repeat_i + 1}. #{(t[:name] || '')}: " +
                        [:rps, :min_latency, :max_latency].map{|prop|
                            val = t[prop]
                            "#{prop}=#{val}"
                        }.join(', ')
                    }.join("\n")
                    puts RedisProxyTestLogger::colorized(short_out, :cyan)
                end
                benchmarks.push({
                    proxy_threads: pthreads,
                    benchmark_threads: bthreads,
                    output: out,
                    tests: tests
                })
                sleep($options[:wait]) if $options[:wait]
            }
        rescue Exception => e
            STDERR.puts e.to_s.red
            if $?.success?
                STDERR.puts e.backtrace.join("\n").yellow
            end
            failed = true
        end
        if failed
            final_cleanup
            exit 1
        end
    }
    final_cleanup
}
duration = Time.now.to_f - started

best_rps = nil
worst_rps = nil
if total_tests > 1
    benchmarks.each{|bm|
        tests = bm[:tests]
        tests.each{|test|
            rps = test[:rps]
            next if !rps
            if !best_rps || rps > best_rps[:rps]
                best_rps = {
                    proxy_threads: bm[:proxy_threads],
                    benchmark_threads: bm[:benchmark_threads],
                    name: test[:name],
                    rps: rps
                }
            end
            if !worst_rps || rps < worst_rps[:rps]
                worst_rps = {
                    proxy_threads: bm[:proxy_threads],
                    benchmark_threads: bm[:benchmark_threads],
                    name: test[:name],
                    rps: rps
                }
            end
        }
    }
end

if best_rps
    puts "Best RPS:\n" +
         "  Test: #{best_rps[:name]}\n" +
         "  Proxy Threads: #{best_rps[:proxy_threads]}\n" +
         "  Benchmark Threads: #{best_rps[:benchmark_threads]}\n" +
         "  RPS: #{best_rps[:rps]}\n"
    puts ''
end
if worst_rps
    puts "Worst RPS:\n" +
         "  Test: #{worst_rps[:name]}\n" +
         "  Proxy Threads: #{worst_rps[:proxy_threads]}\n" +
         "  Benchmark Threads: #{worst_rps[:benchmark_threads]}\n" +
         "  RPS: #{worst_rps[:rps]}\n"
    puts ''
end
puts "Total benchmarks: #{benchmarks.length}"
puts "Total tests: #{total_tests}"

puts "All tests executed in #{'%.1f' % duration}s".cyan
if (output = $options[:output])
    csv = $options[:csv]
    csv_str = nil
    if csv
        require 'csv'
        if File.extname(output).downcase != 'csv'
            output = "#{output}.csv"
        end
        CSV.open(output, "wb") do |csv|
            csv << ["PROXY_THREADS", "BENCHMARK_THREADS", "TEST",
                    "NUM_REQUESTS", "NUM_CLIENTS", "RPS",
                    "MIN_LATENCY", "MAX_LATENCY", "DURATION"]
            benchmarks.each{|bm|
                pthreads = bm[:proxy_threads]
                bthreads = bm[:benchmark_threads]
                bm[:tests].each{|test|
                    rps = test[:rps]
                    next if !rps
                    csv << [pthreads, bthreads, test[:name],
                            test[:num_requests], test[:num_clients], rps,
                            test[:min_latency], test[:max_latency],
                            test[:duration]]
                }
            }
        end
    else
        File.open(output, 'w'){|f|
            benchmarks.each{|bm|
                f.puts(bm[:output] || '')
            }
        }
    end
    puts "Output written to: '#{output}'"
end
