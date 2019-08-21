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

    option  '',   '--proxy-threads NUM', 'Number of proxy threads'
    option  '',   '--benchmark-threads NUM', 'Number of benchmark threads'
    option '-o', '--output PATH', 'Output results to a file'
    option  '',   '--csv', 'Set output format to CSV'

end

opts.auto_help!
opts.parse!
$options = opts.options
$tests = ARGV
if $tests.length == 0
    $tests = %w(get set)
end

def setup(proxy_threads: nil)
    if !$main_cluster
        @cluster = RedisCluster.new
        @cluster.restart
        $main_cluster = @cluster
    end
    if !$main_proxy
        @proxy = RedisClusterProxy.new @cluster, verbose: true,
                                       threads: proxy_threads
        @proxy.start
        $main_proxy = @proxy
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
    if tests.is_a?(Array) && tests.length > 0
        cmd += " -t #{tests.join(',')}"
    elsif tests.is_a?(String) && tests.length > 0
        cmd += " #{tests}"
    end
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
proxy_threads.each{|pthreads|
    failed = false
    setup(proxy_threads: pthreads)
    sleep(1)
    benchmark_threads.each{|bthreads|
        begin
            log "PROXY THREADS: #{pthreads}",nil,:bold if pthreads
            log "BENCHMARK THREADS: #{bthreads}",nil,:bold if bthreads
            out = redis_benchmark($main_proxy.port, $tests, r: 10,
                                  threads: bthreads)
            if !$?.success?
                raise out
            end
            puts RedisProxyTestLogger::colorized(out, :cyan)
            total_tests += $tests.length
            tests = parse_benchmark(out)
            benchmarks.push({
                proxy_threads: pthreads,
                benchmark_threads: bthreads,
                output: out,
                tests: tests
            })
        rescue Exception => e
            STDERR.puts e.to_s.red
            STDERR.puts e.backtrace.join("\n").yellow
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
        res = bm[:tests]
        res.each{|test|
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
