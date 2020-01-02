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

opts =RedisProxyTestUtils::OptionParser.new help_banner_arguments: '[TESTS]' do

    #option   '',   '--proxy-threads NUM', 'Number of proxy threads'
    option '-c',   '--clients NUM', 'Number of clients', type: :int
    option   '',   '--max-keys NUM', 'Max number of keys', type: :int
    option   '',   '--log-level LEVEL', "Proxy's --log-level (default: debug)"
    option   '',   '--dump-queues',"Proxy's --dump-queues"
    option   '',   '--dump-queries',"Proxy's --dump-queries"
    option   '',   '--keep-logs', "Keep Proxies' logs (if any)"
    option   '',   '--valgrind', 'Valgrind mode'

end

opts.auto_help!
opts.parse!
$options = opts.options

$tests = ARGV
if $tests.length == 0
    $tests = %w(basic basic_commands commands_with_key_callback pipeline 
                multislot client_disconnect node_down proxy_command 
                disable_multiplexing auth multi cluster_errors 
                cluster_errors_multislot)
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
end

def log_exceptions(exceptions)
    logfile = File.join RedisProxyTestCase::LOGDIR, "test_#{urand2hex(6)}.err"
    begin
        proxy_dir = RedisProxyTestCase::PROXYDIR
        git_branch = `git -C "#{proxy_dir}" rev-parse --abbrev-ref HEAD`.strip
        git_commit = `git -C "#{proxy_dir}" rev-parse HEAD`.strip
    rescue Exception => e
        git_branch = nil
        git_commit = nil
    end
    begin
        File.open(logfile, 'w'){|f|
            f.puts "Redis Cluster Proxy Test Exceptions"
            f.puts "Date: #{Time.now}"
            f.puts "Ruby Version: #{RUBY_VERSION} (patch #{RUBY_PATCHLEVEL})"
            f.puts "Platform: #{RUBY_PLATFORM}"
            if git_commit && git_branch && git_commit != '' && git_branch != ''
                f.puts "GIT commit: #{git_commit} on branch (#{git_branch})"
            end
            f.puts "Tests: #{$tests.join(', ')}"
            exceptions.each_with_index{|e, i|
                f.puts "\n-----------------------------\n\n"
                f.puts "Exception ##{i + 1}: #{e.class}"
                f.puts "Message: #{e.to_s}"
                f.puts "Backtrace:"
                e.backtrace.each{|bt|
                    f.puts "- #{bt}"
                }
            }
        }
    rescue Exception => e
        puts e
        return nil
    end
    logfile
end

#$stderr = File.new('/dev/null', 'w') #Silence STDERR
Thread.report_on_exception = false if Thread.respond_to? :report_on_exception

Signal.trap("TERM") do
    final_cleanup
end

failures_count = 0
succeeded_count = 0
skipped_count = 0
started = Time.now.to_f
begin
    $tests.each{|name|
        test = RedisProxyTestCase.new name
        test.run
        failures_count += test.failed_tests.length
        succeeded_count += test.succeeded_tests.length
        skipped_count += test.skipped_tests.length
        break if RedisProxyTestCase::interrupted?
    }
rescue Exception => e
    STDERR.puts e.to_s.red
    STDERR.puts e.backtrace.join("\n").yellow
    RedisProxyTestCase::exceptions << e
ensure
    begin
        final_cleanup
    rescue Exception => e
        # ignore remaining thread expections
    end
end
duration = Time.now.to_f - started

if RedisProxyTestCase::interrupted?
    STDERR.puts RedisProxyTestLogger::colorized("Canceled!", :magenta)
    exit 1
end

puts "All tests executed in #{'%.1f' % duration}s".cyan
if RedisProxyTestCase::exceptions.length == 0
    if failures_count.zero?
        puts ("All #{succeeded_count} test(s) were performed without "+
             "errors!").green
        if !$options[:keep_logs]
            # Clean logs
            pattern = File.join(RedisProxyTestCase::LOGDIR, '*.log')
            Dir.glob(pattern).each{|logfile|
                next if File.basename(logfile)[/^valgrind/]
                begin
                    FileUtils.rm logfile
                rescue Exception => e
                end
            }
        end
    else
        puts "#{succeeded_count} test(s) succeeded without errors".green
        puts "#{failures_count} test(s) failed!".red
    end
    if skipped_count > 0
        puts "#{skipped_count} test(s) skipped!".yellow
    end
else
    msg = "#{RedisProxyTestCase::exceptions.length} exception(s) occurred"
    STDERR.puts RedisProxyTestLogger::colorized(msg, :magenta)
    logfile = log_exceptions RedisProxyTestCase::exceptions
    if logfile
        msg = "See: '#{logfile}'"
        STDERR.puts RedisProxyTestLogger::colorized(msg, :magenta)
    end
end
