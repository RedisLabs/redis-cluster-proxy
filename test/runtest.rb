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

$tests = ARGV
if $tests.length == 0
    $tests = %w(basic basic_commands pipeline client_disconnect node_down
                proxy_command disable_multiplexing)
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

Signal.trap("TERM") do
    final_cleanup
end

failures_count = 0
succeeded_count = 0
started = Time.now.to_f
begin
    $tests.each{|name|
        test = RedisProxyTestCase.new name
        test.run
        failures_count += test.failed_tests.length
        succeeded_count += test.succeeded_tests.length
    }
rescue Exception => e
    STDERR.puts e.to_s.red
    STDERR.puts e.backtrace.join("\n").yellow
    RedisProxyTestCase::exceptions << e
ensure
    final_cleanup
end
duration = Time.now.to_f - started

puts "All tests executed in #{'%.1f' % duration}s".cyan
if RedisProxyTestCase::exceptions.length == 0
    if failures_count.zero?
        puts ("All #{succeeded_count} test(s) were performed without "+
             "errors!").green
        # Clean logs
        Dir.glob(File.join(RedisProxyTestCase::LOGDIR, '*.log')).each{|logfile|
            begin
                FileUtils.rm logfile
            rescue Exception => e
            end
        }
    else
        puts "#{succeeded_count} test(s) succeeded without errors".green
        puts "#{failures_count} test(s) failed!".red
    end
else
    STDERR.puts "#{RedisProxyTestCase::exceptions.length} exception(s) occurred"
end
