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

require 'fileutils'
require 'bundler/setup'
require 'redis'
require 'hiredis'

$redis_proxy_test_libdir ||= File.expand_path(File.dirname(__FILE__))
load File.join($redis_proxy_test_libdir, 'crc16_slottable.rb')
load File.join($redis_proxy_test_libdir, 'log.rb')
load File.join($redis_proxy_test_libdir, 'helpers.rb')
load File.join($redis_proxy_test_libdir, 'cluster.rb')
load File.join($redis_proxy_test_libdir, 'proxy.rb')
load File.join($redis_proxy_test_libdir, 'optparser.rb')

class RedisProxyTestCase

    include RedisProxyTestLogger

    LIBDIR = $redis_proxy_test_libdir
    ROOTDIR = ($redis_proxy_test_dir ||= File.dirname(LIBDIR))
    TESTSDIR = File.join(ROOTDIR, 'tests')
    TMPDIR = File.join(ROOTDIR, 'tmp')
    FileUtils.mkdir_p(TMPDIR) if !File.exists?(TMPDIR)
    LOGDIR = File.join(TMPDIR, 'log')
    FileUtils.mkdir_p(LOGDIR) if !File.exists?(LOGDIR)

    @@exceptions = []

    GenericSetup = proc{
        $options ||= {}
        use_valgrind = $options[:valgrind] == true
        loglevel = $options[:log_level] || 'debug'
        dump_queues = $options[:dump_queues]
        dump_queries = $options[:dump_queries]
        if !$main_cluster
            @cluster = RedisCluster.new
            @cluster.restart
            $main_cluster = @cluster
        end
        if !$main_proxy
            @proxy = RedisClusterProxy.new @cluster, log_level: loglevel,
                                                     dump_queues: dump_queues,
                                                     dump_queries: dump_queries,
                                                     valgrind: use_valgrind
            @proxy.start
            $main_proxy = @proxy
        end
    }

    attr_reader :name, :testfile, :tests, :failed_tests, :succeeded_tests,
                :current_test, :started, :duration

    def initialize(name, testfile: nil)
        @name = name
        @tests = []
        @failed_tests = []
        @succeeded_tests = []
        @current_test = nil
        if !@testfile
            @testfile = "#{name}"
            @testfile << '.rb' if File.extname(@testfile) != '.rb'
            @testfile = File.join TESTSDIR, @testfile
        end
        if !File.exists?(@testfile)
            STDERR.puts "Could not find test file: '#{@testfile}'".red
            exit 1
        end
        self.instance_eval(File.read(@testfile), @testfile)
    end

    def run
        log "TESTING #{@name.gsub(/_+/, ' ').upcase}", :cyan
        @started = Time.now.to_f
        failed_setup = false
        if @setup
            begin
                instance_eval(&@setup)
            rescue AssertionFailure => message
                message ||= 'assertion failure'
                log message.red
                #return false
                failed_setup = true
            rescue Exception => e
                on_exception(e)
                failed_setup = true
                #return false
            end
        end
        if !failed_setup
            @tests.each{|test|
                run_test(test)
            }
        end
        if @cleanup
            begin
                instance_eval(&@cleanup)
            rescue AssertionFailure => message
                message ||= 'assertion failure'
                log message.red
            rescue Exception => e
                on_exception(e)
            end
        end
        @duration = Time.now.to_f - @started
    end

    def setup(&block)
        @setup = block
    end

    def cleanup(&block)
        @cleanup = block
    end

    def test(name, &block)
        @tests << {
            name: name,
            exec: block
        }
    end

    def run_test(test)
        @current_test = test
        failed = false
        begin
            started = Time.now.to_f
            instance_eval &(test[:exec])
        rescue AssertionFailure => message
            test[:failed] = failed = true
            test[:failure] = message if message
        rescue Exception => e
            test[:failed] = false
            on_exception(e)
        ensure
            duration = Time.now.to_f - started
            test[:duration] = duration
        end
        if failed
            status = 'FAIL'.red
            @failed_tests << test
        else
            status = 'OK'.green
            @succeeded_tests << test
        end
        log_same_line('')
        message = "[#{status}] #{test[:name]}"
        puts message
        if test[:failure]
            puts test[:failure].to_s.red
        end
        !failed
    end

    def spawn_clients(num, proxy: nil, &block)
        #Thread.abort_on_exception = true
        proxy ||= (@proxy || $main_proxy)
        if !proxy
            log("WARN: missing 'proxy'", :yellow)
            return
        end
        threads = []
        (0...num).each{|tidx|
            t = Thread.new{
                r = Redis.new port: proxy.port
                block.call(r, tidx)
            }
            #t.abort_on_exception = true
            threads << t
        }
        threads.each{|t| t.join}
    end

    def redis_command(client, command, *args)
        begin
            client.send command, *args
        rescue Redis::CommandError => cmderr
            cmderr
        end
    end

    def assert(expr, message = nil)
        if !expr
            message ||= "assertion failure"
            raise AssertionFailure, message
        end
    end

    def assert_equal(a, b, message = nil)
        assert((a == b), message || "#{a.inspect} != #{b.inspect}")
    end

    def assert_nil(o, message = nil)
        assert(o.nil?, message || "#{o.inspect} is not 'nil'")
    end

    def assert_not_nil(o, message = nil)
        assert(!(o.nil?), message || "#{o.inspect} is 'nil'")
    end

    def assert_match(str, tomatch, message = nil)
        message ||= "Could not match #{tomatch.inspect} into #{str.inspect}"
        assert(!str[tomatch].nil?, message)
    end

    def assert_redis_err(reply, message = nil)
        message ||= "#{reply.to_s} is not a redis error"
        assert(reply.is_a?(Redis::CommandError), message)
    end

    def assert_not_redis_err(reply, message = nil)
        message ||=
        "Expected valid reply, but redis replied with error:\n'#{reply.to_s}'\n"
        assert(!reply.is_a?(Redis::CommandError), message)
    end

    def log_test_update(message, test = nil)
        test ||= @current_test
        log_same_line("[  ] #{test[:name]} #{message}")
    end

    def on_exception(e)
        @@exceptions << e
        log_exception(e)
    end

    def RedisProxyTestCase::exceptions
        @@exceptions
    end

    class AssertionFailure < Exception
    end

end
