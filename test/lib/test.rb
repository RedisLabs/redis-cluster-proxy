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
    PROXYDIR = File.dirname(ROOTDIR)

    @@exceptions = []
    @@interrupted = false

    GenericSetup = proc{
        $options ||= {}
        use_valgrind = $options[:valgrind] == true
        loglevel = $options[:log_level] || 'debug'
        dump_queues = $options[:dump_queues]
        dump_queries = $options[:dump_queries]
        dump_buffer = $options[:dump_buffer]
        if !$main_cluster
            @cluster = RedisCluster.new
            @cluster.restart
            $main_cluster = @cluster
        end
        if !$main_proxy
            @proxy = RedisClusterProxy.new @cluster, log_level: loglevel,
                                                     dump_queues: dump_queues,
                                                     dump_queries: dump_queries,
                                                     dump_buffer: dump_buffer,
                                                     valgrind: use_valgrind
            @proxy.start
            $main_proxy = @proxy
        end
    }

    attr_reader :name, :testfile, :tests, :failed_tests, :succeeded_tests,
                :skipped_tests, :current_test, :started, :duration,
                :repeat

    def initialize(name, testfile: nil)
        @name = name
        @tests = []
        @failed_tests = []
        @succeeded_tests = []
        @skipped_tests = []
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
        if $options
            @repeat = $options[:repeat]
        end
        @repeat ||= 1
        self.instance_eval(File.read(@testfile), @testfile)
    end

    def run
        log "TESTING #{@name.gsub(/_+/, ' ').upcase}", :cyan
        @started = Time.now.to_f
        failed_setup = false
        interrupted = false
        if @setup
            begin
                instance_eval(&@setup)
            rescue AssertionFailure => message
                message ||= 'assertion failure'
                log message.to_s.red
                #return false
                failed_setup = true
            rescue Exception => e
                on_exception(e)
                failed_setup = true
                @@interrupted = true if e.is_a? Interrupt
                #return false
            end
        end
        if !failed_setup
            @repeat.times.each{|r|
                @tests.each{|test|
                    run_test(test)
                    if test[:exception].is_a?(Interrupt)
                        interrupted = true
                        break
                    end
                }
            }
        end
        if @cleanup
            begin
                instance_eval(&@cleanup)
            rescue AssertionFailure => message
                message ||= 'assertion failure'
                log message.to_s.red
            rescue Exception => e
                on_exception(e)
                @@interrupted = true if e.is_a? Interrupt
            end
        end
        @duration = Time.now.to_f - @started
        @@interrupted = true if interrupted
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
        interrupt = false
        skipped = false
        begin
            started = Time.now.to_f
            instance_eval &(test[:exec])
        rescue AssertionFailure => message
            test[:failed] = failed = true
            test[:failure] = message if message
        rescue SkipTestException => message
            test[:skipped_because] = message
            skipped = true
        rescue Exception => e
            interrupt = e.is_a?(Interrupt)
            test[:failed] = failed = true if !interrupt
            test[:exception] = e
            on_exception(e)
        ensure
            duration = Time.now.to_f - started
            test[:duration] = duration
        end
        if failed
            status = 'FAIL'.red
            @failed_tests << test
        elsif interrupt
            status = 'CANCEL'.magenta
        elsif skipped
            status = 'SKIPPED'.cyan
            @skipped_tests << test
        else
            status = 'OK'.green
            @succeeded_tests << test
        end
        log_same_line('')
        message = "[#{status}] #{test[:name]}"
        puts message
        if test[:failure]
            puts test[:failure].to_s.red
        elsif test[:skipped_because]
            puts ("-> Skip reason: " + test[:skipped_because].to_s).yellow
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

    def assert_class(obj, cls, message = nil)
        if !cls.is_a? Class
            raise "assert_class(obj,cls,message): cls is expected to be a "+
                  "Ruby Class."
            return false
        end
        message ||= "Object is expected to be a #{cls.to_s}, but it's a " +
            "#{obj.class.to_s}"
        assert(obj.is_a?(cls), message)
    end

    def skip_test(message)
        message ||= "Test skipped"
        raise SkipTestException, message
    end

    def log_test_update(message, test = nil)
        test ||= @current_test
        log_same_line("[  ] #{test[:name]} #{message}")
    end

    def log_to_proxy(proxy, msg)
        proxy.redis_command :proxy, 'log', msg
    end

    def on_exception(e)
        if !e.is_a? Interrupt
            t = Time.now
            RedisProxyTestCase::set_exception_time(e, t)
            @@exceptions << e
            log_exception(e)
        else
            STDERR.flush
            STDOUT.flush
            STDERR.puts colorized("\nInterrupt!\n", :magenta)
            STDERR.flush
            STDOUT.flush
        end
    end

    def interrupted?
        @@interrupted
    end

    def RedisProxyTestCase::interrupted?
        @@interrupted
    end

    def RedisProxyTestCase::exceptions
        @@exceptions
    end

    def RedisProxyTestCase::set_exception_time(e, t = nil)
        t ||= Time.now
        begin
            e.instance_eval{@_proxy_exception_t = t}
        rescue Exception => e
        end
    end

    def RedisProxyTestCase::get_exception_time(e)
        begin
            e.instance_eval{@_proxy_exception_t}
        rescue Exception => e
            nil
        end
    end

    class AssertionFailure < Exception
    end

    class SkipTestException < Exception
    end

end
