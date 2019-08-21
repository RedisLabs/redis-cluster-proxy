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

require 'socket'

def urand2hex(len)
    ur=File.read('/dev/urandom',len)
    s=''
    ur.each_byte{|b|
        s<<("%02x" % b)
    }
    s
end

def shell_exec(cmd, verbose: false, auto_answer: nil, return_value: :output)
    if auto_answer
        cmd = "echo \"#{auto_answer}\" | #{cmd}"
    end
    puts cmd.gray if verbose
    out = `#{cmd}`
    ok = $?.success?
    puts out.gray if verbose
    if return_value == :output
        out
    elsif return_value == :status
        ok
    end
end

def find_redis!
    return $redis_paths if $redis_paths
    paths = {}
    redis_home = ENV['REDIS_HOME']
    %w(server cli benchmark).each{|progname|
        progname = "redis-#{progname}"
        path = nil
        if redis_home
            path = File.join redis_home, progname
            if !File.exists?(redis_home)
                RedisProxyTestLogger::log("WARNING: '#{path}' not found!",
                                          :yellow)
                path = nil
            end
        end
        if !path
            path = `which #{progname}`.strip
            path = nil if path.empty?
        end
        paths[progname] = path
    }
    $redis_paths = paths
end

def is_port_available?(port, ip = '127.0.0.1')
    begin
        TCPSocket.new(ip, port)
    rescue Errno::ECONNREFUSED
        return true
    end
    return false
end

def find_available_ports(from, count = 1)
    available = []
    (from...(from + 1024)).each{|port|
        break if available.length == count
        if is_port_available?(port)
            available << port
        end
    }
    available
end

def find_available_port(from)
    find_available_ports(from)[0]
end
