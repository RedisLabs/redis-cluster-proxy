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

module RedisProxyTestLogger

    STYLES = {
        bold: 1,
        dim: 2,
        italic: 3,
        underline: 4,
        blink: 5
    }

    COLORS = {
        white: 29,
        black: 30,
        red: 31,
        green: 32,
        yellow: 33,
        blue: 34,
        magenta: 35,
        cyan: 36,
        gray: 37,
        reset: 0
    }

    def RedisProxyTestLogger::colorized(string, color = nil, *styles)
        if color || styles.length > 0
            color ||= :white
            color_code = COLORS[color]
            raise "Invalid color #{color}" if !color_code
            prefix = "\e[#{color_code}"
            styles = styles.map{|style|
                style_code = STYLES[style]
                raise "Invalid style #{style}" if !style_code
                style_code
            }.join(';')
            prefix << ";#{styles}" if !styles.empty?
            prefix << 'm'
            string = "#{prefix}#{string}\e[0m"
        end
        string
    end

    def colorized(string, color = nil, *styles)
        RedisProxyTestLogger::colorized(string, color, *styles)
    end

    def RedisProxyTestLogger::log(msg, color = nil, *styles)
        puts RedisProxyTestLogger::colorized(msg, color, *styles)
    end

    def log(msg, color = nil, *styles)
        RedisProxyTestLogger::log(msg, color, *styles)
    end

    def log_same_line(msg, color = nil, *styles, linesize: 79)
        pad = linesize - msg.length
        STDOUT.flush
        msg = RedisProxyTestLogger::colorized(msg, color, *styles)
        print("\r#{msg}#{' ' * pad}")
        STDOUT.flush
        print("\r")
    end

    def log_exception(e)
        STDERR.puts ''
        STDERR.puts(e.to_s.red)
        STDERR.puts(e.backtrace.join("\n").yellow)
    end

end

class String

    RedisProxyTestLogger::COLORS.keys.each{|color|
        define_method(color){
            RedisProxyTestLogger::colorized(self, color)
        }
    }

end
