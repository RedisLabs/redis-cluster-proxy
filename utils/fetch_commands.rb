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
require 'highline'
require 'redis'
require 'optparse'

$path = File.expand_path(File.dirname(__FILE__))
COPY_START_YEAR = 2019
UNSUPPORTED_COMMANDS = %w()
COMMAND_HANDLERS = {
    'proxy' => 'proxyCommand',
    'multi' => 'multiCommand',
    'exec' => 'execOrDiscardCommand',
    'discard' => 'execOrDiscardCommand'
}
CUSTOM_COMMANDS = [
    {
        name: 'proxy',
        flags: nil,
        arity: -2,
    }
]

class Version

    attr_reader :components

    def initialize(vers)
        @version = vers
        @components = vers.split('.').map{|n| n.to_i}
    end

    def < x
        (@components <=> x.components) < 0
    end

    def > x
        (@components <=> x.components) > 0
    end

    def == x
        (@components <=> x.components) == 0
    end

    def to_s
        @version
    end

end

$options = {redis: {}}

optparse = OptionParser.new do |opts|

        opts.banner = "Usage: #{File.basename($0)} [options]\n"# + 

        opts.on('-h', '--host HOST',
                "Redis host"){|h|
            $options[:redis][:host] = h
        }

        opts.on('-p', '--port PORT',
                "Redis host"){|port|
            $options[:redis][:port] = port.to_i
        }

        opts.on('-f', '--flags', 'Generate flags') {
            $options[:flags] = true
        }

        opts.on( '', '--help', 'Display this screen' ) do
            puts opts
            exit
        end

end

optparse.parse!

$h = HighLine.new

$redis = Redis.new $options[:redis]
versfile = File.join $path, 'commands_cur_version.txt'
if File.exists? versfile
    vers = File.read(versfile).strip
    $curvers = Version.new vers
end
$redis_version = $redis.info['redis_version']
puts "Redis Version: #{$redis_version}"
puts "Current Version: #{$curvers.to_s}"
if !$redis_version || $redis_version.empty?
    STDERR.puts "Cannot fetch redis version"
    exit 1
end

$redis_version = Version.new $redis_version
if $curvers && $redis_version < $curvers
    puts "WARNING: current version is less than latest used version!"
    ok = $h.ask("Are you sure to continue? (y/N) ").strip.downcase
    exit 0 if ok != 'y'
end

$all_flags = %w(REDIS_COMMAND_FLAG_NONE)
commands = $redis.command
commands = commands.map{|c| 
    name, arity, flags, first_key, last_key, key_step = c
    name = name.downcase
    if $options[:flags]
        flags = flags.map.each{|flag|
            flag_name =
                "REDIS_COMMAND_FLAG_#{flag.gsub(/[\s_\-]+/, '_').upcase}"
            $all_flags |= [flag_name]
            flag_name
        }
        if flags.length == 0
            flags = 'REDIS_COMMAND_FLAG_NONE'
        else
            flags = flags.join(' | ')
        end
    end
    unsupported = (UNSUPPORTED_COMMANDS.include?(name) ? 1 : 0)
    code =  "    {#{name.inspect}, #{arity.to_i},"
    if $options[:flags]
        code << "\n     #{flags},    \n"
    else
        code << ' '
    end
    handler = COMMAND_HANDLERS[name] || 'NULL'
    code << "#{first_key.to_i}, #{last_key.to_i}, #{key_step.to_i}, "
    code << "#{unsupported}, #{handler}}"
    code
}
custom_commands = CUSTOM_COMMANDS.map{|cmd|
    code = "    {#{cmd[:name].inspect}, #{cmd[:arity].to_i},"
    if $options[:flags]
        flags = cmd[:flags] || []
        flags = flags.map.each{|flag|
            flag_name =
                "REDIS_COMMAND_FLAG_#{flag.gsub(/[\s_\-]+/, '_').upcase}"
            $all_flags |= [flag_name]
            flag_name
        }
        if flags.length == 0
            flags = 'REDIS_COMMAND_FLAG_NONE'
        else
            flags = flags.join(' | ')
        end
        code << "\n     #{flags},    \n"
    else
        code << ' '
    end
    first_key, last_key, key_step = cmd[:first_key], cmd[:last_key],
                                    cmd[:key_step]
    handler = COMMAND_HANDLERS[cmd[:name]] || 'NULL'
    code << "#{first_key.to_i}, #{last_key.to_i}, #{key_step.to_i}, "
    code << "0, #{handler}}"
    code
}

cur_year = Time.now.year.to_i
copy_year = "#{COPY_START_YEAR}"
if cur_year > COPY_START_YEAR
    copy_year << "-#{cur_year}"
end
header = File.read(File.join($path, 'src_header.h'))
header.gsub! '$CP_YEAR', copy_year
handler_def = "typedef int redisClusterProxyCommandHandler(void *);"
struct = <<EOS
typedef struct redisCommandDef {
    char *name;
    int arity;
    int flags;
    int first_key;
    int last_key;
    int key_step;
    int unsupported;
    redisClusterProxyCommandHandler* handle;
} redisCommandDef;
EOS

#p struct
if !$options[:flags]
    struct.gsub! /\s+int flags;/, ''
end

tot_commands = commands.length + custom_commands.length

flag_idx = 0
code = header + "\n\n" + 
"#ifndef __REDIS_CLUSTER_PROXY_COMMANDS_H__\n"+
"#define __REDIS_CLUSTER_PROXY_COMMANDS_H__\n\n"
code << "#include <stdlib.h>\n\n"
code << "#define PROXY_COMMAND_HANDLED      1\n"
code << "#define PROXY_COMMAND_UNHANDLED    0\n\n"
if $options[:flags]
    code << $all_flags.map{|flag|
        if flag_idx == 0
            flag_val = '0'
        else
            flag_val = "1 << #{flag_idx - 1}"
        end
        flag_idx += 1
        "#define #{flag} #{flag_val}"
    }.join("\n") + "\n\n"
end
code << "#{handler_def}\n\n"
code << struct + "\n\n"
code << "extern struct redisCommandDef redisCommandTable[#{tot_commands}];\n\n"
code << "#endif /* __REDIS_CLUSTER_PROXY_COMMANDS_H__  */\n"
#puts code
outfile = File.join $path, 'commands.h'
File.open(outfile, 'w'){|f| f.write(code)}
puts "Code written to: #{outfile}"

code = header + "\n\n" + 
"#include \"commands.h\"\n\n"
if COMMAND_HANDLERS.length > 0
    code << "/* Command Handlers */\n"
    COMMAND_HANDLERS.each{|cmdname, func|
        code << "int #{func}(void *req);\n"
    }
    code << "\n"
end
code << "struct redisCommandDef redisCommandTable[#{tot_commands}] = {\n"
code << commands.join(",\n")
if custom_commands.length > 0
    code << ",\n"
    code << "    /* Custom Commands */\n"
    code << custom_commands.join(",\n")
end
code << "\n"
code << "};\n"

#puts code
outfile = File.join $path, 'commands.c'
File.open(outfile, 'w'){|f| f.write(code)}
puts "Code written to: #{outfile}"
File.open(versfile, 'w'){|f| f.write($redis_version.to_s)}
