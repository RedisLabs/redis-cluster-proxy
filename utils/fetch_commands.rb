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
UNSUPPORTED_COMMANDS = %w(
    subscribe psubscribe debug role migrate acl shutdown info wait
    slaveof replconf time monitor config latency unsubscribe
    replicaof pfselftest lastslave slowlog 
    publish cluster sync readwrite asking
    script randomkey module pfdebug pubsub
    hello memory psync client readonly punsubscribe
)
COMMAND_HANDLERS = {
    'proxy' => 'proxyCommand',
    'multi' => 'multiCommand',
    'exec' => 'execOrDiscardCommand',
    'discard' => 'execOrDiscardCommand',
    'watch' => 'commandWithPrivateConnection',
    'unwatch' => 'commandWithPrivateConnection',
    'blpop' => 'commandWithPrivateConnection',
    'brpop' => 'commandWithPrivateConnection',
    'bzpopmin' => 'commandWithPrivateConnection',
    'bzpopmax' => 'commandWithPrivateConnection',
    'xread' => 'xreadCommand',
    'xreadgroup' => 'xreadCommand',
    'post' => 'securityWarningCommand',
    'host:' => 'securityWarningCommand',
    'ping' => 'pingCommand',
    'auth' => 'authCommand',
    'scan' => 'scanCommand',
    #'randomkey' => 'randomKeyCommand',
}
REPLY_HANDLERS = {
    'mget' => 'mergeReplies',
    'mset' => 'getFirstMultipleReply',
    'dbsize' => 'sumReplies',
    'touch' => 'sumReplies',
    'exists' => 'sumReplies',
    'del' => 'sumReplies',
    'unlink' => 'sumReplies',
    'auth' => 'getFirstMultipleReply',
    'echo' => 'getFirstMultipleReply', # proxy echoCommand ?
    'flushdb' => 'getFirstMultipleReply',
    'flushall' => 'getFirstMultipleReply',
    'lolwut' => 'getFirstMultipleReply',
    'save' => 'getFirstMultipleReply',
    'bgsave' => 'getFirstMultipleReply',
    'keys' => 'mergeReplies',
    'watch' => 'getFirstMultipleReply',
    'unwatch' => 'getFirstMultipleReply',
    'scan' => 'handleScanReply',
    'command' => 'getFirstMultipleReply',
}
GET_KEYS_PROC = {
    'zunionstore' => 'zunionInterGetKeys',
    'zinterstore' => 'zunionInterGetKeys',
    'xread' => 'xreadGetKeys',
    'xreadgroup' => 'xreadGetKeys',
    'sort' => 'sortGetKeys',
    'eval' => 'evalGetKeys',
    'evalsha' => 'evalGetKeys',
}
CUSTOM_COMMANDS = [
    {
        name: 'proxy',
        flags: nil,
        arity: -2,
    }
]

PROXY_FLAGS = {
    MULTISLOT_UNSUPPORTED: '1 << 0',
    DUPLICATE: '1 << 1',
    HANDLE_REPLY: '1 << 2',
}

PROXY_COMMANDS_FLAGS = {
    MULTISLOT_UNSUPPORTED: {
        'zunionstore' => true,
        'zinterstore' => true,
        'sunionstore' => true,
        'sinterstore' => true,
        'sdiffstore' => true,
        'xread' => true,
        'xreadgroup' => true
    },
    DUPLICATE: {
        'keys' => true,
        'unwatch' => true,
        'auth' => true,
    },
    HANDLE_REPLY: {
        'scan' => true,
    }
}

CMDFLAG_PRFX = "CMDFLAG"

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

        opts.on('', '--csv', 'Generate CSV list') {
            $options[:csv] = true
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

csv = [
    ['NAME', 'ARITY', 'FIRST KEY', 'LAST KEY', 'KEY STEP', 'UNSUPPORTED',
     'CUSTOM', 'PROXY FLAGS', 'GET KEYS CALLBACK', 'REPLY HANDLER', 'HANDLER']
]
doc = {
    supported: [],
    unsupported: []
}
$all_flags = %w(REDIS_COMMAND_FLAG_NONE)
commands = $redis.command
commands = commands.map{|c|
    name, arity, flags, first_key, last_key, key_step = c
    name = name.downcase
    cmdflags = flags.dup
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
    proxy_flags = []
    PROXY_FLAGS.each{|flag, val|
        flag_commands = PROXY_COMMANDS_FLAGS[flag]
        next if !flag_commands
        proxy_flags << "#{CMDFLAG_PRFX}_#{flag}" if flag_commands[name]
    }
    if proxy_flags.length > 0
        proxy_flags = proxy_flags.join(' | ')
    else
        proxy_flags = 0
    end
    unsupported = (UNSUPPORTED_COMMANDS.include?(name) ? 1 : 0)
    unsupported = 1 if unsupported.zero? && cmdflags.include?('pubsub')
    code =  "    {#{name.inspect}, #{arity.to_i},"
    if $options[:flags]
        code << "\n     #{flags},\n    "
    else
        code << ' '
    end
    handler = COMMAND_HANDLERS[name] || 'NULL'
    reply_handler = REPLY_HANDLERS[name] || 'NULL'
    get_keys = GET_KEYS_PROC[name] || 'NULL'
    if $options[:csv]
        csv << [
            name, arity, first_key, last_key, key_step,
            (unsupported == 1 ? 'unsupported' : ''), '',
            proxy_flags,
            (get_keys != 'NULL' ? get_keys : ''),
            (reply_handler != 'NULL' ? reply_handler : ''),
            (handler != 'NULL' ? handler : ''),
        ]
    end
    code << "#{first_key.to_i}, #{last_key.to_i}, #{key_step.to_i},"
    if proxy_flags != 0
        if code[-1, 1] == ' '
            code.chop!
        end
        code << "\n     #{proxy_flags},\n     "
    else
        code << " 0, "
    end
    code << "#{unsupported}, "
    code << "#{get_keys}, #{handler}, #{reply_handler}}"
    doc_info = {
        name: name.upcase,
        disables_multiplexing: (handler == 'multiCommand' ||
                                handler == 'commandWithPrivateConnection'),
        can_disable_multiplexing: (handler == 'xreadCommand'),
        "cross-slots unsupported":
            PROXY_COMMANDS_FLAGS[:MULTISLOT_UNSUPPORTED][name],
        sums_multiple_replies: reply_handler == 'sumReplies',
        merges_multiple_replies: reply_handler == 'mergeReplies',
    }
    doc[(unsupported.zero? ? :supported : :unsupported)] << doc_info
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
    reply_handler = REPLY_HANDLERS[cmd[:name]] || 'NULL'
    get_keys = GET_KEYS_PROC[cmd[:name]] || 'NULL'
    if $options[:csv]
        csv << [
            cmd[:name].to_s,
            cmd[:arity].to_i, cmd[:first_key], cmd[:last_key], cmd[:key_step],
            '',
            'custom',
            0,
            (get_keys != 'NULL' ? get_keys : ''),
            (reply_handler != 'NULL' ? reply_handler : ''),
            (handler != 'NULL' ? handler : ''),
        ]
    end
    code << "#{first_key.to_i}, #{last_key.to_i}, #{key_step.to_i}, "
    code << "0, 0, #{get_keys}, #{handler}, #{reply_handler}}"
    code
}

cur_year = Time.now.year.to_i
copy_year = "#{COPY_START_YEAR}"
if cur_year > COPY_START_YEAR
    copy_year << "-#{cur_year}"
end
header = File.read(File.join($path, 'src_header.h'))
header.gsub! '$CP_YEAR', copy_year
handler_def =
    "typedef int redisClusterProxyCommandHandler(void *request);\n" +
    "typedef int redisClusterProxyReplyHandler(void *reply, void *request,\n" +
    "   char *buf, int len);\n" +
    "\n" +
    "/* Callback used to get key indices in some special commands, returns\n" +
    " * the number of keys or -1 if some error occurs.\n" +
    " * Arguments:\n" +
    " *     *req: pointer to the request\n" +
    " *     *first_key, *last_key, *key_step: pointers to keys indices/step\n"+
    " *     **skip: pointer to an array of indices (must be allocated and\n" +
    " *             freed outside) that must be skipped.\n" +
    " *     *skiplen: used to indicate the length of *skip array\n"+
    " *     **err: used for an eventual error message (when -1 is returned)\n"+
    " */\n" +
    "typedef int redisClusterGetKeysCallback(void *req, int *first_key,\n" +
    "   int *last_key, int *key_step, int **skip, int *skiplen, " +
    "char **err);\n";
struct = <<EOS
typedef struct redisCommandDef {
    char *name;
    int arity;
    int flags;
    int first_key;
    int last_key;
    int key_step;
    int proxy_flags;
    int unsupported;
    redisClusterGetKeysCallback     *get_keys;
    redisClusterProxyCommandHandler *handle;
    redisClusterProxyReplyHandler   *handleReply;
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
code << "#define PROXY_REPLY_UNHANDLED      2\n\n"
PROXY_FLAGS.each{|flag, val|
    code << "#define #{CMDFLAG_PRFX}_#{flag} #{val}\n"
    code << "\n" if flag == PROXY_FLAGS.keys.last
}
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
    added = {}
    COMMAND_HANDLERS.each{|cmdname, func|
        next if added[func]
        code << "int #{func}(void *req);\n"
        added[func] = true
    }
    code << "\n"
end
if REPLY_HANDLERS.length > 0
    code << "/* Reply Handlers */\n"
    added = {}
    REPLY_HANDLERS.each{|cmdname, func|
        next if added[func]
        code << "int #{func}(void *reply, void *request, char *buf, int len);\n"
        added[func] = true
    }
    code << "\n"
end
if GET_KEYS_PROC.length > 0
    code << "/* Get Keys Callbacks */\n"
    added = {}
    GET_KEYS_PROC.each{|cmdname, func|
        next if added[func]
        code << "int #{func}(void *req, int *first_key, int *last_key,\n" +
                "   int *key_step, int **skip, int *skiplen, char **err);\n"
        added[func] = true
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

# Generate DOC

doc_content = File.read(File.join($path, 'commands_tpl.md'))
[:supported, :unsupported].each{|type|
    list = doc[type].sort_by{|c| c[:name]}.map{|c|
        l = " - #{c[:name]}"
        extra = c.keys.map{|k|
            next if k == :name
            val = c[k]
            next if !val
            "**#{k.to_s.gsub(/_+/, ' ')}**"
        }.compact.join(', ')
        l << " (#{extra})" if !extra.strip.empty?
        l
    }.join("\n")
    doc_content = doc_content.gsub "%%#{type.to_s.upcase}_COMMANDS_LIST%%", list
}
docfile = File.join $path, 'COMMANDS.md'
File.open(docfile, 'w:utf-8'){|f|
    f.write doc_content
}
puts "Doc written to: #{docfile}"

if $options[:csv]
    require 'csv'
    csvfile = File.join $path, 'proxy_commands.csv'
    CSV.open(csvfile, "wb") do |csv_obj|
        csv.each{|l|
            csv_obj << l
        }
    end
    puts "CSV written to: #{csvfile}"
end
