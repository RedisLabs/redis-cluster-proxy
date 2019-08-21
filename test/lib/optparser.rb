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

module RedisProxyTestUtils

    class OptionParser

        attr_reader :options

        def initialize(**conf, &block)
            @help_banner_arguments = conf[:help_banner_arguments]
            @options = {}
            @available_options = []
            @parser_options = {}
            @parsed = false
            if block
                instance_eval(&block)
            end
        end

        def option(short, long, descr, name: nil, default: nil, type: nil, &b)
            if short
                short.strip!
                short = nil if short.empty?
            end
            if long
                long.strip!
                long = nil if long.empty?
            end
            if !long && !short
                raise "You must specify at least 'short' or 'long' option name"
                exit 1
            end
            opt = {
                short: short,
                long: long,
                descr: descr,
                name: name,
                default: default,
                type: type,
                block: b
            }
            help_def = []
            [:short, :long].each{|key|
                if (df = opt[key])
                    if df[0, 1] != '-'
                        prfx = (key == :short ? '-' : '--')
                        df = (opt[key] = "#{prfx}#{df}")
                    end
                    help_def << df
                    df,arg = df.split /[[:space:]]/, 2
                    opt[:"#{key}_name"] = df
                    opt[:argument_name] = arg if arg
                end
            }
            opt[:help] = help_def.join(', ')
            if !opt[:name] && opt[:long_name]
                n = opt[:long_name].sub /^\-+/, ''
                n = n.gsub /\-+/, '_'
                opt[:name] = :"#{n.downcase}"
            end
            @available_options << opt
            @parser_options[opt[:short_name]] = opt if opt[:short_name]
            @parser_options[opt[:long_name]] = opt if opt[:long_name]
            opt
        end

        def parse!
            return @options if @parsed
            current_opt = nil
            opt_range = []
            ARGV.each_with_index{|arg, idx|
                next if opt_range.length == 2
                val = nil
                if arg[0, 1] == '-'
                    opt = @parser_options[arg]
                    if !opt
                        STDERR.puts "Invalid option: '#{arg}'"
                        exit 1
                    end
                    if current_opt
                        STDERR.puts "Missing argument for optiion: '#{arg}'"
                        exit 1
                    end
                    current_opt = opt
                    opt_range[0] ||= idx
                    next if current_opt[:argument_name]
                    val = true
                else
                    if !current_opt
                        if opt_range[0]
                            opt_range[1] ||= (idx - 1)
                        end
                    else
                        val = arg
                    end
                end
                if current_opt
                    if (block = current_opt[:block])
                        block.call(val)
                    else
                        if (type = current_opt[:type]) && val
                            if type == :int
                                val = val.to_i
                            elsif type == :float
                                val = val.to_f
                            elsif type == :bool
                                val = val.downcase
                                val = (val == 'true' || val == 'yes')
                            end
                        end
                    end
                    name = current_opt[:name]
                    if !name
                        name = current_opt[:short_name].gsub('-', '')
                    end
                    name = name.to_sym if !name.is_a?(Symbol)
                    @options[name] = val
                    current_opt = nil
                end
            }
            opt_range[0] ||= 0 if opt_range[1]
            opt_range[1] ||= -1 if opt_range[0]
            @available_options.each{|opt|
                df_val = opt[:default]
                next if df_val.nil?
                name = opt[:name]
                if !name
                    name = opt[:short_name].gsub('-', '')
                end
                name = name.to_sym if !name.is_a?(Symbol)
                if !@options.key?(name)
                    @options[name] = df_val
                end
            }
            ARGV.slice!(opt_range[0]..opt_range[1]) if opt_range.length == 2
            @parsed = true
            @options
        end

        def auto_help!(short: nil, long: nil, descr: 'Print this help')
            short ||= '-h'
            long ||= '--help'
            short = nil if @parser_options[short]
            long = nil if @parser_options[long]
            if !short && !long
                raise "Short and long form of help already defined!"
                exit 1
            end
            me = self
            option(short, long, descr){
                me.print_help
                exit 1
            }
        end

        def help_string
            maxlen = 0
            @available_options.each{|opt|
                help_str = opt[:help]
                next if !help_str
                len = help_str.length
                maxlen = len if len > maxlen
            }
            args = @help_banner_arguments || '[ARGS]'
            "Usage: #{$0} [OPTIONS] #{args}\n" +
            @available_options.map{|opt|
                help_str = opt[:help]
                next if !help_str
                "    #{help_str.ljust(maxlen)}    #{opt[:descr] || ''}"
            }.compact.join("\n")
        end

        def print_help
            STDERR.puts help_string
        end

    end

end
