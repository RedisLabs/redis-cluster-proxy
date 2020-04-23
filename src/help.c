/*
 * Copyright (C) 2019  Giuseppe Fabio Nicotra <artix2 at gmail dot com>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published
 * by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <stdlib.h>

const char *proxyCommandHelp[] = {
    "PROXY <subcommand> arg arg ... arg",
    "INFO   [section]           -- Get info about Proxy",
    "CONFIG GET <param>         -- Get Proxy's confinguration value for <param>",
    "CONFIG SET <param> <value> -- Set Proxy's confinguration value for "
                                   "<param> to <value>",
    "MULTIPLEXING STATUS|OFF    -- Get current client's multiplexing status "
                                   "or turn it off (private cluster connection)",
    "COMMAND [type]             -- List commands currently known by the Proxy."
                                   "type can be used to filter commands "
                                   "(unsupported|crosslots-unsupported)",
    "CLIENT <subcmd>            -- Execute client specific actions (type "
                                   "`PROXY CLIENT HELP` for more info)",
    "CLUSTER [subcmd]           -- Execute cluster specific actions (type "
                                   "`PROXY CLUSTER HELP` for more info)",
    "DEBUG <subcmd>             -- Utilities for debugging the proxy (type "
                                   "`PROXY DEBUG HELP` for more info)",
    "SHUTDOWN [ASAP]            -- Shutdown the proxy. If `ASAP` is used, "
                                   "perform an immeditae dirty exit, "
                                   "otherwise send a SIGINT.",
    "LOG [level] <message>      -- Log message to Proxy's log, for debugging "
                                   "purpose",
    NULL
};

const char *proxyCommandSubcommandClientHelp[] = {
    "PROXY CLIENT <subcommand> [arg arg ... arg]",
    "ID     -- Get current client's internal id",
    "THREAD -- Get current client's thread id",
    NULL
};

const char *proxyCommandSubcommandClusterHelp[] = {
    "PROXY CLUSTER [subcommand]",
    "-,INFO     -- Get info for the cluster associated with the calling client",
    "STATUS     -- Get status for the cluster associated with the calling "
                   "client. Status can be: updated|updating|broken",
    "CONNECTION -- Get the connection type for the cluster associated with "
                   "the calling client. Type can be: shared|private",
    "NODES      -- Get a list of the master nodes of the cluster associated "
                   "with the calling client. Type can be: shared|private",
    "UPDATE     -- Request an update of the configuration for the cluster "
                   "associated with the current client.",
    NULL
};

const char *proxyCommandSubcommandDebugtHelp[] = {
    "PROXY DEBUG <subcommand> [ARGS...]",
    "SEGFAULT            -- Cause a SEGFAULT on the proxy",
    "ASSERT              -- Cause an assertion failure  on the proxy",
    "KILL <thread> [sig] -- Send signal to thread (can be MAIN,SELF "
    "or the thread ID). Signal can be TERM,INT,KILL or a signal number.",
    NULL
};

const char *mainHelpString =
"Usage: redis-cluster-proxy [OPTIONS] "
"[node1_host:node1_port,node2_host:node2_port,...]\n"
"  -c <file>            Configuration file\n"
"  -p, --port <port>    Port (default: %d). Use 0 in order to disable \n"
"                       TCP connections at all\n"
"  --max-clients <n>    Max clients (default: %d)\n"
"  --threads <n>        Thread number (default: %d, max: %d)\n"
"  --tcpkeepalive       TCP Keep Alive (default: %d)\n"
"  --tcp-backlog        TCP Backlog (default: %d)\n"
"  --daemonize          Execute the proxy in the background\n"
"  --pidfile <path>     Specify the pidfile. If no pidfile is explicitly\n"
"                       specified and --daemonize is on, the default pidfile\n"
"                       will be: %s\n"
"  --logfile <path>     Specify the logfile. By default, STDOUT will be used.\n"
"                       If --daemonize is on and no logfile has been\n"
"                       specified, the proxy won't log at all.\n"
"  --unixsocket <sock_file>\n"
"                       UNIX socket path (empty by default)\n"
"  --unixsocketperm <mode>\n"
"                       UNIX socket permissions (default: %o)\n"
"  --bind <address>     Bind an interface (can be used multiple times \n"
"                       to bind multiple interfaces)\n"
"  --connections-pool-size <size>\n"
"                       Size of the connections pool used to provide \n"
"                       ready-to-use sockets to private connections.\n"
"                       Use 0 to disable connections pool at all.\n"
"                       Default: %d, Max: %d\n"
"  --connections-pool-min-size <size>\n"
"                       Minimum number of connections in the the pool.\n"
"                       Below this value, the thread will start re-spawning\n"
"                       connections at the defined rate until the pool will\n"
"                       be full again. Default: %d\n"
"  --connections-pool-spawn-every <ms>\n"
"                       Interval in milliseconds used to re-spawn connections\n"
"                       in the pool. Default: %d\n"
"  --connections-pool-spawn-rate <num>\n"
"                       Number of connections to re-spawn in the pool at\n"
"                       every cycle. Default: %d\n"
"  --disable-multiplexing <opt>\n"
"                       When should multiplexing be disabled\n"
"                       Values: (auto|always) (default: auto)\n"
"  --enable-cross-slot  Enable cross-slot queries (warning: cross-slot"
"\n                       queries routed to multiple nodes cannot be"
                        " atomic).\n"
"  -a, --auth <passw>   Authentication password\n"
"  --auth-user <name>   Authentication username\n"
"  --disable-colors     Disable colorized output\n"
"  --log-level <level>  Minimum log level: (default: info)\n"
"                       (debug|info|success|warning|error)\n"
"  --dump-queries       Dump query args (only for log-level "
                        "'debug') \n"
"  --dump-buffer        Dump query buffer (only for log-level "
                        "'debug') \n"
"  --dump-queues        Dump request queues (only for log-level "
                        "'debug') \n"
"  -h, --help           Print this help\n";
