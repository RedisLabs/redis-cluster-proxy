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
    "              client. Status can be: updated|updating|broken",
    "CONNECTION -- Get the connection type for the cluster associated with "
    "              the calling client. Type can be: shared|private",
    "NODES      -- Get a list of the master nodes of the cluster associated "
    "              with the calling client. Type can be: shared|private",
    "UPDATE     -- Request an update of the configuration for the cluster "
    "              associated with the current client.",
    NULL
};
