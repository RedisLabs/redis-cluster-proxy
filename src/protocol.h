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

#ifndef __REDIS_CLUSTER_PROXY_PROTOCOL_H__
#define __REDIS_CLUSTER_PROXY_PROTOCOL_H__

#include <stdint.h>
#include "proxy.h"

#define ERROR_CLUSTER_RECONFIG \
    "-CLUSTERDOWN Failed to fetch cluster configuration"
#define ERROR_COMMAND_UNSUPPORTED_CROSSSLOT \
    "Cross-slot queries are not supported for this command"
#define ERROR_OOM "Out of memory"
#define ERROR_MULTIPLE_REPLIES_ITER_FAIL "Failed to iterate multiple replies"
#define ERROR_CLUSTER_READ_FAIL "Failed to read from cluster"
#define ERROR_CLUSTER_WRITE_FAIL "Failed to write to cluster"
#define ERROR_UNKNOWN_SUBCMD \
    "Unknown subcommand or wrong number of arguments for '%s'."
#define ERROR_MERGE_REPLY_INVALID_FMT \
    "Invalid reply format while merging multiple replies from cluster"
#define ERROR_NODE_DISCONNECTED "Cluster node disconnected: "
#define ERROR_WRONG_ARGC "wrong number of arguments for '%' command"
#define ERROR_INVALID_QUERY "Invalid query format"
#define ERROR_NO_NODE "Failed to get node for query"
#define ERROR_INVALID_REPLY "Invalid reply format from cluster"

int initReplyArray(client *c);
void addReplyArray(client *c, uint64_t req_id);
void addReplyStringLen(client *c, const char *str, int len, uint64_t req_id);
void addReplyString(client *c, const char *str, uint64_t req_id);
void addReplyBulkStringLen(client *c, const char *str, int len,
                           uint64_t req_id);
void addReplyBulkString(client *c, const char *str, uint64_t req_id);
void addReplyInt(client *c, int64_t integer, uint64_t req_id);
void addReplyNull(client *c, uint64_t req_id);
void addReplyErrorLen(client *c, const char *err, int len, uint64_t req_id);
void addReplyError(client *c, const char *err, uint64_t req_id);
void addReplyErrorUnknownSubcommand(client *c, const char *subcmd,
                                    const char *help, uint64_t req_id);
void addReplyErrorWrongArgc(client *c, const char *cmdname, uint64_t req_id);
void addReplyHelp(client *c, const char **help, uint64_t req_id);
void addReplyRaw(client *c, const char *buf, size_t len, uint64_t req_id);

#endif /* __REDIS_CLUSTER_PROXY_PROTOCOL_H__ */
