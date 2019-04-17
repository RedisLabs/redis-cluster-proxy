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

#ifndef __REDIS_CLUSTER_PROXY_H__
#define __REDIS_CLUSTER_PROXY_H__

#include <pthread.h>
#include <stdint.h>
#include <hiredis.h>
#include "ae.h"
#include "anet.h"
#include "cluster.h"
#include "commands.h"
#include "sds.h"
#include "rax.h"

#define REDIS_CLUSTER_PROXY_VERSION "0.0.1"
#define CLIENT_STATUS_NONE          0
#define CLIENT_STATUS_LINKED        1
#define CLIENT_STATUS_UNLINKED      2

#define getClientLoop(c) (proxy.threads[c->thread_id]->loop)

struct client;
struct proxyThread;

typedef struct clientRequest{
    struct client *client;
    uint64_t id;
    sds buffer;
    int query_offset;
    int is_multibulk;
    int argc;
    int num_commands;
    long long pending_bulks;
    int current_bulk_length;
    int *offsets;
    int *lengths;
    int offsets_size;
    int slot;
    clusterNode *node;
    struct redisCommandDef *command;
    size_t written;
    int parsing_status;
    int has_write_handler;
    int has_read_handler;
    struct clientRequest *prev_request; /* Previous pipelined request */
    struct clientRequest *next_request; /* Next pipelined request */
} clientRequest;

typedef struct {
    redisCluster *cluster;
    aeEventLoop *main_loop;
    int fds[2];
    int fd_count;
    int tcp_backlog;
    char neterr[ANET_ERR_LEN];
    struct proxyThread **threads;
    uint64_t numclients;
    rax *commands;
    pthread_mutex_t numclients_mutex;
} redisClusterProxy;

typedef struct client {
    uint64_t id;
    int fd;
    sds ip;
    int thread_id;
    sds obuf;
    size_t written;
    int status;
    int has_write_handler;
    uint64_t next_request_id;
    struct clientRequest *current_request; /* Currently reading */
    uint64_t min_reply_id;
    rax *unordered_requests;
    list *requests_to_process; /* Requests not completely parsed */
} client;

void freeRequest(clientRequest *req, int delete_from_lists);
void freeRequestList(list *request_list);

#endif /* __REDIS_CLUSTER_PROXY_H__ */
