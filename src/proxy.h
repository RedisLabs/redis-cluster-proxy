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

#ifdef __STDC_NO_ATOMICS__
#error "Missing support for C11 Atomics, please update your C compiler."
#endif

#include <pthread.h>
#include <stdint.h>
#include <sys/resource.h>
#include <hiredis.h>
#include <time.h>
#include "ae.h"
#include "anet.h"
#include "cluster.h"
#include "commands.h"
#include "sds.h"
#include "rax.h"
#include "config.h"
#include "version.h"

#define CLIENT_STATUS_NONE          0
#define CLIENT_STATUS_LINKED        1
#define CLIENT_STATUS_UNLINKED      2

#define PROXY_MAIN_THREAD_ID -1
#define PROXY_UNKN_THREAD_ID -999

#define getClientLoop(c) (proxy.threads[c->thread_id]->loop)

struct client;
typedef struct proxyThread {
    int thread_id;
    int io[2];
    pthread_t thread;
    redisCluster *cluster;
    aeEventLoop *loop;
    list *clients;
    list *unlinked_clients;
    list *pending_messages;
    list *connections_pool;
    int is_spawning_connections;
    uint64_t next_client_id;
    sds msgbuffer;
} proxyThread;

typedef struct clientRequest {
    struct client *client;
    uint64_t id;
    sds buffer;
    int query_offset;
    int is_multibulk;
    int argc;
    int num_commands;
    long long pending_bulks;
    long long current_bulk_length;
    int *offsets;
    int *lengths;
    int offsets_size;
    int slot;
    clusterNode *node;
    struct redisCommandDef *command;
    size_t written;
    int parsing_status;
    int has_write_handler;
    int need_reprocessing;
    int parsed;
    int owned_by_client;
    int closes_transaction;
    list *child_requests;
    rax  *child_replies;
    uint64_t max_child_reply_id;
    struct clientRequest *parent_request;
    /* Pointers to *listNode used in various list. They allow to quickly
     * have a reference to the node instead of searching it via listSearchKey.
     */
    listNode *requests_lnode; /* Pointer to node in client->requests list */
    listNode *requests_to_send_lnode; /* Pointer to node in
                                       * redisClusterConnection->
                                       *  requests_to_send list */
    listNode *requests_pending_lnode; /* Pointer to node in
                                       * redisClusterConnection->
                                       * requests_pending list */
} clientRequest;

typedef struct {
    aeEventLoop *main_loop;
    int fds[BINDADDR_MAX];
    int fd_count;
    int unixsocket_fd;
    int tcp_backlog;
    char neterr[ANET_ERR_LEN];
    struct proxyThread **threads;
    _Atomic uint64_t numclients;
    rax *commands;
    int min_reserved_fds;
    time_t start_time;
    sds configfile;
    size_t system_memory_size;
    pthread_t main_thread;
    _Atomic int exit_asap;
} redisClusterProxy;

typedef struct client {
    uint64_t id;
    int fd;
    sds ip;
    int port;
    sds addr;
    int thread_id;
    sds obuf;
    size_t written;
    list *reply_array;
    int status;
    int has_write_handler;
    int flags;
    uint64_t next_request_id;
    struct clientRequest *current_request; /* Currently reading */
    uint64_t min_reply_id;
    rax *unordered_replies;
    list *requests;                  /* All client's requests */
    list *requests_to_process;       /* Requests not completely parsed */
    int requests_with_write_handler; /* Number of request that are still
                                      * being writing to cluster */
    list *requests_to_reprocess;     /* Requestst to re-process after cluster
                                      * re-configuration completes */
    int pending_multiplex_requests;  /* Number of request that have to be
                                      * written/read before sending requests
                                      * to private cluster connection */

    redisCluster *cluster;
    int multi_transaction;
    clientRequest *multi_request;
    clusterNode *multi_transaction_node;
    sds auth_user;                  /* Used by client who wants to authenticate
                                     * itself with different credentials from
                                     * the ones used in the proxy config */
    sds auth_passw;
    /* Pointers to *listNode used in various list. They allow to quickly
     * have a reference to the node instead of searching it via listSearchKey.
     */
    listNode *clients_lnode; /* Pointer to node in thread->clients list */
    listNode *unlinked_clients_lnode; /* Pointer to node in
                                       * thread->unlinked_clients list */
} client;

int getCurrentThreadID(void);
int processRequest(clientRequest *req, int *parsing_status,
    clientRequest **next);
void freeRequest(clientRequest *req);
void freeRequestList(list *request_list);
void onClusterNodeDisconnection(clusterNode *node);

#endif /* __REDIS_CLUSTER_PROXY_H__ */
