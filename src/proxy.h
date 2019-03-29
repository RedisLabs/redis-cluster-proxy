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
#include "ae.h"
#include "anet.h"
#include "cluster.h"
#include "sds.h"
#include "dict.h"

#define REDIS_CLUSTER_PROXY_VERSION "0.0.1"
#define CLIENT_STATUS_NONE          0
#define CLIENT_STATUS_LINKED        1
#define CLIENT_STATUS_UNLINKED      2

#define getClientLoop(c) (proxy.threads[c->thread_id]->loop)

struct _redisClusterProxyThread;

typedef struct {
    redisCluster *cluster;
    aeEventLoop *main_loop;
    int fds[2];
    int fd_count;
    int tcp_backlog;
    char neterr[ANET_ERR_LEN];
    struct _redisClusterProxyThread **threads;
    uint64_t numclients;
    dict *commands;
    pthread_mutex_t numclients_mutex;
} redisClusterProxy;

typedef struct {
    int fd;
    sds ip;
    int thread_id;
    sds ibuf;
    sds obuf;
    size_t written;
    int status;
    int has_write_handler;
} client;

#endif /* __REDIS_CLUSTER_PROXY_H__ */
