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

#ifndef __REDIS_CLUSTER_PROXY_CLUSTER_H__
#define __REDIS_CLUSTER_PROXY_CLUSTER_H__

#include "sds.h"
#include "adlist.h"
#include "rax.h"
#include <pthread.h>
#include <hiredis.h>

#define CLUSTER_SLOTS 16384

struct redisCluster;

typedef struct redisClusterConnection {
    redisContext *context;
    list *requests_to_send;
    list *requests_pending;
    int has_read_handler;
} redisClusterConnection;

typedef struct clusterNode {
    redisClusterConnection **connections;
    struct redisCluster *cluster;
    sds ip;
    int port;
    sds name;
    int flags;
    sds replicate;  /* Master ID if node is a replica */
    int is_replica;
    int *slots;
    int slots_count;
    int replicas_count;
    sds *migrating; /* An array of sds where even strings are slots and odd
                     * strings are the destination node IDs. */
    sds *importing; /* An array of sds where even strings are slots and odd
                     * strings are the source node IDs. */
    int migrating_count; /* Length of the migrating array (migrating slots*2) */
    int importing_count; /* Length of the importing array (importing slots*2) */
    pthread_mutex_t connection_mutex;
} clusterNode;

typedef struct redisCluster {
    list *nodes;
    rax  *slots_map;
    int numthreads;
} redisCluster;

redisCluster *createCluster(int numthreads);
void freeCluster(redisCluster *cluster);
int fetchClusterConfiguration(redisCluster *cluster, char *ip, int port,
                              char *hostsocket);
redisContext *getClusterNodeContext(clusterNode *node, int thread_id);
redisContext *clusterNodeConnect(clusterNode *node, int thread_id);
redisContext *clusterNodeConnectAtomic(clusterNode *node, int thread_id);
void clusterNodeDisconnect(clusterNode *node, int thread_id);
clusterNode *searchNodeBySlot(redisCluster *cluster, int slot);
clusterNode *getNodeByKey(redisCluster *cluster, char *key, int keylen,
                          int *getslot);
clusterNode *getFirstMappedNode(redisCluster *cluster);
#endif /* __REDIS_CLUSTER_PROXY_CLUSTER_H__ */
