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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <hiredis.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include "anet.h"
#include "cluster.h"
#include "zmalloc.h"
#include "logger.h"
#include "config.h"
#include "proxy.h"

#define CLUSTER_NODE_KEEPALIVE_INTERVAL 15
#define CLUSTER_PRINT_REPLY_ERROR(n, err) \
    proxyLogErr("Node %s:%d replied with error:\n%s\n", \
                n->ip, n->port, err);

uint16_t crc16(const char *buf, int len);

/* -----------------------------------------------------------------------------
 * Key space handling
 * -------------------------------------------------------------------------- */

/* We have 16384 hash slots. The hash slot of a given key is obtained
 * as the least significant 14 bits of the crc16 of the key.
 *
 * However if the key contains the {...} pattern, only the part between
 * { and } is hashed. This may be useful in the future to force certain
 * keys to be in the same node (assuming no resharding is in progress). */
static unsigned int clusterKeyHashSlot(char *key, int keylen) {
    int s, e; /* start-end indexes of { and } */

    for (s = 0; s < keylen; s++)
        if (key[s] == '{') break;

    /* No '{' ? Hash the whole key. This is the base case. */
    if (s == keylen) return crc16(key,keylen) & 0x3FFF;

    /* '{' found? Check if we have the corresponding '}'. */
    for (e = s+1; e < keylen; e++)
        if (key[e] == '}') break;

    /* No '}' or nothing between {} ? Hash the whole key. */
    if (e == keylen || e == s+1) return crc16(key,keylen) & 0x3FFF;

    /* If we are here there is both a { and a } on its right. Hash
     * what is in the middle between { and }. */
    return crc16(key+s+1,e-s-1) & 0x3FFF;
}

static redisClusterConnection *createClusterConnection(void) {
    redisClusterConnection *conn = zmalloc(sizeof(*conn));
    if (conn == NULL) return NULL;
    conn->context = NULL;
    conn->has_read_handler = 0;
    conn->connected = 0;
    conn->requests_pending = listCreate();
    if (conn->requests_pending == NULL) {
        zfree(conn);
        return NULL;
    }
    conn->requests_to_send = listCreate();
    if (conn->requests_to_send == NULL) {
        listRelease(conn->requests_pending);
        zfree(conn);
        return NULL;
    }
    return conn;
}

static void freeClusterConnection(redisClusterConnection *conn) {
    freeRequestList(conn->requests_pending);
    freeRequestList(conn->requests_to_send);
    redisContext *ctx = conn->context;
    if (ctx != NULL) redisFree(ctx);
    zfree(conn);
}


/* Check whether reply is NULL or its type is REDIS_REPLY_ERROR. In the
 * latest case, if the 'err' arg is not NULL, it gets allocated with a copy
 * of reply error (it's up to the caller function to free it), elsewhere
 * the error is directly printed. */
static int clusterCheckRedisReply(clusterNode *n, redisReply *r, char **err) {
    int is_err = 0;
    if (!r || (is_err = (r->type == REDIS_REPLY_ERROR))) {
        if (is_err) {
            if (err != NULL) {
                *err = zmalloc((r->len + 1) * sizeof(char));
                strcpy(*err, r->str);
            } else CLUSTER_PRINT_REPLY_ERROR(n, r->str);
        }
        return 0;
    }
    return 1;
}

redisCluster *createCluster(int thread_id) {
    redisCluster *cluster = zcalloc(sizeof(*cluster));
    if (!cluster) return NULL;
    cluster->thread_id = thread_id;
    cluster->nodes = listCreate();
    if (cluster->nodes == NULL) {
        zfree(cluster);
        return NULL;
    }
    cluster->slots_map = raxNew();
    if (cluster->slots_map == NULL) {
        listRelease(cluster->nodes);
        zfree(cluster);
        return NULL;
    }
    cluster->requests_to_reprocess = raxNew();
    if (cluster->requests_to_reprocess == NULL) {
        freeCluster(cluster);
        return NULL;
    }
    cluster->is_reconfiguring = 0;
    cluster->broken = 0;
    return cluster;
}

static void freeClusterNode(clusterNode *node) {
    if (node == NULL) return;
    int i;
    if (node->connection != NULL) {
        onClusterNodeDisconnection(node);
        freeClusterConnection(node->connection);
    }
    if (node->ip) sdsfree(node->ip);
    if (node->name) sdsfree(node->name);
    if (node->replicate) sdsfree(node->replicate);
    if (node->migrating != NULL) {
        for (i = 0; i < node->migrating_count; i++) sdsfree(node->migrating[i]);
        zfree(node->migrating);
    }
    if (node->importing != NULL) {
        for (i = 0; i < node->importing_count; i++) sdsfree(node->importing[i]);
        zfree(node->importing);
    }
    zfree(node->slots);
    zfree(node);
}

static void freeClusterNodes(redisCluster *cluster) {
    if (!cluster || !cluster->nodes) return;
    listIter li;
    listNode *ln;
    listRewind(cluster->nodes, &li);
    while ((ln = listNext(&li)) != NULL) {
        clusterNode *node = ln->value;
        freeClusterNode(node);
    }
    listRelease(cluster->nodes);
}

int resetCluster(redisCluster *cluster) {
    if (cluster->slots_map) raxFree(cluster->slots_map);
    freeClusterNodes(cluster);
    cluster->slots_map = raxNew();
    cluster->nodes = listCreate();
    if (!cluster->slots_map) return 0;
    if (!cluster->nodes) return 0;
    return 1;
}

void freeCluster(redisCluster *cluster) {
    proxyLogDebug("Free cluster\n");
    if (cluster->slots_map) raxFree(cluster->slots_map);
    freeClusterNodes(cluster);
    if (cluster->requests_to_reprocess)
        raxFree(cluster->requests_to_reprocess);
    zfree(cluster);
}

static clusterNode *createClusterNode(char *ip, int port, redisCluster *c) {
    clusterNode *node = zcalloc(sizeof(*node));
    if (!node) return NULL;
    node->cluster = c;
    node->ip = sdsnew(ip);
    node->port = port;
    node->name = NULL;
    node->flags = 0;
    node->replicate = NULL;
    node->replicas_count = 0;
    node->slots = zmalloc(CLUSTER_SLOTS * sizeof(int));
    node->slots_count = 0;
    node->migrating = NULL;
    node->importing = NULL;
    node->migrating_count = 0;
    node->importing_count = 0;
    node->connection = createClusterConnection();
    if (node->connection == NULL) {
        freeClusterNode(node);
        return NULL;
    }
    return node;
}

redisContext *clusterNodeConnect(clusterNode *node) {
    redisContext *ctx = getClusterNodeContext(node);
    if (ctx) {
        onClusterNodeDisconnection(node);
        redisFree(ctx);
        ctx = NULL;
    }
    proxyLogDebug("Connecting to node %s:%d\n", node->ip, node->port);
    ctx = redisConnectNonBlock(node->ip, node->port);
    if (ctx->err) {
        proxyLogErr("Could not connect to Redis at %s:%d: %s\n",
                    node->ip, node->port, ctx->errstr);
        redisFree(ctx);
        node->connection->context = NULL;
        return NULL;
    }
    /* Set aggressive KEEP_ALIVE socket option in the Redis context socket
     * in order to prevent timeouts caused by the execution of long
     * commands. At the same time this improves the detection of real
     * errors. */
    anetKeepAlive(NULL, ctx->fd, CLUSTER_NODE_KEEPALIVE_INTERVAL);
    if (config.auth) {
        redisReply *reply = redisCommand(ctx, "AUTH %s", config.auth);
        int ok = clusterCheckRedisReply(node, reply, NULL);
        if (reply != NULL) freeReplyObject(reply);
        if (!ok) {
            proxyLogErr("Failed to authenticate to %s:%d\n", node->ip,
                        node->port);
            redisFree(ctx);
            node->connection->context = NULL;
            return NULL;
        }
    }
    node->connection->context = ctx;
    return ctx;
}

void clusterNodeDisconnect(clusterNode *node) {
    redisContext *ctx = getClusterNodeContext(node);
    if (ctx == NULL) return;
    proxyLogDebug("Disconnecting from node %s:%d\n", node->ip, node->port);
    onClusterNodeDisconnection(node);
    redisFree(ctx);
    node->connection->context = NULL;
}

/* Map to slot into the cluster's radix tree map after converting the slot
 * to bigendian. */
void mapSlot(redisCluster *cluster, int slot, clusterNode *node) {
    uint32_t slot_be = htonl(slot);
    raxInsert(cluster->slots_map, (unsigned char *) &slot_be,
              sizeof(slot_be), node, NULL);
}

int clusterNodeLoadInfo(redisCluster *cluster, clusterNode *node, list *friends,
                        redisContext *ctx)
{
    int success = 1, free_ctx = 0;
    redisReply *reply =  NULL;
    if (ctx == NULL) {
        ctx = redisConnect(node->ip, node->port);
        if (ctx->err) {
            fprintf(stderr, "Could not connect to Redis at %s:%d: %s\n",
                    node->ip, node->port, ctx->errstr);
            redisFree(ctx);
            return 0;
        }
        free_ctx = 1;
    }
    reply = redisCommand(ctx, "CLUSTER NODES");
    success = (reply != NULL);
    if (!success) goto cleanup;
    success = (reply->type != REDIS_REPLY_ERROR);
    if (!success) {
        fprintf(stderr, "Failed to retrieve cluster configuration.\n");
        fprintf(stderr, "Cluster node %s:%d replied with error:\n%s\n",
                node->ip, node->port, reply->str);
        goto cleanup;
    }
    char *lines = reply->str, *p, *line;
    while ((p = strstr(lines, "\n")) != NULL) {
        *p = '\0';
        line = lines;
        lines = p + 1;
        char *name = NULL, *addr = NULL, *flags = NULL, *master_id = NULL;
        int i = 0;
        while ((p = strchr(line, ' ')) != NULL) {
            *p = '\0';
            char *token = line;
            line = p + 1;
            switch(i++){
            case 0: name = token; break;
            case 1: addr = token; break;
            case 2: flags = token; break;
            case 3: master_id = token; break;
            }
            if (i == 8) break; // Slots
        }
        if (!flags) {
            fprintf(stderr, "Invalid CLUSTER NODES reply: missing flags.\n");
            success = 0;
            goto cleanup;
        }
        if (addr == NULL) {
            fprintf(stderr, "Invalid CLUSTER NODES reply: missing addr.\n");
            success = 0;
            goto cleanup;
        }
        int myself = (strstr(flags, "myself") != NULL);
        char *ip = NULL;
        int port = 0;
        char *paddr = strchr(addr, ':');
        if (paddr != NULL) {
            *paddr = '\0';
            ip = addr;
            addr = paddr + 1;
            /* If internal bus is specified, then just drop it. */
            if ((paddr = strchr(addr, '@')) != NULL) *paddr = '\0';
            port = atoi(addr);
        }
        if (myself) {
            if (node->ip == NULL && ip != NULL) {
                node->ip = ip;
                node->port = port;
            }
        } else {
            if (friends == NULL) continue;
            clusterNode *friend = createClusterNode(ip, port, cluster);
            if (friend == NULL) {
                success = 0;
                goto cleanup;
            }
            listAddNodeTail(friends, friend);
            continue;
        }
        if (name != NULL && node->name == NULL) node->name = sdsnew(name);
        node->is_replica = (strstr(flags, "slave") != NULL ||
                           (master_id != NULL && master_id[0] != '-'));
        if (i == 8) {
            int remaining = strlen(line);
            while (remaining > 0) {
                p = strchr(line, ' ');
                if (p == NULL) p = line + remaining;
                remaining -= (p - line);

                char *slotsdef = line;
                *p = '\0';
                if (remaining) {
                    line = p + 1;
                    remaining--;
                } else line = p;
                char *dash = NULL;
                if (slotsdef[0] == '[') {
                    slotsdef++;
                    if ((p = strstr(slotsdef, "->-"))) { // Migrating
                        *p = '\0';
                        p += 3;
                        char *closing_bracket = strchr(p, ']');
                        if (closing_bracket) *closing_bracket = '\0';
                        sds slot = sdsnew(slotsdef);
                        sds dst = sdsnew(p);
                        node->migrating_count += 2;
                        node->migrating =
                            zrealloc(node->migrating,
                                (node->migrating_count * sizeof(sds)));
                        node->migrating[node->migrating_count - 2] =
                            slot;
                        node->migrating[node->migrating_count - 1] =
                            dst;
                    }  else if ((p = strstr(slotsdef, "-<-"))) {//Importing
                        *p = '\0';
                        p += 3;
                        char *closing_bracket = strchr(p, ']');
                        if (closing_bracket) *closing_bracket = '\0';
                        sds slot = sdsnew(slotsdef);
                        sds src = sdsnew(p);
                        node->importing_count += 2;
                        node->importing = zrealloc(node->importing,
                            (node->importing_count * sizeof(sds)));
                        node->importing[node->importing_count - 2] =
                            slot;
                        node->importing[node->importing_count - 1] =
                            src;
                    }
                } else if ((dash = strchr(slotsdef, '-')) != NULL) {
                    p = dash;
                    int start, stop;
                    *p = '\0';
                    start = atoi(slotsdef);
                    stop = atoi(p + 1);
                    mapSlot(cluster, start, node);
                    mapSlot(cluster, stop, node);
                    while (start <= stop) {
                        int slot = start++;
                        node->slots[node->slots_count++] = slot;
                    }
                } else if (p > slotsdef) {
                    int slot = atoi(slotsdef);
                    node->slots[node->slots_count++] = slot;
                    mapSlot(cluster, slot, node);
                }
            }
        }
    }
cleanup:
    if (free_ctx) redisFree(ctx);
    freeReplyObject(reply);
    return success;
}

int fetchClusterConfiguration(redisCluster *cluster, char *ip, int port,
                              char *hostsocket)
{
    int success = 1;
    redisContext *ctx = NULL;
    list *friends = NULL;
    if (hostsocket == NULL)
        ctx = redisConnect(ip, port);
    else
        ctx = redisConnectUnix(hostsocket);
    if (ctx->err) {
        fprintf(stderr, "Could not connect to Redis at ");
        if (hostsocket == NULL)
            fprintf(stderr,"%s:%d: %s\n", ip, port, ctx->errstr);
        else fprintf(stderr,"%s: %s\n", hostsocket, ctx->errstr);
        redisFree(ctx);
        return 0;
    }
    clusterNode *firstNode = createClusterNode(ip, port, cluster);
    if (!firstNode) {success = 0; goto cleanup;}
    listAddNodeTail(cluster->nodes, firstNode);
    friends = listCreate();
    success = (friends != NULL);
    if (!success) goto cleanup;
    success = clusterNodeLoadInfo(cluster, firstNode, friends, ctx);
    if (!success) goto cleanup;
    listIter li;
    listNode *ln;
    listRewind(friends, &li);
    while ((ln = listNext(&li))) {
        clusterNode *friend = ln->value;
        success = clusterNodeLoadInfo(cluster, friend, NULL, NULL);
        if (!success) {
            listDelNode(friends, ln);
            freeClusterNode(friend);
            goto cleanup;
        }
        listAddNodeTail(cluster->nodes, friend);
    }
cleanup:
    redisFree(ctx);
    if (friends) listRelease(friends);
    return success;
}

clusterNode *searchNodeBySlot(redisCluster *cluster, int slot) {
    clusterNode *node = NULL;
    raxIterator iter;
    raxStart(&iter, cluster->slots_map);
    int slot_be = htonl(slot);
    if (!raxSeek(&iter, ">=", (unsigned char*) &slot_be, sizeof(slot_be))) {
        proxyLogErr("Failed to seek cluster node into slots map.\n");
        raxStop(&iter);
        return NULL;
    }
    if (raxNext(&iter)) node = (clusterNode *) iter.data;
    raxStop(&iter);
    return node;
}

clusterNode *getNodeByKey(redisCluster *cluster, char *key, int keylen,
                          int *getslot)
{
    clusterNode *node = NULL;
    int slot = clusterKeyHashSlot(key, keylen);
    node = searchNodeBySlot(cluster, slot);
    if (node && getslot != NULL) *getslot = slot;
    return node;
}

clusterNode *getFirstMappedNode(redisCluster *cluster) {
    clusterNode *node = NULL;
    raxIterator iter;
    raxStart(&iter, cluster->slots_map);
    if (!raxSeek(&iter, "^", NULL, 0)) {
        raxStop(&iter);
        return NULL;
    }
    if (raxNext(&iter)) node = (clusterNode *) iter.data;
    raxStop(&iter);
    return node;
}

/* Reconfigure the cluster. Wait until all request pending or still writing to
 * cluster have finished and the fetch the cluster configuration again.
 * Return values:
 *      CLUSTER_RECONFIG_WAIT: there are requests pendng or writing
 *                             to cluster, so reconfiguration will start
 *                             after these queues are empty.
 *      CLUSTER_RECONFIG_STARTED: reconfiguration has started
 *      CLUSTER_RECONFIG_ERR: some error occurred during reconfiguration.
 *                            In this case clsuter->broken is set to 1.
 *      CLUSTER_RECONFIG_ENDED: reconfiguration ended with success. */
int startClusterReconfiguration(redisCluster *cluster) {
    if (cluster->broken) return CLUSTER_RECONFIG_ERR;
    int status = CLUSTER_RECONFIG_WAIT;
    listIter li;
    listNode *ln;
    int requests_to_wait = 0;
    listRewind(cluster->nodes, &li);
    sds ip = NULL;
    int port = 0;
    /* Count all requests_pending or request_to_send that are still
     * writing to cluster. */
    while ((ln = listNext(&li))) {
        clusterNode *node = ln->value;
        if (!ip) {
            ip = sdsnew(node->ip);
            port = node->port;
        }
        if (node->is_replica) continue;
        redisClusterConnection *conn = node->connection;
        if (conn == NULL) continue;
        requests_to_wait += listLength(conn->requests_pending);
        listIter rli;
        listNode *rln;
        listRewind(conn->requests_to_send, &rli);
        while ((rln = listNext(&rli))) {
            clientRequest *req = rln->value;
            if (req->has_write_handler) requests_to_wait++;
            else {
                /* All requests to send that aren't writing to cluster
                 * are directly added to request_to_reprocess and removed
                 * from the `requests_to_send` queue. */
                clusterAddRequestToReprocess(cluster, req);
                listDelNode(conn->requests_to_send, rln);
            }
        }
    }
    proxyLogDebug("Cluster reconfiguration: still waiting for %d requests\n",
                  requests_to_wait);
    cluster->is_reconfiguring = 1;
    /* If the are requests pending or writing to cluster, just return
     * CLUSTER_RECONFIG_WAIT status. */
    if (requests_to_wait) goto final;
    status = CLUSTER_RECONFIG_STARTED;
    /* Start the reconfiguration. */
    proxyLogDebug("Reconfiguring cluster (thread: %d)\n", cluster->thread_id);
    if (!resetCluster(cluster)) {
        proxyLogErr("Failed to reset cluster!\n");
        status = CLUSTER_RECONFIG_ERR;
        goto final;
    }
    proxyLogDebug("Reconfiguring cluster from node %s:%d (thread: %d)\n",
                  ip, port, cluster->thread_id);
    if (!fetchClusterConfiguration(cluster, ip, port, NULL)) {
        proxyLogErr("Failed to fetch cluster configuration! (thread: %d)\n",
                    cluster->thread_id);
        status = CLUSTER_RECONFIG_ERR;
        goto final;
    }
    /* Re-process all the requests that were moved to
     * `cluster->requests_to_reprocess` */
    raxIterator iter;
    raxStart(&iter, cluster->requests_to_reprocess);
    if (!raxSeek(&iter, "^", NULL, 0)) {
        raxStop(&iter);
        status = CLUSTER_RECONFIG_ERR;
        proxyLogErr("Failed to reset 'cluster->requests_to_reprocess' "
                    "(thread: %d)\n", cluster->thread_id);
        goto final;
    }
    cluster->is_reconfiguring = 0;
    proxyLogDebug("Reprocessing cluster requests (thread: %d)\n",
                  cluster->thread_id);
    while (raxNext(&iter)) {
        clientRequest *req = (clientRequest *) iter.data;
        req->need_reprocessing = 0;
        if (raxRemove(cluster->requests_to_reprocess, iter.key, iter.key_len,
             NULL)) raxSeek(&iter,">",iter.key,iter.key_len);
        listNode *ln = listSearchKey(req->client->requests_to_reprocess, req);
        if (ln) listDelNode(req->client->requests_to_reprocess, ln);
        processRequest(req);
    }
    raxStop(&iter);
    proxyLogDebug("Cluster reconfiguration ended (thread: %d)\n",
                  cluster->thread_id);
    status = CLUSTER_RECONFIG_ENDED;
final:
    if (ip) sdsfree(ip);
    if (status == CLUSTER_RECONFIG_ERR) cluster->broken = 1;
    return status;
}

/* Add the request to `cluster->requests_to_reprocess` rax. Also add it
 * to the client's `requests_to_reprocess` list.
 * The request's node will also be set to NULL (since the current configuration
 * will be reset) and `need_reprocessing` will be set to 1.
 * The `written` count will be also set to 0, since the request must be
 * written to the cluster again when the new cluster's configuration will be
 * available. */
void clusterAddRequestToReprocess(redisCluster *cluster, void *r) {
    clientRequest *req = r;
    req->need_reprocessing = 1;
    req->node = NULL;
    req->slot = -1;
    req->written = 0;
    sds id = sdscatprintf(sdsempty(), "%lld:%lld", req->client->id, req->id);
    raxInsert(cluster->requests_to_reprocess, (unsigned char *) id,
              sdslen(id), req, NULL);
    listAddNodeTail(req->client->requests_to_reprocess, req);
    sdsfree(id);
}

void clusterRemoveRequestToReprocess(redisCluster *cluster, void *r) {
    clientRequest *req = r;
    req->need_reprocessing = 0;
    sds id = sdscatprintf(sdsempty(), "%lld:%lld", req->client->id, req->id);
    raxRemove(cluster->requests_to_reprocess, (unsigned char *) id,
              sdslen(id), NULL);
    sdsfree(id);
}
