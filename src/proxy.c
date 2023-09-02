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

#include "fmacros.h"
#include "redis_config.h"
#include "proxy.h"
#include "logger.h"
#include "zmalloc.h"
#include "protocol.h"
#include "endianconv.h"
#include "util.h"
#include "help.h"
#include "reply_order.h"
#include <stdio.h>
#include <string.h>
#include <strings.h>
#include <stdlib.h>
#include <ctype.h>
#include <errno.h>
#include <signal.h>
#include <unistd.h>
#include <fcntl.h>
#include <inttypes.h>
#include <sys/stat.h>
#include <sys/utsname.h>
#include "assert.h" /* Use proxy's assert */

#define QUERY_OFFSETS_MIN_SIZE              10
#define MAX_THREADS                         500
#define EL_INSTALL_HANDLER_FAIL             9999
#define REQ_STATUS_UNKNOWN                  -1
#define PARSE_STATUS_INCOMPLETE             -1
#define PARSE_STATUS_ERROR                  0
#define PARSE_STATUS_OK                     1
#define UNDEFINED_SLOT                      -1
#define PROTO_INLINE_MAX_SIZE               (1024*64)

#define MAX_ACCEPTS                         1000
#define NET_IP_STR_LEN                      46

#define THREAD_IO_READ                      0
#define THREAD_IO_WRITE                     1

#define QUEUE_TYPE_SENDING                  1
#define QUEUE_TYPE_PENDING                  2

#define THREAD_MSG_STOP                     1

#define CLIENT_CLOSE_AFTER_REPLY            (1 << 1)

#define UNUSED(V) ((void) V)

#define getThread(c) (proxy.threads[c->thread_id])
#define getCluster(c) \
    (c->cluster ? c->cluster : proxy.threads[c->thread_id]->cluster)
#define addObjectToList(obj, listowner, listname, success) do {\
    if (listAddNodeTail(listowner->listname, obj) == NULL) {\
        if (success != NULL) *success = 0;\
        obj->listname ## _lnode = NULL;\
        break;\
    }\
    if (success != NULL) *success = 1;\
    obj->listname ## _lnode = listLast(listowner->listname);\
} while (0)
#define removeObjectFromList(obj, listowner, listname) do {\
    if (obj->listname ## _lnode != NULL) {\
        listDelNode(listowner->listname, obj->listname ## _lnode);\
        obj->listname ## _lnode = NULL;\
    }\
} while (0)
#define enqueueRequestToSend(req) (enqueueRequest(req, QUEUE_TYPE_SENDING))
#define dequeueRequestToSend(req) (dequeueRequest(req, QUEUE_TYPE_SENDING))
#define enqueuePendingRequest(req) (enqueueRequest(req, QUEUE_TYPE_PENDING))
#define dequeuePendingRequest(req) (dequeueRequest(req, QUEUE_TYPE_PENDING))
#define getFirstRequestToSend(node, isempty) \
    (getFirstQueuedRequest(node->connection->requests_to_send,\
     isempty))
#define getFirstRequestPending(node, isempty) \
    (getFirstQueuedRequest(node->connection->requests_pending,\
     isempty))
#define clientRequiresAuth(c) \
    ((c->auth_user && (!config.auth_user || strcmp(config.auth_user, c->auth_user))) || (config.auth_user && !c->auth_user))

#define REQID_PRINTF_FMT "%d:%" PRId64 ":%" PRId64
#define REQID_PRINTF_ARG(r) r->client->thread_id, r->client->id, r->id
#define getClientPeerIP(c) (c->ip ? c->ip : config.unixsocket)
#define strRepr(s) (sdscatrepr(sdsempty(), s, strlen(s)))
#define sdsRepr(s) (sdscatrepr(sdsempty(), s, sdslen(s)))
#define PROXY_CMD_LOG_MAX_LEN   4096

/* Globals */

redisClusterProxy proxy;
redisClusterProxyConfig config;
static struct utsname proxy_os;
redisCommandDef *authCommandDef = NULL;
redisCommandDef *scanCommandDef = NULL;
int ae_api_kqueue = 0;

#ifdef __GNUC__
__thread int thread_id;
#else
_Thread_local int thread_id;
#endif

/* Forward declarations. */

static proxyThread *createProxyThread(int index);
static void freeProxyThread(proxyThread *thread);
static void *execProxyThread(void *ptr);
static client *createClient(int fd, char *ip);
static void unlinkClient(client *c);
static void freeClient(client *c);
static clientRequest *createRequest(client *c);
void readQuery(aeEventLoop *el, int fd, void *privdata, int mask);
static int writeToClient(client *c);
static int writeToCluster(aeEventLoop *el, int fd, clientRequest *req);
static void writeToClusterHandler(aeEventLoop *el, int fd, void *privdata,
                                  int mask);
static void readClusterReply(aeEventLoop *el, int fd, void *privdata, int mask);
static clusterNode *getRequestNode(clientRequest *req, sds *err);
static clientRequest *handleNextRequestsToCluster(clusterNode *node,
    clientRequest **failed);
static clientRequest *getFirstQueuedRequest(list *queue, int *is_empty);
static int enqueueRequest(clientRequest *req, int queue_type);
static void dequeueRequest(clientRequest *req, int queue_type);
static int sendMessageToThread(proxyThread *thread, sds buf);
static int installIOHandler(aeEventLoop *el, int fd, int mask, aeFileProc *proc,
                            void *data, int retried);
static int disableMultiplexingForClient(client *c);
char *redisClusterProxyGitSHA1(void);
char *redisClusterProxyGitDirty(void);
char *redisClusterProxyGitBranch(void);
static int processThreadPipeBufferForNewClients(proxyThread *thread);
static redisClusterConnection *getRequestConnection(clientRequest *req);
#ifdef HAVE_BACKTRACE
void sigsegvHandler(int sig, siginfo_t *info, void *secret);
#endif

/* Hiredis helpers */

int processItem(redisReader *r);

/* This function does the same things as redisReaderGetReply, but
 * it does not trim the reader's buffer, in order to let the proxy's
 * read handler to get the full reply's buffer. Consuming and trimming
 * ther reader's buffer is up to the proxy. */

static int __hiredisReadReplyFromBuffer(redisReader *r, void **reply) {
    /* Default target pointer to NULL. */
    if (reply != NULL)
        *reply = NULL;

    /* Return early when this reader is in an erroneous state. */
    if (r->err)
        return REDIS_ERR;

    /* When the buffer is empty, there will never be a reply. */
    if (r->len == 0)
        return REDIS_OK;

    /* Set first item to process when the stack is empty. */
    if (r->ridx == -1) {
        r->rstack[0].type = -1;
        r->rstack[0].elements = -1;
        r->rstack[0].idx = -1;
        r->rstack[0].obj = NULL;
        r->rstack[0].parent = NULL;
        r->rstack[0].privdata = r->privdata;
        r->ridx = 0;
    }

    /* Process items in reply. */
    while (r->ridx >= 0)
        if (processItem(r) != REDIS_OK)
            break;

    /* Return ASAP when an error occurred. */
    if (r->err)
        return REDIS_ERR;

    /* Emit a reply when there is one. */
    if (r->ridx == -1) {
        if (reply != NULL)
            *reply = r->reply;
        r->reply = NULL;
    }
    return REDIS_OK;
}

/* Utils */

int getCurrentThreadID(void) {
    return thread_id;
}

static void logClusterReplyFailure(char *err, redisReply *reply,
                                   clusterNode *node)
{
    if (reply == NULL) return;
    if (node == NULL || node->cluster == NULL) return;
    client *c = node->cluster->owner;
    sds conndescr = sdsempty(), repdescr = sdsempty();
    if (c) {
        conndescr = sdscatprintf(conndescr, "private connection for client "
            "%d:%" PRId64 " to node %s:%d",
            c->thread_id, c->id, node->ip, node->port);
    } else {
        conndescr = sdscatprintf(conndescr, "shared connection to node %s:%d "
            "on thread %d", node->ip, node->port, node->cluster->thread_id);
    }
    if (reply->type == REDIS_REPLY_ERROR || reply->type == REDIS_REPLY_STRING)
        repdescr = sdscatfmt(repdescr, "Reply is: '%s'", reply->str);
    else if (reply->type == REDIS_REPLY_INTEGER)
        repdescr = sdscatfmt(repdescr, "Reply is: %d", reply->integer);
    proxyLogErr("WARN: %s on %s. %s", err, conndescr, repdescr);
    sdsfree(conndescr);
    sdsfree(repdescr);
}

/* Custom Commands */

static sds proxySubCommandConfig(clientRequest *r, sds option, sds value,
                                 sds *err)
{
    void *opt = NULL;
    sds reply = NULL;
    int ok = 0;
    int is_int = 0, is_float = 0, is_string = 0, read_only = 0;
    int max_int = -1;
    UNUSED(is_float);
    if (strcmp("log-level", option) == 0) {
        opt = &(config.loglevel);
    } else if (strcmp("threads", option) == 0) {
        is_int = 1;
        opt = &(config.num_threads);
        read_only = 1;
    } else if (strcmp("max-clients", option) == 0) {
        is_int = 1;
        opt = &(config.max_clients);
        read_only = 1;
    } else if (strcmp("connections-pool-size", option) == 0) {
        is_int = 1;
        max_int = MAX_POOL_SIZE;
        opt = &(config.connections_pool.size);
    } else if (strcmp("connections-pool-min-size", option) == 0) {
        is_int = 1;
        max_int = config.connections_pool.size;
        opt = &(config.connections_pool.min_size);
    } else if (strcmp("connections-pool-spawn-every", option) == 0) {
        is_int = 1;
        opt = &(config.connections_pool.spawn_every);
    } else if (strcmp("connections-pool-spawn-rate", option) == 0) {
        is_int = 1;
        opt = &(config.connections_pool.spawn_rate);
    } else if (strcmp("tcpkeepalive", option) == 0) {
        is_int = 1;
        opt = &(config.tcpkeepalive);
        read_only = 1;
    } else if (strcmp("tcp-backlog", option) == 0) {
        is_int = 1;
        opt = &(config.tcp_backlog);
        read_only = 1;
    } else if (strcmp("daemonize", option) == 0) {
        is_int = 1;
        opt = &(config.daemonize);
        read_only = 1;
    } else if (strcmp("dump-queries", option) == 0) {
        is_int = 1;
        opt = &(config.dump_queries);
    } else if (strcmp("dump-buffer", option) == 0) {
        is_int = 1;
        opt = &(config.dump_buffer);
    } else if (strcmp("dump-replies", option) == 0) {
        is_int = 1;
        opt = &(config.dump_queues);
    } else if (strcmp("enable-cross-slot", option) == 0) {
        is_int = 1;
        opt = &(config.cross_slot_enabled);
    } else if (strcmp("unixsocketperm", option) == 0) {
        is_int = 1;
        read_only = 1;
        opt = &(config.unixsocketperm);
    } else if (strcmp("unixsocket", option) == 0) {
        read_only = 1;
        is_string = 1;
        opt = &(config.unixsocket);
    } else if (strcmp("pidfile", option) == 0) {
        read_only = 1;
        is_string = 1;
        opt = &(config.pidfile);
    } else if (strcmp("logfile", option) == 0) {
        read_only = 1;
        is_string = 1;
        opt = &(config.logfile);
    } else if (strcmp("bind", option) == 0) {
        opt = &(config.bindaddr);
        read_only = 1;
    }
    if (opt == NULL) {
        if (err) *err = sdsnew("Invalid config option");
        return NULL;
    }
    if (opt == &(config.loglevel)) {
        if (value != NULL) {
            int i, level = -1;
            for (i = 0; i <= LOGLEVEL_ERROR; i++) {
                if (!strcasecmp(value, redisProxyLogLevels[i])) {
                    level = i;
                    break;
                }
            }
            if (level == -1) {
                *err = sdsnew("Invalid log-level");
                return NULL;
            }
            config.loglevel = level;
            ok = 1;
        } else {
            reply = sdsnew(redisProxyLogLevels[config.loglevel]);
        }
    } else if (opt == &(config.bindaddr)) {
        if (value != NULL) {
            if (err) *err = sdsnew("This config option is read-only");
            return NULL;
        }
        if (!initReplyArray(r->client)) {
            *err = sdsnew(ERROR_OOM);
            return NULL;
        }
        int j;
        addReplyString(r->client, "bind", r->id);
        sds c = sdscatfmt(sdsempty(), "*%u\r\n", config.bindaddr_count);
        for (j = 0; j < config.bindaddr_count; j++)
            c = sdscatprintf(c, "+%s\r\n", config.bindaddr[j]);
        listAddNodeTail(r->client->reply_array, c);
        addReplyArray(r->client, r->id);
    } else {
        if (value == NULL) {
            if (!initReplyArray(r->client)) {
                *err = sdsnew(ERROR_OOM);
                return NULL;
            }
            addReplyString(r->client, option, r->id);
            if (is_int) addReplyInt(r->client, *((int *) opt), r->id);
            else if (is_string) {
                char *str = (char *) *((char **) opt);
                if (str != NULL) addReplyString(r->client, str, r->id);
                else addReplyNull(r->client, r->id);
            }
            else addReplyError(r->client, "Unsupported CONFIG option", r->id);
            addReplyArray(r->client, r->id);
        } else {
            if (read_only) *err = sdsnew("This config option is read-only");
            else {
                if (is_int) {
                    int val = atoi(value);
                    if (max_int >= 0 && val > max_int) {
                        *err = sdscatfmt(sdsempty(),
                            "Maximum value for '%s' is %i", option, max_int);
                        return NULL;
                    }
                    *((int *) opt) = val;
                    ok = 1;
                }
            }
        }
    }
    if (reply == NULL && ok) reply = sdsnew("OK");
    return reply;
}

static void proxySubCommandCommand(clientRequest *req, sds cmdtype) {
    int only_unsupported = 0;
    int only_crossslots_disabled = 0;
    if (cmdtype) {
        if (!strcasecmp("unsupported", cmdtype)) only_unsupported = 1;
        else if (!strcasecmp("crossslots-unsupported", cmdtype))
            only_crossslots_disabled = 1;
        else {
            addReplyError(req->client,
                          "Invalid argument `TYPE` for `PROXY COMMAND`. "
                          "Supported types: unsupported, "
                          "crossslots-unsupported",
                          req->id);
            return;
        }
    }
    if (!initReplyArray(req->client)) {
        addReplyError(req->client, ERROR_OOM, req->id);
        return;
    }
    int i, command_count = sizeof(redisCommandTable) / sizeof(redisCommandDef);
    for (i = 0; i < command_count; i++) {
        redisCommandDef *cmd = redisCommandTable + i;
        if (only_unsupported && !cmd->unsupported) continue;
        if (only_crossslots_disabled) {
            int unsupported = cmd->proxy_flags & CMDFLAG_MULTISLOT_UNSUPPORTED;
            int multi_keys = (
                cmd->first_key > 0 &&
                (cmd->last_key < 0 || (cmd->last_key - cmd->first_key) > 0)
            );
            if (multi_keys && cmd->handleReply == NULL) unsupported = 1;
            if (!unsupported) continue;
        }
        sds c = sdsnew("*6\r\n");
        c = sdscatprintf(c, "+%s\r\n", cmd->name);
        c = sdscatfmt(c, ":%i\r\n", cmd->arity);
        c = sdscatfmt(c, ":%i\r\n", cmd->first_key);
        c = sdscatfmt(c, ":%i\r\n", cmd->last_key);
        c = sdscatfmt(c, ":%i\r\n", cmd->key_step);
        c = sdscatfmt(c, ":%i\r\n", cmd->unsupported);
        listAddNodeTail(req->client->reply_array, c);
    }
    addReplyArray(req->client, req->id);
}

static void proxySubCommandClient(clientRequest *req, sds subcmd) {
    if (strcasecmp("id", subcmd) == 0) {
        sds id = sdscatprintf(sdsempty(), "%d:%" PRId64,
                              req->client->thread_id, req->client->id);
        addReplyString(req->client, id, req->id);
        sdsfree(id);
    } else if (strcasecmp("thread", subcmd) == 0) {
        addReplyInt(req->client, req->client->thread_id, req->id);
    } else if (strcasecmp("help", subcmd) == 0) {
        addReplyHelp(req->client, proxyCommandSubcommandClientHelp, req->id);
    } else {
        addReplyErrorUnknownSubcommand(req->client, "PROXY CLIENT",
            "PROXY CLIENT HELP", req->id);
    }
}

static void proxySubCommandCluster(clientRequest *req, sds subcmd) {
    int fetch_info = 0;
    sds info_field = NULL;
    redisCluster *cluster = getCluster(req->client);
    if (cluster == NULL) {
        addReplyError(req->client, "-FATAL no cluster", req->id);
        return;
    }
    if (subcmd ==  NULL || strcasecmp("info", subcmd) == 0) {
        fetch_info = 1;
    } else if (strcasecmp("update", subcmd) == 0) {
        cluster->is_updating = 1;
        int status = updateCluster(cluster);
        if (status == CLUSTER_RECONFIG_ERR)
            addReplyError(req->client, "Update failed", req->id);
        else if (status == CLUSTER_RECONFIG_WAIT)
            addReplyString(req->client, "waiting", req->id);
        else if (status == CLUSTER_RECONFIG_STARTED)
            addReplyString(req->client, "started", req->id);
        else if (status == CLUSTER_RECONFIG_ENDED)
            addReplyString(req->client, "done", req->id);
        else addReplyString(req->client, "OK", req->id);
    } else if (strcasecmp("help", subcmd) == 0) {
        addReplyHelp(req->client, proxyCommandSubcommandClusterHelp, req->id);
    } else if (strcasecmp("status", subcmd) == 0 ||
               strcasecmp("connection", subcmd) == 0 ||
               strcasecmp("nodes", subcmd) == 0) {
        fetch_info = 1;
        info_field = subcmd;
    } else {
        addReplyErrorUnknownSubcommand(req->client, "PROXY CLUSTER",
            "PROXY CLUSTER HELP", req->id);
    }
    if (fetch_info) {
        if (!initReplyArray(req->client)) {
            addReplyError(req->client, ERROR_OOM, req->id);
            return;
        }
        if (!info_field || strcasecmp("status", info_field) == 0) {
            char *status = "updated";
            if (cluster->broken) status = "broken";
            else if (cluster->is_updating || cluster->update_required)
                status = "updating";
            addReplyString(req->client, "status", req->id);
            addReplyString(req->client, status, req->id);
        }
        if (!info_field || strcasecmp("connection", info_field) == 0) {
            char *connection = "shared";
            if (req->client->cluster == cluster) connection = "private";
            addReplyString(req->client, "connection", req->id);
            addReplyString(req->client, connection, req->id);
        }
        if (!info_field || strcasecmp("nodes", info_field) == 0) {
            list *names = clusterGetMasterNames(cluster);
            addReplyString(req->client, "nodes", req->id);
            if (!names) addReplyNull(req->client, req->id);
            else {
                sds c = sdscatfmt(sdsempty(), "*%U\r\n", listLength(names));
                listIter li;
                listNode *ln;
                listRewind(names, &li);
                while ((ln = listNext(&li))) {
                    char *name = ln->value;
                    clusterNode *node = getNodeByName(cluster, name);
                    if (!node) {
                        c = sdscatprintf(c, "$-1\r\n");
                        continue;
                    }
                    int connected = (node->connection != NULL &&
                                     node->connection->context != NULL);
                    if (node->replicas_count < 0) {
                        node->replicas_count = 0;
                        listIter rli;
                        listNode *rln;
                        listRewind(cluster->nodes, &rli);
                        while ((rln = listNext(&rli))) {
                            clusterNode *r = rln->value;
                            if (!r->is_replica || !r->replicate) continue;
                            if (sdscmp(r->replicate, name) == 0)
                                node->replicas_count++;
                        }
                    }
                    c = sdscatfmt(c, "*%i\r\n", 12);
                    c = sdscat(c, "+name\r\n");
                    c = sdscatprintf(c, "+%s\r\n", node->name);
                    c = sdscat(c, "+ip\r\n");
                    c = sdscatprintf(c, "+%s\r\n", node->ip);
                    c = sdscat(c, "+port\r\n");
                    c = sdscatfmt(c, ":%i\r\n", node->port);
                    c = sdscat(c, "+slots\r\n");
                    c = sdscatfmt(c, ":%i\r\n", node->slots_count);
                    c = sdscat(c, "+replicas\r\n");
                    c = sdscatfmt(c, ":%i\r\n", node->replicas_count);
                    c = sdscat(c, "+connected\r\n");
                    c = sdscatfmt(c, ":%i\r\n", connected);
                }
                listAddNodeTail(req->client->reply_array, c);
            }
        }
        addReplyArray(req->client, req->id);
    }
}

sds genInfoString(sds section, redisCluster *cluster) {
    int default_section = (section == NULL ||
                           strcasecmp("default", section) == 0);
    int all_sections = (!default_section && section &&
                        strcasecmp("all", section) == 0);
    sds info = sdsempty();
    int sections = 0;
    if (default_section || all_sections ||
        !strcasecmp("proxy", section))
    {
        if (sections++) info = sdscat(info,"\r\n");
        time_t uptime = time(NULL) - proxy.start_time;
        info = sdscatprintf(info,
            "# Proxy\r\n"
            "proxy_version:%s\r\n"
            "proxy_git_sha1:%s\r\n"
            "proxy_git_dirty:%i\r\n"
            "proxy_git_branch:%s\r\n"
            "os:%s %s %s\r\n"
            "arch_bits:%d\r\n"
            "multiplexing_api:%s\r\n"
            "gcc_version:%d.%d.%d\r\n"
            "process_id:%ld\r\n"
            "threads:%d\n"
            "tcp_port:%d\r\n"
            "uptime_in_seconds:%jd\r\n"
            "uptime_in_days:%jd\r\n"
            "config_file:%s\r\n"
            "acl_user:%s\r\n",
            REDIS_CLUSTER_PROXY_VERSION,
            redisClusterProxyGitSHA1(),
            strtol(redisClusterProxyGitDirty(), NULL, 10) > 0,
            redisClusterProxyGitBranch(),
            proxy_os.sysname, proxy_os.release, proxy_os.machine,
            ((sizeof(long) == 8) ? 64 : 32),
            aeGetApiName(),
#ifdef __GNUC__
            __GNUC__,__GNUC_MINOR__,__GNUC_PATCHLEVEL__,
#else
            0,0,0,
#endif
            (long) getpid(),
            config.num_threads,
            config.port,
            (intmax_t)uptime,
            (intmax_t)(uptime/(3600*24)),
            (proxy.configfile ? proxy.configfile : ""),
            (config.auth_user ? config.auth_user : "default")
        );
        if (proxy.unixsocket_fd != -1) {
            info = sdscatprintf(info,
                 "unix_socket:%s\r\n"
                 "unix_socket_permissions:%o\r\n",
                 config.unixsocket,
                 config.unixsocketperm
            );
        }
    }
    if (default_section || all_sections ||
        !strcasecmp("memory", section))
    {
        char hmem[64];
        char total_system_hmem[64];
        size_t zmalloc_used = zmalloc_used_memory();
        size_t total_system_mem = proxy.system_memory_size;

        bytesToHuman(hmem,zmalloc_used);
        bytesToHuman(total_system_hmem,total_system_mem);
        if (sections++) info = sdscat(info,"\r\n");
        info = sdscatprintf(info,
            "# Memory\r\n"
            "used_memory:%zu\r\n"
            "used_memory_human:%s\r\n"
            "total_system_memory:%lu\r\n"
            "total_system_memory_human:%s\r\n",
            zmalloc_used,
            hmem,
            (unsigned long)total_system_mem,
            total_system_hmem
        );
    }
    if (default_section || all_sections ||
        !strcasecmp("clients", section))
    {
        if (sections++) info = sdscat(info,"\r\n");
        info = sdscatprintf(info,
                     "# Clients\r\n"
                     "connected_clients:%" PRId64 "\r\n"
                     "max_clients:%d\r\n",
                     proxy.numclients,
                     config.max_clients
        );
        int i;
        for (i = 0; i < config.num_threads; i++) {
            info = sdscatprintf(info,
                "thread_%d_clinets:%" PRId64 "\r\n",
                i, proxy.threads[i]->process_clients
            );
        }
    }
    if (default_section || all_sections ||
        !strcasecmp("cluster", section))
    {
        if (sections++) info = sdscat(info,"\r\n");
        redisClusterEntryPoint *ep = (cluster ? cluster->entry_point : NULL);
        info = sdscatprintf(info,
                     "# Cluster\r\n"
                     "address:%s\r\n"
                     "entry_node:%s:%d\r\n",
                     (ep && ep->address ? ep->address : ""),
                     (ep && ep->host ? ep->host : ""),
                     (ep ? ep->port : 0)

        );
    }
    return info;
}

/* Command handlers */

int commandWithPrivateConnection(void *r){
    clientRequest *req = r;
    client *c = req->client;
    redisCluster *cluster = getCluster(c);
    if (cluster && (cluster->broken || cluster->is_updating))
        return PROXY_COMMAND_UNHANDLED;
    if (!disableMultiplexingForClient(c)) {
        unlinkClient(c);
        return PROXY_COMMAND_HANDLED;
    }
    return PROXY_COMMAND_UNHANDLED;
}

int xreadCommand(void *r) {
    clientRequest *req = r;
    int i, blocking = 0;
    for (i = 1; i < req->argc; i++) {
        if (i >= req->offsets_size) break;
        int offset = req->offsets[i];
        int len = req->lengths[i];
        assert(((size_t) (offset + len)) < sdslen(req->buffer));
        char *arg = req->buffer + offset;
        blocking = (
            memcmp(arg, "block", len) == 0 ||
            memcmp(arg, "BLOCK", len) == 0
        );
        if (blocking) break;
    }
    if (blocking) {
        client *c = req->client;
        redisCluster *cluster = getCluster(c);
        if (cluster && (cluster->broken || cluster->is_updating))
            return PROXY_COMMAND_UNHANDLED;
        if (!disableMultiplexingForClient(c)) {
            unlinkClient(c);
            return PROXY_COMMAND_HANDLED;
        }
    }
    return PROXY_COMMAND_UNHANDLED;
}

int securityWarningCommand(void *r) {
    clientRequest *req = r;
    proxyLogWarn("Possible SECURITY ATTACK detected. It looks like somebody "
        "is sending POST or Host: commands to Redis. This is likely due to an "
        "attacker attempting to use Cross Protocol Scripting to compromise "
        "your Redis instance. Connection aborted.");
    unlinkClient(req->client);
    return PROXY_COMMAND_HANDLED;
}

int pingCommand(void *r) {
    clientRequest *req = r;
    addReplyString(req->client, "PONG", req->id);
    return PROXY_COMMAND_HANDLED;
}

int multiCommand(void *r) {
    clientRequest *req = r;
    client *c = req->client;
    if (!c->multi_transaction) {
        /* Disable multiplexing for the requesting client and create a
         * private cluster connection for it. */
        if (!disableMultiplexingForClient(c)) {
            unlinkClient(c);
            return PROXY_COMMAND_HANDLED;
        }
        if (c->multi_request != NULL) freeRequest(c->multi_request);
        c->multi_transaction = 1;
        /* Defer the MULTI request to be sent later just before
         * the first query of the transaction, since it's not possibile
         * to determine the target node in this moment. */
        c->multi_request = req;
        c->multi_request->owned_by_client = 1;
    } else return PROXY_COMMAND_UNHANDLED;
    addReplyString(c, "OK", req->id);
    return PROXY_COMMAND_HANDLED;
}

int execOrDiscardCommand(void *r) {
    clientRequest *req = r;
    client *c = req->client;
    if (!c->multi_transaction) {
        sds err = sdsnew(req->command->name);
        sdstoupper(err);
        err = sdscat(err, " without MULTI");
        addReplyError(c, err, req->id);
        sdsfree(err);
        return PROXY_COMMAND_HANDLED;
    }
    if (c->multi_request != NULL && c->multi_request->node != NULL)
        req->node = c->multi_request->node;
    req->closes_transaction = 1;
    return PROXY_COMMAND_UNHANDLED;
}

int authCommand(void *r) {
    int status = PROXY_COMMAND_UNHANDLED;
    clientRequest *req = r;
    client *c = req->client;
    sds user = NULL, passw = NULL;
    if (req->argc == 1) {
        addReplyErrorWrongArgc(c, "auth", req->id);
        status = PROXY_COMMAND_HANDLED;
        goto final;
    } else if (req->argc == 2) {
        if (req->offsets_size < 2) {
            unlinkClient(c);
            status = PROXY_COMMAND_HANDLED;
            goto final;
        }
        passw = sdsnewlen(req->buffer + req->offsets[1], req->lengths[1]);
    } else if (req->argc > 2) {
        if (req->offsets_size < 3) {
            unlinkClient(c);
            status = PROXY_COMMAND_HANDLED;
            goto final;
        }
        user = sdsnewlen(req->buffer + req->offsets[1], req->lengths[1]);
        passw = sdsnewlen(req->buffer + req->offsets[2], req->lengths[2]);
    }
    if ((user && (!config.auth_user || strcmp(user, config.auth_user)) != 0) ||
        (!user && config.auth_user))
    {
        /* Disable multiplexing for this client, since it's requiring to
         * authenticate itself with credentials different from the ones
         * used by the proxy. */
        int is_handled = commandWithPrivateConnection(req);
        if (c->status == CLIENT_STATUS_UNLINKED || c->cluster == NULL) {
            status = is_handled;
            goto final;
        }
        c->auth_user = user;
        c->auth_passw = passw;
    }
final:
    if (user != NULL && user != c->auth_user) sdsfree(user);
    if (passw != NULL && passw != c->auth_passw) sdsfree(passw);
    return status;
}

/* Scan all master nodes in the cluster. Node order is alphabeticallty and it's
 * taken from `clusterGetMasterNames`. The index of the node is taken from the
 * cursor last 4 digits. If the index is beyond the list of all the master
 * the choosen node will be the last one. If the cursor is '0', the choosen
 * node will be the first one. This special cursor format will be handled by
 * `handleScanReply`, that will append the index of the node to scan to the
 * cursor replied from the cluster. The real cursor sent to the cluster won't
 * actually contain the index suffix. */
int scanCommand(void *r) {
    clientRequest *req = r;
    client *c = req->client;
    if (req->argc < 2) {
        addReplyErrorWrongArgc(c, req->command->name, req->id);
        return PROXY_COMMAND_HANDLED;
    }
    if (req->offsets_size < 2) {
        addReplyError(c, ERROR_INVALID_QUERY, req->id);
        return PROXY_COMMAND_HANDLED;
    }
    int status = PROXY_COMMAND_UNHANDLED;
    sds cursor = sdsnewlen(req->buffer + req->offsets[1], req->lengths[1]);
    char *errptr = NULL;
    uint64_t cursor_n = strtoll(cursor, &errptr, 10);
    int idx = 0;
    if (cursor_n == 0) {
        if (errptr && *errptr != '\0') {
            proxyLogDebug("Invalid SCAN cursor: '%s'. (errptr: '%s')",
                          cursor, errptr);
            addReplyError(c, ERROR_INVALID_QUERY, req->id);
            status = PROXY_COMMAND_HANDLED;
            goto final;
        }
    } else {
        /* Take the last 4-digits in the cursor: they'll be used as the
         * node index */
         int crslen = sdslen(cursor);
         if (crslen > 4) {
            int prefixlen = crslen - 4;
            char *p = cursor + prefixlen;
            /* Read the node index */
            idx = atoi(p);
            p = req->buffer + req->offsets[1] + crslen;
            sds remaining = sdsnew(p);
            sdsrange(cursor, 0, prefixlen - 1);
            p = req->buffer + req->offsets[1];
            while (*p != '$' && p > req->buffer) p--;
            if (p <= req->buffer) {
                addReplyError(c, ERROR_INVALID_QUERY, req->id);
                sdsfree(remaining);
                status = PROXY_COMMAND_HANDLED;
                goto final;
            }
            /* Remove the index suffix from the buffer that has to be sent
             * to the node. */
            sdsrange(req->buffer, 0, p - req->buffer);
            req->buffer = sdscatfmt(req->buffer, "%u\r\n%S%S",
                                    (unsigned int) prefixlen,
                                    cursor, remaining);
            sdsfree(remaining);
         }
    }
    redisCluster *cluster = getCluster(c);
    list *node_names = clusterGetMasterNames(cluster);
    if (!node_names) goto final;
    /* If the index is beyond the current nodes, use the last node. */
    if ((unsigned long)idx >= listLength(node_names))
        idx = listLength(node_names) - 1;
    listNode *ln = listIndex(node_names, (long) idx);
    clusterNode *node = NULL;
    if (ln) {
        sds name = listNodeValue(ln);
        if (name) node = getNodeByName(cluster, name);
    }
    if (!node) {
        addReplyError(c, ERROR_NO_NODE, req->id);
        status = PROXY_COMMAND_HANDLED;
        goto final;
    }
    /* Set the choosen node in the request. */
    req->node = node;
final:
    if (cursor) sdsfree(cursor);
    return status;
}

int proxyCommand(void *r) {
    clientRequest *req = r;
    sds subcmd = NULL, err = NULL;
    if (req->argc < 2) {
        addReplyErrorUnknownSubcommand(req->client, "PROXY", "PROXY HELP",
            req->id);
        goto final;
    }
    assert(req->offsets_size >= 2);
    int buflen = sdslen(req->buffer);
    int offset = req->offsets[1];
    int len = req->lengths[1];
    char *p = req->buffer + offset, *end = req->buffer + buflen;
    assert(p < end);
    assert((p + len) < end);
    subcmd = sdsnewlen(p, len);
    if (strcasecmp("config", subcmd) == 0) {
        if (req->argc < 3) {
            addReplyErrorUnknownSubcommand(req->client, "PROXY CONFIG",
                "PROXY HELP", req->id);
            goto final;
        }
        assert(req->offsets_size >= 3);
        offset = req->offsets[2];
        len = req->lengths[2];
        p = req->buffer + offset, end = req->buffer + buflen;
        assert(p < end);
        assert((p + len) < end);
        sds action = sdsnewlen(p, len), value = NULL;
        int is_set_action = 0;
        if (strcasecmp("get", action) == 0 || strcasecmp("set", action) == 0) {
            is_set_action = (action[0] == 's' || action[0] == 'S');
        } else {
            addReplyErrorUnknownSubcommand(req->client, "PROXY CONFIG",
                "PROXY HELP", req->id);
            sdsfree(action);
            goto final;
        }
        sdsfree(action);
        if (req->argc < 4 || (is_set_action && req->argc < 5)) {
            addReplyErrorUnknownSubcommand(req->client, "PROXY CONFIG",
                "PROXY HELP", req->id);
            goto final;
        }
        assert(req->offsets_size >= 4);
        offset = req->offsets[3];
        len = req->lengths[3];
        p = req->buffer + offset, end = req->buffer + buflen;
        assert(p < end);
        assert((p + len) < end);
        sds option = sdsnewlen(p, len);
        if (is_set_action) {
            assert(req->offsets_size >= 5);
            offset = req->offsets[4];
            len = req->lengths[4];
            p = req->buffer + offset, end = req->buffer + buflen;
            assert(p < end);
            assert((p + len) < end);
            value = sdsnewlen(p, len);
        }
        sds reply = proxySubCommandConfig(req, option, value, &err);
        if (reply != NULL) {
            addReplyString(req->client, reply, req->id);
            sdsfree(reply);
        }
        sdsfree(option);
        if (value != NULL) sdsfree(value);
    } else if (strcasecmp("multiplexing", subcmd) == 0) {
        if (req->argc < 3) {
            addReplyErrorUnknownSubcommand(req->client, "PROXY MULTIPLEXING",
                "PROXY HELP", req->id);
            goto final;
        }
        assert(req->offsets_size >= 3);
        offset = req->offsets[2];
        len = req->lengths[2];
        p = req->buffer + offset, end = req->buffer + buflen;
        assert(p < end);
        assert((p + len) < end);
        sds action = sdsnewlen(p, len);
        if (strcasecmp("status", action) == 0) {
            char *status = (req->client->cluster != NULL ? "off" : "on");
            addReplyString(req->client, status, req->id);
        } else if (strcasecmp("off", action) == 0) {
            if (disableMultiplexingForClient(req->client))
                addReplyString(req->client, "OK", req->id);
            else
                err = sdsnew("Failed to disable multiplexing");
        } else {
            addReplyErrorUnknownSubcommand(req->client, "PROXY MULTIPLEXING",
                "PROXY HELP", req->id);
        }
        sdsfree(action);
    } else if (strcasecmp("ping", subcmd) == 0) {
        addReplyString(req->client, "PONG", req->id);
    } else if (strcasecmp("info", subcmd) == 0) {
        sds section = NULL;
        if (req->argc > 3) {
            addReplyErrorUnknownSubcommand(req->client, "PROXY INFO",
                "PROXY HELP", req->id);
            goto final;
        } else if (req->argc == 3) {
            assert(req->offsets_size >= 3);
            int offset = req->offsets[2];
            int len = req->lengths[2];
            section = sdsnewlen(req->buffer + offset, len);
        }
        sds infostr = genInfoString(section, getCluster(req->client));
        addReplyBulkString(req->client, infostr, req->id);
        sdsfree(infostr);
        if (section != NULL) sdsfree(section);
    } else if (strcasecmp("command", subcmd) == 0) {
        sds cmdtype = NULL;
        if (req->argc > 2) {
            assert(req->offsets_size >= 2);
            int offset = req->offsets[2];
            int len = req->lengths[2];
            cmdtype = sdsnewlen(req->buffer + offset, len);
        }
        proxySubCommandCommand(req, cmdtype);
        if (cmdtype) sdsfree(cmdtype);
    } else if (strcasecmp("client", subcmd) == 0) {
        if (req->argc < 3) {
            addReplyErrorUnknownSubcommand(req->client, "PROXY CLIENT",
                "PROXY CLIENT HELP", req->id);
            goto final;
        }
        assert(req->offsets_size >= 3);
        int offset = req->offsets[2];
        int len = req->lengths[2];
        sds arg = sdsnewlen(req->buffer + offset, len);
        proxySubCommandClient(req, arg);
        if (arg) sdsfree(arg);
    } else if (strcasecmp("cluster", subcmd) == 0) {
        sds arg = NULL;
        if (req->argc > 2) {
            assert(req->offsets_size >= 2);
            arg = sdsnewlen(req->buffer + req->offsets[2], req->lengths[2]);
        }
        proxySubCommandCluster(req, arg);
        if (arg) sdsfree(arg);
    } else if (strcasecmp("log", subcmd) == 0) {
        int loglvl = LOGLEVEL_DEBUG;
        sds msg = NULL;
        /* PROXY LOG <LEVEL> <MESSAGE> */
        if (req->argc == 4) {
            assert(req->offsets_size >= 4);
            int offset = req->offsets[2];
            int len = req->lengths[2];
            int i;
            sds lvlname = sdsnewlen(req->buffer + offset, len);
            loglvl = -1;
            for (i = 0; i <= LOGLEVEL_ERROR; i++) {
                if (strcasecmp(lvlname, redisProxyLogLevels[i]) == 0) {
                    loglvl = i;
                    break;
                }
            }
            if (loglvl < 0) {
                err = sdsnew("Invalid log level, valid level names: ");
                for (i = 0; i <= LOGLEVEL_ERROR; i++) {
                    char *sep = (i > 0 ? ", " : "");
                    err = sdscatfmt(err, "%s%s", sep, redisProxyLogLevels[i]);
                }
                goto final;
            }
            offset = req->offsets[3];
            len = req->lengths[3];
            msg = sdsnewlen(req->buffer + offset, len);
        /* PROXY LOG <MESSAGE> */
        } else if (req->argc == 3) {
            assert(req->offsets_size >= 3);
            int offset = req->offsets[2];
            int len = req->lengths[2];
            msg = sdsnewlen(req->buffer + offset, len);
        } else {
            addReplyErrorUnknownSubcommand(req->client, "PROXY LOG",
                "PROXY HELP", req->id);
            goto final;
        }
        int msglen = sdslen(msg);
        if (msglen > PROXY_CMD_LOG_MAX_LEN) {
            sdsfree(msg);
            err = sdsnew("Log message too long!");
            goto final;
        }
        proxyLog(loglvl, msg);
        sdsfree(msg);
        addReplyString(req->client, "OK", req->id);
    } else if (strcasecmp("debug", subcmd) == 0) {
        if (req->argc < 3) {
            addReplyErrorUnknownSubcommand(req->client, "PROXY DEBUG",
                "PROXY DEBUG HELP", req->id);
            goto final;
        }
        assert(req->offsets_size >= 3);
        int offset = req->offsets[2];
        int len = req->lengths[2];
        sds type = sdsnewlen(req->buffer + offset, len);
        if (strcasecmp("segfault", type) == 0) *((char*)-1) = 'x';
        else if (strcasecmp("assert", type) == 0) assert(1 == 2);
        else if (strcasecmp("help", type) == 0)
            addReplyHelp(req->client,proxyCommandSubcommandDebugtHelp,req->id);
        else if (strcasecmp("kill", type) == 0) {
            if (req->argc < 4) {
                addReplyErrorUnknownSubcommand(req->client, "PROXY DEBUG",
                    "PROXY DEBUG HELP", req->id);
                goto final;
            }
            assert(req->offsets_size >= 4);
            offset = req->offsets[3];
            len = req->lengths[3];
            int thread_id, signal = SIGTERM;
            sds thread_arg = sdsnewlen(req->buffer + offset, len);
            if (strcasecmp("main", thread_arg) == 0) thread_id = -1;
            else if (strcasecmp("self", thread_arg) == 0)
                thread_id = getCurrentThreadID();
            else thread_id = atoi(thread_arg);
            sdsfree(thread_arg);
            if (thread_id >= config.num_threads) {
                addReplyError(req->client, "Invalid thread id", req->id);
                goto final;
            }
            pthread_t thread;
            if (thread_id < 0) thread = proxy.main_thread;
            else thread = proxy.threads[thread_id]->thread;
            if (req->argc > 4) {
                assert(req->offsets_size > 4);
                offset = req->offsets[4];
                len = req->lengths[4];
                sds sig = sdsnewlen(req->buffer + offset, len);
                if (isdigit(sig[0])) signal = atoi(sig);
                else {
                    if (strcasecmp("TERM", sig) == 0) signal = SIGTERM;
                    else if (strcasecmp("INT", sig) == 0) signal = SIGINT;
                    else if (strcasecmp("KILL", sig) == 0) signal = SIGKILL;
                    else {
                        sdsfree(sig);
                        addReplyError(req->client, "Invalid signal", req->id);
                        goto final;
                    }
                }
                sdsfree(sig);
            }
            addReplyString(req->client, "OK", req->id);
            pthread_kill(thread, signal);
        } else {
            addReplyErrorUnknownSubcommand(req->client, "PROXY DEBUG",
                "PROXY DEBUG HELP", req->id);
            goto final;
        }
        sdsfree(type);
    } else if (strcasecmp("shutdown", subcmd) == 0) {
        int asap = 0;
        if (req->argc > 2) {
            assert(req->offsets_size >= 2);
            int offset = req->offsets[2];
            int len = req->lengths[2];
            sds mode = sdsnewlen(req->buffer + offset, len);
            asap = (strcasecmp("asap", mode) == 0);
            sdsfree(mode);
        }
        if (asap) proxy.exit_asap = 1;
        kill(getpid(), SIGINT);
    } else if (strcasecmp("help", subcmd) == 0) {
        addReplyHelp(req->client, proxyCommandHelp, req->id);
    } else {
        addReplyErrorUnknownSubcommand(req->client, "PROXY",
            "PROXY HELP", req->id);
    }
final:
    if (err != NULL) {
        addReplyError(req->client, err, req->id);
        sdsfree(err);
    }
    if (subcmd != NULL) sdsfree(subcmd);
    freeRequest(req);
    return PROXY_COMMAND_HANDLED;
}

/* Reply Handlers */

int mergeReplies(void *_reply, void *_req, char *buf, int len) {
    UNUSED(_reply);
    UNUSED(buf);
    UNUSED(len);
    clientRequest *req = _req;
    raxIterator iter;
    raxStart(&iter, req->child_replies);
    if (!raxSeek(&iter, "^", NULL, 0)) {
        raxStop(&iter);
        addReplyError(req->client, ERROR_MULTIPLE_REPLIES_ITER_FAIL,
                      req->id);
        return 0;
    }
    int count = 0, ok = 1;
    sds reply = NULL;
    sds merged_replies = sdsempty();
    char *err = NULL;
    sds replyrepr = NULL;
    while (raxNext(&iter)) {
        sds child_reply = (sds) iter.data;
        if (child_reply == NULL) continue;
        if (child_reply[0] == '-') {
            /* Reply is an error, reply the error and exit. */
            addReplyRaw(req->client, child_reply, sdslen(child_reply),
                        req->id);
            raxStop(&iter);
            return 1;
        }
        if (config.dump_buffer)
            replyrepr = sdsRepr(child_reply);
        char *p = strchr(child_reply, '*'), *endl = NULL, *strptr = NULL;
        ok = (p != NULL);
        if (ok) endl = strchr(++p, '\r');
        ok = (endl != NULL);
        if (!ok) {
            if (config.dump_buffer) {
                proxyLogDebug("Child reply:\n%s\np:\n%s\nendl:\n%s",
                              replyrepr, p, endl);
            }
            err = ERROR_MERGE_REPLY_INVALID_FMT;
            goto final;
        }
        *endl = '\0';
        int c = strtol(p, &strptr, 10);
        ok = (strptr != p);
        if (ok) ok = (*(++endl) == '\n');
        if (!ok) {
            if (config.dump_buffer) {
                proxyLogDebug("Invalid count!\nChild reply:\n%s\np:\n%s\n"
                              "endl:\n%s", replyrepr, p, endl);
            }
            err = ERROR_MERGE_REPLY_INVALID_FMT;
            goto final;
        }
        count += c;
        p = endl + 1;
        merged_replies = sdscatfmt(merged_replies, "%s", p);
    }
final:
    raxStop(&iter);
    if (replyrepr != NULL) sdsfree(replyrepr);
    proxyLogDebug("Writing reply for request " REQID_PRINTF_FMT
                  " to client buffer...",
                  REQID_PRINTF_ARG(req));
    if (err != NULL) {
        addReplyError(req->client, err, req->id);
        proxyLogDebug("%s", err);
    } else {
        reply = sdscatfmt(sdsempty(), "*%u\r\n%S", count, merged_replies);
        addReplyRaw(req->client, reply, sdslen(reply), req->id);
    }
    req->client->min_reply_id = req->max_child_reply_id + 1;
    if (reply) sdsfree(reply);
    sdsfree(merged_replies);
    return ok;
}

/* Reply with the first reply within multiple replies from multiple queries.
 * If there's at least an error within the replies, reply with the error. */
int getFirstMultipleReply(void *_reply, void *_req, char *buf, int len) {
    UNUSED(_reply);
    UNUSED(buf);
    UNUSED(len);
    clientRequest *req = _req;
    raxIterator iter;
    raxStart(&iter, req->child_replies);
    if (!raxSeek(&iter, "^", NULL, 0)) {
        raxStop(&iter);
        addReplyError(req->client, ERROR_MULTIPLE_REPLIES_ITER_FAIL,
                      req->id);
        return 0;
    }
    sds first = NULL;
    while (raxNext(&iter)) {
        sds child_reply = (sds) iter.data;
        if (child_reply == NULL) continue;
        if (child_reply[0] == '-') {
            /* Reply is an error, so reply the error to the client */
            first = child_reply;
            break;
        }
        if (first == NULL) first = child_reply;
    }
    raxStop(&iter);
    if (first) {
        addReplyRaw(req->client, first, sdslen(first), req->id);
        req->client->min_reply_id = req->max_child_reply_id + 1;
    } else return 0;
    return 1;
}

int getRandomReply(void *_reply, void *_req, char *buf, int len) {
    UNUSED(_reply);
    UNUSED(buf);
    UNUSED(len);
    clientRequest *req = _req;
    raxIterator iter;
    raxStart(&iter, req->child_replies);
    sds random_reply = NULL;
    while (random_reply == NULL) {
        if (raxSize(req->child_replies) == 0) break;
        if (!raxSeek(&iter, "^", NULL, 0)) {
            raxStop(&iter);
            char *err = (errno == ENOMEM ? ERROR_OOM :
                                           ERROR_MULTIPLE_REPLIES_ITER_FAIL);
            addReplyError(req->client, err, req->id);
            return 0;
        }
        if (!raxRandomWalk(&iter, 0)) {
            raxStop(&iter);
            char *err = (errno == ENOMEM ? ERROR_OOM :
                                           ERROR_MULTIPLE_REPLIES_ITER_FAIL);
            addReplyError(req->client, err, req->id);
            return 0;
        }
        while (raxNext(&iter)) {
            random_reply = (sds) iter.data;
            if (random_reply == NULL ||random_reply[0] == '-') {
                if (raxRemove(req->child_replies, iter.key, iter.key_len, NULL))
                    raxSeek(&iter,">",iter.key,iter.key_len);
                continue;
            }
            if (random_reply != NULL)  break;
        }
    }
    raxStop(&iter);
    if (random_reply == NULL) addReplyNull(req->client, req->id);
    else addReplyRaw(req->client, random_reply, sdslen(random_reply), req->id);
    req->client->min_reply_id = req->max_child_reply_id + 1;
    return 1;
}

int sumReplies(void *_reply, void *_req, char *buf, int len) {
    UNUSED(_reply);
    UNUSED(buf);
    UNUSED(len);
    clientRequest *req = _req;
    raxIterator iter;
    raxStart(&iter, req->child_replies);
    if (!raxSeek(&iter, "^", NULL, 0)) {
        raxStop(&iter);
        addReplyError(req->client, ERROR_MULTIPLE_REPLIES_ITER_FAIL,
                      req->id);
        return 0;
    }
    uint64_t tot = 0;
    while (raxNext(&iter)) {
        sds child_reply = (sds) iter.data;
        if (child_reply == NULL) continue;
        if (child_reply[0] == '-') {
            /* Reply is an error, reply the error and exit. */
            addReplyRaw(req->client, child_reply, sdslen(child_reply),
                        req->id);
            raxStop(&iter);
            return 1;
        }
        if (child_reply[0] == ':') {
            char *strprt = NULL;
            int64_t val = strtoll(child_reply + 1, &strprt, 10);
            tot += val;
        } else {
            addReplyError(req->client, ERROR_INVALID_REPLY, req->id);
            raxStop(&iter);
            return 0;
        }
    }
    raxStop(&iter);
    addReplyInt(req->client, tot, req->id);
    req->client->min_reply_id = req->max_child_reply_id + 1;
    return 1;
}

/* Handle replies for 'SCAN' command. The cursor contained in the reply will
 * be modified by appending the index of the node that has to be scanned, since
 * 'SCAN' command has to scann  all the master nodes in the cluster (see
 * `scanCommand`). The list of the nodes is alphabetically ordered.
 * When the reply contains a non-zero cursor, keep scanning the current
 * node (the one associated with the request). When reply will contain a zero
 * cursor, the scan for the current node is completed, so choose the next node
 * in the list, or just reply the zero cursor to the client if there's no
 * another nodes left. The index of the choosen node (the index is relative to
 * the alphabetically sorted list of masters) is then appended to the cursor
 * contained in the reply, as a 4-digits suffix. */
int handleScanReply(void *_reply, void *_req, char *buf, int len) {
    UNUSED(_reply);
    int status = PROXY_REPLY_UNHANDLED;
    clientRequest *req = _req;
    client *c = req->client;
    if (req->node == NULL || req->node->name == NULL) {
        addReplyError(c, "Missing node, probabily disconnected", req->id);
        freeRequest(req);
        return 0;
    }
    sds buffer = sdsnewlen(buf, len), cursor = NULL, crslenstr = NULL,
                 remaining = NULL;
    char *p = strchr(buffer, '$'), *err = NULL, *lenptr, *crsptr;
    if (!p || ((++p) - buffer) >= len) {
        err = ERROR_INVALID_REPLY;
        goto final;
    }
    lenptr = p;
    p = strstr(lenptr, "\r\n");
    if (!p) {
        err = ERROR_INVALID_REPLY;
        goto final;
    }
    crslenstr = sdsnewlen(lenptr, (p - lenptr));
    int crslen = atoi(crslenstr);
    if (crslen == 0) {
        err = ERROR_INVALID_REPLY;
        goto final;
    }
    p += 2;
    if (!p || (p - buffer) >= len) {
        err = ERROR_INVALID_REPLY;
        goto final;
    }
    crsptr = p;
    p = strchr(crsptr, '\r');
    if (!p) {
        err = ERROR_INVALID_REPLY;
        goto final;
    }
    cursor = sdsnewlen(crsptr, crslen);
    char *errptr = NULL;
    uint64_t cursor_n = strtoll(cursor, &errptr, 10);
    if (cursor_n == 0 && errptr && *errptr != '\0') {
        err = ERROR_INVALID_REPLY;
        goto final;
    }
    redisCluster *cluster = getCluster(c);
    list *names = clusterGetMasterNames(cluster);
    if (!names) {
        err = "Failed to scan nodes: could not find cluster node names";
        goto final;
    }
    int idx = 0, found = 0;
    listIter li;
    listNode *ln;
    listRewind(names, &li);
    while ((ln = listNext(&li))) {
        sds name = listNodeValue(ln);
        if (strcmp(req->node->name, name) == 0) {
            found = 1;
            break;
        }
        idx++;
    }
    if (!found) {
        err = ERROR_NO_NODE;
        goto final;
    }
    /* Server replied with cursor = 0, so scan for current node ended.
     * Increment idx in order to take to next node to scan. If idx is
     * beyond the current nodes, scan ended for all the nodes. */
    if (cursor_n == 0) {
        /* Scan has been performed on all nodes, so simply reply as normal. */
        if ((unsigned long)(++idx) >= listLength(names)) {
            status = PROXY_REPLY_UNHANDLED;
            goto final;
        }
    }
    cursor = sdscatprintf(cursor, "%.04d", idx);
    crslen = sdslen(cursor);
    remaining = sdsnewlen(p, len - (p - buffer));
    sdsrange(buffer, 0, (lenptr - 1) - buffer);
    buffer = sdscatfmt(buffer, "%u\r\n%S%S", sdslen(cursor), cursor, remaining);
    addReplyRaw(c, buffer, sdslen(buffer), req->id);
    status = 1;
final:
    sdsfree(buffer);
    if (cursor) sdsfree(cursor);
    if (crslenstr) sdsfree(crslenstr);
    if (remaining) sdsfree(remaining);
    if (err) goto onerr;
    else if (status != PROXY_REPLY_UNHANDLED) freeRequest(req);
    return status;
onerr:
    addReplyError(c, err, req->id);
    freeRequest(req);
    return 0;
}

/* Get Keys Callbacks */

int zunionInterGetKeys(void *r, int *first_key, int *last_key, int *key_step,
                       int **skip, int *skiplen, char **err)
{
    int numkeys = 0;
    clientRequest *req = r;
    assert(skip != NULL);
    assert(skiplen != NULL);
    if (req->argc < 4) {
        if (err) *err = "Wrong number of arguments";
        return -1;
    }
    if (req->offsets_size < 4) {
        if (err) *err = ERROR_INVALID_QUERY;
        return -1;
    }
    *skip = zmalloc(sizeof(int));
    if (*skip == NULL) {
        if (err) *err = "Failed to alloc memory";
        return -1;
    }
    *skiplen = 1;
    /* Skip "num-keys" argument (index 2) */
    (*skip)[0] = 2;
    *first_key = 1; /* DESTKEY */
    /* Get "num-keys" argument value. */
    sds numkeys_s = sdsnewlen(req->buffer + req->offsets[2],
                              req->lengths[2]);
    numkeys = atoi(numkeys_s);
    sdsfree(numkeys_s);
    *last_key = 2 + numkeys;
    if (*last_key <= 2) {
        if (err) *err = "Invalid request format";
        return -1;
    }
    *key_step = 1;
    numkeys += 1;
    return numkeys;
}

int xreadGetKeys(void *r, int *first_key, int *last_key, int *key_step,
                 int **skip, int *skiplen, char **err)
{
    int numkeys = 0, i;
    clientRequest *req = r;
    UNUSED(skip);
    UNUSED(skiplen);
    if (req->argc < 4) {
        if (err) *err = "Wrong number of arguments";
        return -1;
    }
    if (req->offsets_size < 4) {
        if (err) *err = "Invalid request format";
        return -1;
    }
    int streams_pos = -1;
    for (i = 1; i < req->argc; i++) {
        char *arg = req->buffer + req->offsets[i];
        int len = req->lengths[i];
        if (!memcmp(arg, "block", len) ||
            !memcmp(arg, "BLOCK", len) ||
            !memcmp(arg, "count", len) ||
            !memcmp(arg, "COUNT", len))
        {
            i++;
        } else if (!memcmp(arg, "group", len) ||
                   !memcmp(arg, "GROUP", len))
        {
            i += 2;
        } else if (!memcmp(arg, "streams", len) ||
                   !memcmp(arg, "STREAMS", len))
        {
            streams_pos = i;
            break;
        } else break;
    }
    if (streams_pos != -1) numkeys = req->argc - streams_pos - 1;
    if (streams_pos == -1 || numkeys == 0 || numkeys % 2 != 0)
        goto syntax_err;
    numkeys /= 2; /* We have half the keys as there are arguments because
                     there are also the IDs, one per key. */
    if (numkeys <= 0) goto syntax_err;
    *first_key = streams_pos + 1;
    *last_key = *first_key + (numkeys - 1);
    *key_step = 1;
    return numkeys;
syntax_err:
    if (err) *err = "Syntax error";
    return -1;
}

int sortGetKeys(void *r, int *first_key, int *last_key, int *key_step,
                int **skip, int *skiplen, char **err)
{
    int numkeys = 0, storepos = -1, i;
    clientRequest *req = r;
    assert(skip != NULL);
    assert(skiplen != NULL);
    if (req->argc < 2) {
        if (err) *err = "Wrong number of arguments";
        return -1;
    }
    if (req->offsets_size < 2) {
        if (err) *err = "Invalid request format";
        return -1;
    }
    *first_key = 1;
    *key_step = 1;
    *skiplen = 0;
    for (i = 2; i < req->argc; i++) {
        char *arg = req->buffer + req->offsets[i];
        int len = req->lengths[i];
        if (!memcmp(arg, "store", len) ||
            !memcmp(arg, "STORE", len))
        {
            storepos = i;
            break;
        }
    }
    if (storepos == -1) *last_key = *first_key;
    else {
        *last_key = storepos + 1;
        *skiplen = storepos - *first_key;
        *skip = zmalloc(*skiplen * sizeof(int));
        if (*skip == NULL) {
            if (err) *err = "Failed to alloc memory";
            return -1;
        }
        for (i = 2; i <= storepos; i++) (*skip)[i - 2] = i;
    }
    return numkeys;
}

int evalGetKeys(void *r, int *first_key, int *last_key, int *key_step,
                int **skip, int *skiplen, char **err)
{
    int numkeys = 0;
    UNUSED(skip);
    UNUSED(skiplen);
    clientRequest *req = r;
    if (req->argc < 3) {
        if (err) *err = "Wrong number of arguments";
        return -1;
    }
    if (req->offsets_size < 3) {
        if (err) *err = "Invalid request format";
        return -1;
    }
    sds numkeys_s = sdsnewlen(req->buffer + req->offsets[2], req->lengths[2]);
    numkeys = atoi(numkeys_s);
    *first_key = 3;
    *last_key = 3 + numkeys;
    *key_step = 1;
    sdsfree(numkeys_s);
    return numkeys;
}

/* Proxy functions */

static void dumpQueue(clusterNode *node, int thread_id, int type) {
    if (node == NULL) return;
    redisClusterConnection *conn = node->connection;
    if (conn == NULL) return;
    list *queue = NULL;
    if (type == QUEUE_TYPE_PENDING) queue = conn->requests_pending;
    else if (type == QUEUE_TYPE_SENDING) queue = conn->requests_to_send;
    if (queue == NULL) return;
    sds msg = sdsnew("Node ");
    msg = sdscatprintf(msg, "%s:%d[thread %d] -> %s",
                       node->ip, node->port, thread_id,
                       (type == QUEUE_TYPE_PENDING ? "requests pending: [" :
                                                     "requests to send: ["));
    listIter li;
    listNode *ln;
    listRewind(queue, &li);
    int i = 0;
    while ((ln = listNext(&li))) {
        if (i++ > 0) msg = sdscat(msg, ", ");
        clientRequest *req = ln->value;
        if (req == NULL) msg = sdscat(msg, "NULL");
        else msg = sdscatprintf(msg, REQID_PRINTF_FMT, REQID_PRINTF_ARG(req));
    }
    msg = sdscat(msg, "]");
    proxyLogDebug(msg);
    sdsfree(msg);
}

redisCommandDef *getRedisCommand(sds name) {
    redisCommandDef *cmd = NULL;
    raxIterator iter;
    raxStart(&iter, proxy.commands);
    if (raxSeek(&iter, "=", (unsigned char*) name, sdslen(name)))
        if (raxNext(&iter)) cmd = (redisCommandDef *) iter.data;
    raxStop(&iter);
    return cmd;
}

void printHelp(void) {
    fprintf(stderr, mainHelpString,
        DEFAULT_PORT, DEFAULT_MAX_CLIENTS, DEFAULT_THREADS, MAX_THREADS,
        DEFAULT_TCP_KEEPALIVE, DEFAULT_TCP_BACKLOG, DEFAULT_PID_FILE,
        DEFAULT_UNIXSOCKETPERM, DEFAULT_CONNECTIONS_POOL_SIZE, MAX_POOL_SIZE,
        DEFAULT_CONNECTIONS_POOL_MINSIZE, DEFAULT_CONNECTIONS_POOL_INTERVAL,
        DEFAULT_CONNECTIONS_POOL_SPAWNRATE);
}

int parseOptions(int argc, char **argv) {
    int i;
    for (i = 1; i < argc; i++) {
        int lastarg = (i == (argc - 1));
        char *arg = argv[i];
        if ((!strcmp("-p", arg) || !strcmp("--port", arg)) && !lastarg)
            config.port = atoi(argv[++i]);
        else if ((!strcmp(argv[i],"-a") || !strcmp("--auth", arg)) && !lastarg)
            config.auth = zstrdup(argv[++i]);
        else if (!strcmp("--auth-user", arg) && !lastarg)
            config.auth_user = zstrdup(argv[++i]);
        else if (!strcmp("--disable-colors", arg))
            config.use_colors = 0;
        else if (!strcmp("--daemonize", arg))
            config.daemonize = 1;
        else if (!strcmp("--pidfile", arg) && !lastarg)
            config.pidfile = zstrdup(argv[++i]);
        else if (!strcmp("--logfile", arg) && !lastarg)
            config.logfile = zstrdup(argv[++i]);
        else if (!strcmp("--max-clients", arg) && !lastarg)
            config.max_clients = atoi(argv[++i]);
        else if (!strcmp("--tcpkeepalive", arg) && !lastarg)
            config.tcpkeepalive = atoi(argv[++i]);
        else if (!strcmp("--tcp-backlog", arg) && !lastarg)
            config.tcp_backlog = atoi(argv[++i]);
        else if (!strcmp("--connections-pool-size", arg) && !lastarg)
            config.connections_pool.size = atoi(argv[++i]);
        else if (!strcmp("--connections-pool-min-size", arg) && !lastarg)
            config.connections_pool.min_size = atoi(argv[++i]);
        else if (!strcmp("--connections-pool-spawn-every", arg) && !lastarg)
            config.connections_pool.spawn_every = atoi(argv[++i]);
        else if (!strcmp("--connections-pool-spawn-rate", arg) && !lastarg)
            config.connections_pool.spawn_rate = atoi(argv[++i]);
        else if (!strcmp("--dump-queries", arg))
            config.dump_queries = 1;
        else if (!strcmp("--dump-buffer", arg))
            config.dump_buffer = 1;
        else if (!strcmp("--dump-queues", arg))
            config.dump_queues = 1;
        else if (!strcmp(argv[i], "--unixsocket") && !lastarg)
            config.unixsocket = zstrdup(argv[++i]);
        else if (!strcmp(argv[i], "--unixsocketperm") && !lastarg) {
            errno = 0;
            config.unixsocketperm = (mode_t)strtol(argv[++i], NULL, 8);
            if (errno || config.unixsocketperm > 0777) {
                fprintf(stderr,"Invalid socket file permissions:%s\n",argv[i]);
                exit(1);
            }
        } else if (!strcmp("--bind", arg) && !lastarg) {
            if (config.bindaddr_count >= BINDADDR_MAX) {
                fprintf(stderr, "You can bind max. %d interfaces\n",
                        BINDADDR_MAX);
                exit(1);
            }
            config.bindaddr[config.bindaddr_count++] = zstrdup(argv[++i]);
        } else if (!strcmp("-c", arg) && !lastarg) {
            char *cfgfile = argv[++i];
            if (!parseOptionsFromFile(cfgfile)) exit(1);
            proxy.configfile = sdsnew(cfgfile);
        } else if (!strcmp("--threads", arg) && !lastarg) {
            config.num_threads = atoi(argv[++i]);
            if (config.num_threads > MAX_THREADS) {
                fprintf(stderr, "Warning: maximum threads allowed: %d\n",
                                MAX_THREADS);
                config.num_threads = MAX_THREADS;
            } else if (config.num_threads < 1) config.num_threads = 1;
        } else if (!strcmp("--log-level", arg) && !lastarg) {
            char *level_name = argv[++i];
            int j = 0, level = -1;
            for (; j <= LOGLEVEL_ERROR; j++) {
                if (!strcasecmp(level_name, redisProxyLogLevels[j])) {
                    level = j;
                    break;
                }
            }
            if (level < 0) {
                fprintf(stderr, "Invalid log level '%s', valid levels:\n", arg);
                for (j = 0; j <= LOGLEVEL_ERROR; j++) {
                    if (j > 0) fprintf(stderr, ", ");
                    fprintf(stderr, "%s", redisProxyLogLevels[j]);
                }
                fprintf(stderr, "\n");
                exit(1);
            }
            config.loglevel = level;
        } else if (!strcmp("--disable-multiplexing", arg) && !lastarg) {
            char *val = argv[++i];
            if (!strcasecmp("always", val))
                config.disable_multiplexing = CFG_DISABLE_MULTIPLEXING_ALWAYS;
            else if (!strcasecmp("auto", val))
                config.disable_multiplexing = CFG_DISABLE_MULTIPLEXING_AUTO;
            else {
                fprintf(stderr, "Invalid option for --disable-multiplexing, "
                        "valid options are:\nauto|always\n");
                exit(1);
            }
        } else if (!strcmp("--enable-cross-slot", arg)) {
            config.cross_slot_enabled = 1;
        } else if (!strcmp("--help", arg) || !strcmp("-h", arg)) {
            printHelp();
            exit(0);
        } else {
            if (*arg == '-') goto invalid;
            break;
        }
    }
    return i;
invalid:
    fprintf(stderr, "Invalid option '%s' or invalid number of option "
                    "arguments\n\n", argv[i]);
    printHelp();
    exit(1);
}

/* This function will try to raise the max number of open files accordingly to
 * the configured max number of clients. It also reserves a number of file
 * descriptors (proxy.min_reserved_fds) for thread pipes, listen sockets, and
 * so on.
 *
 * If it will not be possible to set the limit accordingly to the configured
 * max number of clients, the function will do the reverse setting
 * comfig.max_clients to the value that we can actually handle. */
void adjustOpenFilesLimit(void) {
    rlim_t maxfiles = config.max_clients + proxy.min_reserved_fds;
    struct rlimit limit;

    if (getrlimit(RLIMIT_NOFILE,&limit) == -1) {
        proxyLogWarn("Unable to obtain the current NOFILE limit (%s), "
                     "assuming 1024 and setting the max clients configuration "
                     "accordingly.", strerror(errno));
        config.max_clients = 1024 - proxy.min_reserved_fds;
    } else {
        rlim_t oldlimit = limit.rlim_cur;

        /* Set the max number of files if the current limit is not enough
         * for our needs. */
        if (oldlimit < maxfiles) {
            rlim_t bestlimit;
            int setrlimit_error = 0;

            /* Try to set the file limit to match 'maxfiles' or at least
             * to the higher value supported less than maxfiles. */
            bestlimit = maxfiles;
            while(bestlimit > oldlimit) {
                rlim_t decr_step = 16;

                limit.rlim_cur = bestlimit;
                limit.rlim_max = bestlimit;
                if (setrlimit(RLIMIT_NOFILE,&limit) != -1) break;
                setrlimit_error = errno;

                /* We failed to set file limit to 'bestlimit'. Try with a
                 * smaller limit decrementing by a few FDs per iteration. */
                if (bestlimit < decr_step) break;
                bestlimit -= decr_step;
            }

            /* Assume that the limit we get initially is still valid if
             * our last try was even lower. */
            if (bestlimit < oldlimit) bestlimit = oldlimit;

            if (bestlimit < maxfiles) {
                unsigned int old_max_clients = config.max_clients;
                config.max_clients = bestlimit - proxy.min_reserved_fds;
                /* max-clients is unsigned so may overflow: in order
                 * to check if max-clients is now logically less than 1
                 * we test indirectly via bestlimit. */
                if (bestlimit <= (rlim_t) proxy.min_reserved_fds) {
                    proxyLogWarn("Your current 'ulimit -n' "
                        "of %llu is not enough for the server to start. "
                        "Please increase your open file limit to at least "
                        "%llu. Exiting.",
                        (unsigned long long) oldlimit,
                        (unsigned long long) maxfiles);
                    exit(1);
                }
                proxyLogWarn("You requested max-clients of %d "
                    "requiring at least %llu max file descriptors.",
                    old_max_clients,
                    (unsigned long long) maxfiles);
                proxyLogWarn("Server can't set maximum open files "
                    "to %llu because of OS error: %s.",
                    (unsigned long long) maxfiles, strerror(setrlimit_error));
                proxyLogWarn("Current maximum open files is %llu. "
                    "max-clients has been reduced to %d to compensate for "
                    "low ulimit. "
                    "If you need higher max-clients increase 'ulimit -n'.",
                    (unsigned long long) bestlimit, config.max_clients);
            } else {
                proxyLogInfo("Increased maximum number of open files "
                    "to %llu (it was originally set to %llu).",
                    (unsigned long long) maxfiles,
                    (unsigned long long) oldlimit);
            }
        }
    }
}

/* Check that server.tcp_backlog can be actually enforced in Linux according
 * to the value of /proc/sys/net/core/somaxconn, or warn about it. */
static void checkTcpBacklogSettings(void) {
#ifdef HAVE_PROC_SOMAXCONN
    FILE *fp = fopen("/proc/sys/net/core/somaxconn","r");
    char buf[1024];
    if (!fp) return;
    if (fgets(buf,sizeof(buf),fp) != NULL) {
        int somaxconn = atoi(buf);
        if (somaxconn > 0 && somaxconn < proxy.tcp_backlog) {
            proxyLogWarn("The TCP backlog setting of %d cannot be enforced "
			 "because /proc/sys/net/core/somaxconn is set to the "
			 "lower value of %d.", proxy.tcp_backlog, somaxconn);
        }
    }
    fclose(fp);
#endif
}

static void initProxy(void) {
    int i;
    proxy.exit_asap = 0;
    proxy.neterr[0] = '\0';
    proxy.numclients = 0;
    proxy.system_memory_size = zmalloc_get_memory_size();
    proxy.min_reserved_fds = 10 + (config.num_threads * 3) +
                             (proxy.fd_count * 2);
    /* Populate commands table. */
    proxy.commands = raxNew();
    int command_count = sizeof(redisCommandTable) / sizeof(redisCommandDef);
    for (i = 0; i < command_count; i++) {
        redisCommandDef *cmd = redisCommandTable + i;
        raxInsert(proxy.commands, (unsigned char*) cmd->name,
                  strlen(cmd->name), cmd, NULL);
        if (strcasecmp("auth", cmd->name) == 0) authCommandDef = cmd;
        else if (strcasecmp("scan", cmd->name) == 0) scanCommandDef = cmd;
    }
    proxy.main_loop = aeCreateEventLoop(proxy.min_reserved_fds);
    proxy.threads = zmalloc(config.num_threads *
                            sizeof(proxyThread *));
    if (proxy.threads == NULL) {
        fprintf(stderr, "FATAL: failed to allocate memory for threads.\n");
        exit(1);
    }
    proxyLogHdr("Starting %d threads...", config.num_threads);
    for (i = 0; i < config.num_threads; i++) {
        proxyLogDebug("Creating thread %d...", i);
        proxy.threads[i] = NULL;
        proxy.threads[i] = createProxyThread(i);
        if (proxy.threads[i] == NULL) {
            proxyLogErr("FATAL: failed to create thread %d.", i);
            exit(1);
        }
        pthread_t *t = &(proxy.threads[i]->thread);
        if (pthread_create(t, NULL, execProxyThread, proxy.threads[i])){
            fprintf(stderr, "FATAL: Failed to start thread %d.\n", i);
            proxyLogErr("FATAL: Failed to start thread %d.", i);
            exit(1);
        }
    }
    proxyLogHdr("All thread(s) started!");
}

void closeListeningSockets() {
    int j;
    proxyLogInfo("Closing listening sockets.");
    for (j = 0; j < proxy.fd_count; j++) close(proxy.fds[j]);
    if (config.unixsocket) {
        proxyLogInfo("Removing the unix socket file.");
        unlink(config.unixsocket); /* don't care if this fails */
    }
}

static void releaseProxy(void) {
    int i;
    if (proxy.main_loop != NULL) {
        aeStop(proxy.main_loop);
        aeDeleteEventLoop(proxy.main_loop);
        proxy.main_loop = NULL;
    }
    if (proxy.threads != NULL) {
        for (i = 0; i < config.num_threads; i++) {
            proxyThread *thread =  proxy.threads[i];
            if (thread) freeProxyThread(thread);
            proxy.threads[i] = NULL;
        }
        zfree(proxy.threads);
    }
    if (proxy.commands)
        raxFree(proxy.commands);
    closeListeningSockets();
    for (i = 0; i < config.bindaddr_count; i++) {
        zfree(config.bindaddr[i]);
    }
    if (config.pidfile && config.pidfile[0] != '\0') unlink(config.pidfile);
}

void writeHandler(aeEventLoop *el, int fd, void *privdata, int mask) {
    UNUSED(el);
    UNUSED(fd);
    UNUSED(mask);
    client *c = privdata;
    assert(c != NULL);
    writeToClient(c);
}

static void writeRepliesToClients(struct aeEventLoop *el) {
    proxyThread *thread = el->privdata;
    assert(thread != NULL);
    if (thread->clients == NULL) return;
    listIter li;
    listNode *ln;
    listRewind(thread->clients, &li);
    while ((ln = listNext(&li)) != NULL) {
        client *c = ln->value;
        if (c->status == CLIENT_STATUS_UNLINKED) continue;
        if (!writeToClient(c)) continue;
        if (c->written > 0 && c->written < sdslen(c->obuf)) {
            if (installIOHandler(el, c->fd, AE_WRITABLE, writeHandler, c, 0)) {
                c->has_write_handler = 1;
            } else {
                c->has_write_handler = 0;
                proxyLogDebug("Failed to create write handler for client.");
            }
        }
    }
}

/* This function gets called every time threads' lopps are entering the
 * main loop of the event driven library, that is, before to sleep
 * for ready file descriptors. */
void beforeThreadSleep(struct aeEventLoop *eventLoop) {
    proxyThread *thread = eventLoop->privdata;
    writeRepliesToClients(eventLoop);
    processThreadPipeBufferForNewClients(thread);
    if (thread->cluster->is_updating ||
        thread->cluster->broken) return;
    listIter li;
    listNode *ln;
    listRewind(thread->cluster->nodes, &li);
    while ((ln = listNext(&li))) {
        clusterNode *node = ln->value;
        handleNextRequestsToCluster(node, NULL);
    }
    listRewind(thread->unlinked_clients, &li);
    while ((ln = listNext(&li))) {
        client *c = ln->value;
        listDelNode(thread->unlinked_clients, ln);
        if (c != NULL) {
            c->unlinked_clients_lnode = NULL;
            freeClient(c);
        }
    }
}

static int processThreadPipeBufferForNewClients(proxyThread *thread) {
    client *c = NULL;
    int buflen = sdslen(thread->msgbuffer);
    int msgsize = sizeof(c);
    int processed = 0, count = buflen / msgsize, i;
    for (i = 0; i < count; i++) {
        char *p = thread->msgbuffer + (i * msgsize);
        client **pc = (void*) p;
        c = (client *) *pc;
        /* If thread is going to be freed, free all clients that were
         * still waiting to be added on the thread itself. */
        if (thread->loop == NULL) {
            if (c != NULL && c != (void*) THREAD_MSG_STOP) freeClient(c);
            processed++;
            continue;
        }
        if (c == (void*) THREAD_MSG_STOP) {
            proxyLogInfo("Stopping thread %d", thread->thread_id);
            aeStop(thread->loop);
            processed++;
            sdsrange(thread->msgbuffer, processed * msgsize, -1);
            return processed;
        }
        aeEventLoop *el = thread->loop;
        int added = 0;
        int *p_added = &added;
        addObjectToList(c, thread, clients, p_added);
        if (!added) {
            proxyLogErr("Failed to add client %d:%" PRId64 " to thread %d",
                thread->thread_id, c->id, thread->thread_id);
            freeClient(c);
            processed++;
            continue;
        }
        proxyLogDebug("Client %d:%" PRId64 " added to thread %d",
                      c->thread_id, c->id, c->thread_id);
        errno = 0;
        if (!installIOHandler(el, c->fd, AE_READABLE, readQuery, c, 0)) {
            proxyLogErr("ERROR: Failed to create read query handler for "
                        "client %d:%" PRId64 " from %s", c->thread_id,
                        c->id, c->addr);
            errno = EL_INSTALL_HANDLER_FAIL;
            freeClient(c);
            processed++;
            continue;
        }
        c->status = CLIENT_STATUS_LINKED;
        processed++;
    }
    if (processed > 0)
        sdsrange(thread->msgbuffer, processed * msgsize, -1);
    return processed;
}

static void readThreadPipe(aeEventLoop *el, int fd, void *privdata, int mask) {
    UNUSED(el);
    UNUSED(mask);
    proxyThread *thread = privdata;
    int processed = 0, nread = 0;
    char buf[4096];
    nread = read(fd, buf, sizeof(buf));
    if (nread == -1) {
        if (errno == EAGAIN) {
            return;
        } else {
            proxyLogDebug("Error reading from thread pipe: %s",
                          strerror(errno));
            return;
        }
    }
    thread->msgbuffer = sdscatlen(thread->msgbuffer, buf, nread);
    processed = processThreadPipeBufferForNewClients(thread);
    UNUSED(processed);
}

static void printClusterConfiguration(redisCluster *cluster) {
    redisClusterEntryPoint *ep = cluster->entry_point;
    if (ep != NULL && ep->address != NULL)
        proxyLogHdr("Cluster Address: %s", ep->address);
    if (config.loglevel == LOGLEVEL_DEBUG) {
        int j;
        clusterNode *last_n = NULL;
        for (j = 0; j < CLUSTER_SLOTS; j++) {
            clusterNode *n = searchNodeBySlot(cluster, j);
            if (n == NULL) {
                proxyLogErr("NULL node for slot %d", j);
                break;
            }
            if (n != last_n) {
                last_n = n;
                proxyLogDebug("Slot %d -> node %d", j, n->port);
            }
        }
    }
    proxyLogHdr("Cluster has %d masters and %d replica(s)",
        cluster->masters_count, cluster->replicas_count);
}

/* Populate the thread's connections pool with already connected connections.
 * Every connection actually is a radix tree (rax) which maps different
 * redisClusterConnection objects to the names of the nodes of the cluster.
 * The connections pool is a list containing all the rax objects.
 * The functions populates the pool until maximum si reached, as defined by
 * the configuration option (configuraqble via '--connections-pool-size').
 * If rate is not zero, the pool will be populated until current + rate or
 * max is reached.
 * The function returns the final size of the pool or -1 in case of error.*/
static int populateConnectionsPool(proxyThread *thread, int rate) {
    int max = config.connections_pool.size;
    list *pool = thread->connections_pool;
    if (pool == NULL) pool = thread->connections_pool = listCreate();
    if (pool == NULL) {
        proxyLogWarn("Failed to allocate connections_pool for thread %d",
            thread->thread_id);
        return -1;
    }
    redisCluster *cluster = thread->cluster;
    if (pool == NULL || cluster == NULL) return -1;
    if (cluster->broken || cluster->is_updating || cluster->update_required)
        return -1;
    if (rate > 0) max = listLength(pool) + rate;
    if (max > config.connections_pool.size) max = config.connections_pool.size;
    int maxtries = max * 2, tries = 0;
    aeEventLoop *el = thread->loop;
    proxyLogDebug("Populating connections pool for thread %d",
        thread->thread_id);
    while (listLength(pool) < (unsigned long) max) {
        if (++tries >= maxtries) break;
        rax *connections = raxNew();
        if (connections == NULL) continue;
        listIter li;
        listNode *ln;
        listRewind(cluster->nodes, &li);
        while ((ln = listNext(&li))) {
            clusterNode *node = ln->value;
            if (node->is_replica || !node->name || !node->ip) continue;
            redisClusterConnection *conn = createClusterConnection();
            if (conn == NULL) break;
            conn->context = redisConnectNonBlock(node->ip, node->port);
            if (conn->context == NULL) continue;
            if (!installIOHandler(el, conn->context->fd, AE_WRITABLE,
                writeToClusterHandler, conn, 0)) {
                proxyLogWarn("Populate connection pool: failed to install "
                    "write handler for node %s:%d", node->ip, node->port);
                freeClusterConnection(conn);
                continue;
            }
            raxInsert(connections, (unsigned char*) node->name,
                      strlen(node->name), conn, NULL);
        }
        if (raxSize(connections) == 0) {
            raxFree(connections);
            continue;
        }
        listAddNodeTail(pool, connections);
    }
    proxyLogDebug("Connections pool for thread %d has %lu connections",
        thread->thread_id, listLength(pool));
    return (int) listLength(pool);
}

/* Try to recycle private connections to cluster owned by a client that
 * doesn't need them anymore (ie. it's going to be closed or freed) by
 * adding them to the thread's connections pool.
 * Connections are discarder if:
 *  - the pool is already full (its size is equal to `--connections-pool-size`)
 *  - connectionas are not valid (ie. disconnected, broken cluster, and so on)
 */
static int recyclePrivateClusterConnection(client *c) {
    redisCluster *cluster = c->cluster;
    assert(cluster != NULL);
    if (cluster->broken || cluster->is_updating || cluster->update_required)
        return 0;
    proxyThread *thread = getThread(c);
    if (thread == NULL) return 0;
    list *pool = thread->connections_pool;
    if (pool == NULL) return 0;
    /* Exit if the pool is already full. */
    if (listLength(pool) >= (unsigned long) config.connections_pool.size)
        return 0;
    rax *connections = raxNew();
    if (connections == NULL) return 0;
    listIter li, nli;
    listNode *ln, *nln;
    listRewind(cluster->nodes, &li);
    /* Cycle all nodes in the private cluster in order to get and check their
     * connection.
     * Ensure that:
     *  - The node must be a master
     *  - The connection must be connected
     *  - There are no requests not completely written to the cluster
     *  - There are no requests that still have to be read from the cluster
     */
    while ((ln = listNext(&li))) {
        clusterNode *node = listNodeValue(ln);
        if (node == NULL) goto fail;
        if (node->is_replica) continue;
        redisClusterConnection *conn = node->connection;
        if (conn == NULL) goto fail;
        if (!conn->connected) goto fail;
        listRewind(conn->requests_to_send, &nli);
        while ((nln = listNext(&nli))) {
            clientRequest *req = listNodeValue(nln);
            if (req != NULL && req->has_write_handler) goto fail;
        }
        listRewind(conn->requests_pending, &nli);
        while ((nln = listNext(&nli))) {
            clientRequest *req = listNodeValue(nln);
            if (req != NULL) goto fail;
        }
        /* Reset connection lists and node.*/
        listEmpty(conn->requests_to_send);
        listEmpty(conn->requests_pending);
        conn->node = NULL;
        /* Insert the connection by mapping it to the node's name. */
        raxInsert(connections, (unsigned char*) node->name,
                  strlen(node->name), conn, NULL);
        node->connection = NULL;
    }
    /* Add the connection to the pool. */
    listAddNodeHead(pool, connections);
    return 1;
fail:
    if (connections != NULL) {
        raxFreeWithCallback(connections,
            (void (*)(void *))freeClusterConnection);
    }
    return 0;
}

/* Function used by a time event (aeTimeEvent) that is registered whenever the
 * size of the thread's connections pool dropb below the configured minimum
 * (configurable via '--connections-pool-min-size').
 * The time event is executed with an interval specified in the global
 * configuration, configurable via the '--connections-pool-spawn-every' option.
 * The interval is expressed in milliseconds. */
static int threadConnectionPoolCron(aeEventLoop *el, long long id, void *data) {
    proxyThread *thread = el->privdata;
    UNUSED(id);
    UNUSED(data);
    int every = config.connections_pool.spawn_every;
    if (thread->connections_pool == NULL) goto finished;
    int size = listLength(thread->connections_pool);
    int rate = config.connections_pool.spawn_rate;
    if (size >= config.connections_pool.size) goto finished;
    size = populateConnectionsPool(thread, rate);
    if (size >= config.connections_pool.size || size < 0) goto finished;
    return every;
finished:
    thread->is_spawning_connections = 0;
    return AE_NOMORE;
}

static proxyThread *createProxyThread(int index) {
    int is_first = (index == 0);
    proxyThread *thread = zcalloc(sizeof(*thread));
    if (thread == NULL) return NULL;
    thread->loop = NULL;
    if (pipe(thread->io) == -1) {
        proxyLogErr("ERROR: failed to open pipe for thread!");
        zfree(thread);
        return NULL;
    }
    thread->thread_id = index;
    thread->next_client_id = 0;
    thread->process_clients = 0;
    thread->connections_pool = listCreate();
    thread->is_spawning_connections = 0;
    thread->cluster = createCluster(index);
    if (thread->cluster == NULL) {
        proxyLogErr("ERROR: failed to allocate cluster for thread: %d",
                    index);
        freeProxyThread(thread);
        return NULL;
    }
    if (is_first) proxyLogHdr("Fetching cluster configuration...");
    if (!fetchClusterConfiguration(thread->cluster, config.entry_points,
                                   config.entry_points_count))
    {
        proxyLogErr("ERROR: Failed to fetch cluster configuration!");
        freeProxyThread(thread);
        return NULL;
    }
    if (is_first) {
        printClusterConfiguration(thread->cluster);
        int nodecount = thread->cluster->masters_count +
                        thread->cluster->replicas_count;
        int poolsize = config.connections_pool.size;
        if (poolsize <= 0) poolsize = 1;
        proxy.min_reserved_fds += (nodecount * config.num_threads * poolsize);
        adjustOpenFilesLimit();
    }
    thread->clients = listCreate();
    if (thread->clients == NULL) goto fail;
    thread->unlinked_clients = listCreate();
    if (thread->unlinked_clients == NULL) goto fail;
    thread->pending_messages = listCreate();
    if (thread->pending_messages == NULL) goto fail;
    listSetFreeMethod(thread->pending_messages, zfree);
    int loopsize = proxy.min_reserved_fds + config.max_clients;
    thread->loop = aeCreateEventLoop(loopsize);
    if (thread->loop == NULL) {
        proxyLogErr("Failed to allocate event loop for thread %d", index);
        goto fail;
    }
    thread->loop->privdata = thread;
    aeSetBeforeSleepProc(thread->loop, beforeThreadSleep);
    if (!installIOHandler(thread->loop, thread->io[THREAD_IO_READ],
                          AE_READABLE, readThreadPipe, thread, 0))
    {
        proxyLogErr("Failed to install thread pipe read handler for "
                    "thread %d", index);
        if (errno > 0) proxyLogErr("Thread pipe error: %s", strerror(errno));
        goto fail;
    }
    thread->msgbuffer = sdsempty();
    if (config.connections_pool.size > 0) populateConnectionsPool(thread, 0);
    return thread;
fail:
    if (thread) freeProxyThread(thread);
    return NULL;
}

static void handlePendingAwakeMessages(aeEventLoop *el, int fd, void *privdata,
                                       int mask)
{
    UNUSED(el);
    UNUSED(fd);
    UNUSED(mask);
    proxyThread *thread = privdata;
    listIter li;
    listNode *ln;
    listRewind(thread->pending_messages, &li);
    while ((ln = listNext(&li))) {
        sds msg = ln->value;
        int sent = sendMessageToThread(thread, msg);
        if (sent == -1) continue;
        else {
            listDelNode(thread->pending_messages, ln);
            if (!sent) {
                proxyLogErr("Failed to send message to thread %d",
                            thread->thread_id);
            }
        }
    }
}

static int sendMessageToThread(proxyThread *thread, sds buf) {
    int fd = thread->io[THREAD_IO_WRITE];
    int nwritten = 0, totwritten = 0, buflen = sdslen(buf);
    if (buflen == 0) return 0;
    while (totwritten < buflen) {
        nwritten = write(fd, buf + nwritten, sdslen(buf));
        if (nwritten == -1) {
            if (errno == EAGAIN) {
                goto install_write_handler;
            }
            sdsfree(buf);
            return 0;
        } else if (nwritten == 0) break;
        totwritten += nwritten;
    }
    if (totwritten == buflen) {
        sdsfree(buf);
        aeDeleteFileEvent(thread->loop, fd, AE_WRITABLE);
    } else goto install_write_handler;
    return 1;
install_write_handler:
    sdsrange(buf, totwritten, -1);
    listAddNodeTail(thread->pending_messages, buf);
    if (!installIOHandler(thread->loop, fd, AE_WRITABLE,
        handlePendingAwakeMessages, thread, 0))
    {
        proxyLogDebug("Failed to create thread awake write handler on "
                      "thread %d", thread->thread_id);
        listNode *ln = listSearchKey(thread->pending_messages, buf);
        if (ln != NULL) listDelNode(thread->pending_messages, ln);
        sdsfree(buf);
        return 0;
    }
    return -1;
}

static int awakeThreadForNewClient(proxyThread *thread, client *c) {
    sds buf = sdsnewlen(&c, sizeof(c));
    return sendMessageToThread(thread, buf);
}

int sendStopMessageToThread(proxyThread *thread) {
    proxyLogDebug("Sending stop message to thread %d", thread->thread_id);
    sds buf = sdsempty();
    buf = sdsMakeRoomFor(buf, sizeof(void*));
    int msg = THREAD_MSG_STOP;
    memset(buf, 0, sizeof(void*));
    memcpy(buf, &msg, sizeof(msg));
    sdsIncrLen(buf, sizeof(void*));
    return sendMessageToThread(thread, buf);
}

static void freeProxyThread(proxyThread *thread) {
    proxyLogDebug("Freeing thread %d", thread->thread_id);
    if (thread->loop != NULL) {
        aeDeleteEventLoop(thread->loop);
        thread->loop = NULL;
    }
    listIter li;
    listNode *ln;
    if (thread->clients != NULL) {
        listRewind(thread->clients, &li);
        while ((ln = listNext(&li)) != NULL) {
            client *c = ln->value;
            c->requests_with_write_handler = 0;
            freeClient(c);
        }
    }
    if (thread->unlinked_clients != NULL) {
        listRewind(thread->unlinked_clients, &li);
        while ((ln = listNext(&li)) != NULL) {
            client *c = ln->value;
            if (c != NULL) {
                c->requests_with_write_handler = 0;
                freeClient(c);
            }
        }
    }
    if (thread->pending_messages != NULL) {
        /* Check if there are still clients that have to be added and
         * free them. */
        processThreadPipeBufferForNewClients(thread);
        listRelease(thread->pending_messages);
        thread->pending_messages = NULL;
    }
    if (thread->clients) {
        listRelease(thread->clients);
        thread->clients = NULL;
    }
    if (thread->unlinked_clients) {
        listRelease(thread->unlinked_clients);
        thread->unlinked_clients = NULL;
    }
    if (thread->io[0]) close(thread->io[0]);
    if (thread->io[1]) close(thread->io[1]);
    if (thread->msgbuffer) sdsfree(thread->msgbuffer);
    if (thread->cluster != NULL) freeCluster(thread->cluster);
    if (thread->connections_pool != NULL) {
        listRewind(thread->connections_pool, &li);
        while ((ln = listNext(&li)) != NULL) {
            rax *connections = ln->value;
            raxFreeWithCallback(connections,
                (void (*)(void *)) freeClusterConnection);
        }
        listRelease(thread->connections_pool);
    }
    proxy.threads[thread->thread_id] = NULL;
    zfree(thread);
}

int selectThreadWithLessClients(void) {
    int i, thread_id = 0;
    for (i = 1; i < config.num_threads; i++) {
        if (proxy.threads[i]->process_clients <
            proxy.threads[thread_id]->process_clients)
        {
            thread_id = i;
        }
    }
    return thread_id;
}

static client *createClient(int fd, char *ip) {
    client *c = zcalloc(sizeof(*c));
    if (c == NULL) {
        proxyLogErr("Failed to allocate memory for client: %s", ip);
        close(fd);
        return NULL;
    }
    c->requests = listCreate();
    if (c->requests == NULL) {
        freeClient(c);
        return NULL;
    }
    c->unordered_replies = raxNew();
    if (c->unordered_replies == NULL) {
        freeClient(c);
        return NULL;
    }
    c->status = CLIENT_STATUS_NONE;
    c->flags = 0;
    c->fd = fd;
    c->ip = ip ? sdsnew(ip) : NULL;
    c->port = 0;
    c->addr = NULL;
    c->obuf = sdsempty();
    c->reply_array = NULL;
    c->current_request = NULL;
    c->cluster = NULL;
    anetNonBlock(NULL, fd);
    anetEnableTcpNoDelay(NULL, fd);
    if (config.tcpkeepalive)
        anetKeepAlive(NULL, fd, config.tcpkeepalive);
    /* Select thread with less clients */
    c->thread_id = selectThreadWithLessClients();
    proxy.numclients++;
    proxy.threads[c->thread_id]->process_clients++;
    c->id = proxy.threads[c->thread_id]->next_client_id++;
    if (proxy.threads[c->thread_id]->next_client_id == UINT64_MAX)
        proxy.threads[c->thread_id]->next_client_id = 0;
    c->next_request_id = 0;
    c->min_reply_id = 0;
    c->requests_with_write_handler = 0;
    c->requests_to_reprocess = listCreate();
    c->pending_multiplex_requests = 0;
    c->multi_transaction = 0;
    c->multi_request = NULL;
    c->multi_transaction_node = NULL;
    c->auth_user = NULL;
    c->auth_passw = NULL;
    c->clients_lnode = NULL;
    c->unlinked_clients_lnode = NULL;
    proxyLogDebug("Created client %d:%" PRId64 " with address %p",
        c->thread_id, c->id, (void *)c);
    if (config.disable_multiplexing == CFG_DISABLE_MULTIPLEXING_ALWAYS) {
        if (!disableMultiplexingForClient(c)) {
            unlinkClient(c);
            return NULL;
        }
    }
    return c;
}

/* Disable multiplexing on the specified client by creating a private
 * slots map (radix tree) on the client itself. The slots map will be
 * a duplicate of the shared map (proxy.cluster->slots_map), and every
 * node in the map will be a duplicate of the shared map's corresponding
 * node, but the duplicated node will have the cluster member set to
 * NULL, so that it will have just one connection (redisClusterConnection *).
 * This connection will give client a private socket to the node and private
 * request queues. */
static int disableMultiplexingForClient(client *c) {
    if (c->cluster != NULL) return 1;
    proxyLogDebug("Disabling multiplexing for client %d:%" PRId64,
                  c->thread_id, c->id);
    proxyThread *thread = proxy.threads[c->thread_id];
    c->cluster = duplicateCluster(thread->cluster);
    if (c->cluster == NULL) return 0;
    rax *pool_connections = NULL;
    /* Try to fetch an already-connected redisClusterConnection from thread's
     * connections pool, only if client does not require a different
     * authentication */
    if (!clientRequiresAuth(c) && thread->connections_pool != NULL) {
        listNode *cln = listFirst(thread->connections_pool);
        if (cln != NULL) {
            pool_connections = cln->value;
            listDelNode(thread->connections_pool, cln);
            proxyLogDebug("Got connections from thread's connections pool, "
                          "%lu remaining", listLength(thread->connections_pool));
            /* When the size of the thread's connections pool is below the
             * configured minimum, create a time event to re-populate it. */
            int min_size = config.connections_pool.min_size;
            if (!thread->is_spawning_connections &&
                (int) listLength(thread->connections_pool) < min_size)
            {
                thread->is_spawning_connections = 1;
                aeCreateTimeEvent(thread->loop,
                    (long long) config.connections_pool.spawn_every,
                    threadConnectionPoolCron, NULL,NULL);
            }
        }
    }
    c->cluster->owner = c;
    listIter li;
    listNode *ln;
    listRewind(c->cluster->nodes, &li);
    while ((ln = listNext(&li))) {
        clusterNode *node = (clusterNode *) ln->value;
        clusterNode *source = node->duplicated_from;
        assert(source != NULL);
        redisClusterConnection *conn = source->connection;
        if (pool_connections != NULL) {
            redisClusterConnection *poolconn = NULL;
            /* Try to find and remove the connection associated to the name
             * of the node. */
            if (!raxRemove(pool_connections, (unsigned char*) source->name,
                 sdslen(source->name), (void **) &poolconn)) poolconn = NULL;
            if (poolconn && (!poolconn->connected || !poolconn->context)) {
                freeClusterConnection(poolconn);
                poolconn = NULL;
            }
            if (poolconn != NULL) {
                /* Associate the connection taken from pool to the new node. */
                proxyLogDebug("Assigning connection from pool for "
                              "node %s:%d to private connection owned by "
                              "client %d:%" PRId64, node->ip, node->port,
                              c->thread_id, c->id);
                freeClusterConnection(node->connection);
                node->connection = poolconn;
                poolconn->node = node;
            }
        }

        /* Move requests from shared connection to private connection. */
        listIter rli;
        listNode *rln;
        listRewind(conn->requests_to_send, &rli);
        while ((rln = listNext(&rli))) {
            clientRequest *req = rln->value;
            if (req->client != c) continue;
            if (req->has_write_handler) {
                c->pending_multiplex_requests++;
                continue;
            }
            /* Replace request node with duplicated node owned by the client */
            req->node = node;
            int *p_ok = NULL;
            addObjectToList(req, node->connection, requests_to_send, p_ok);
            listDelNode(conn->requests_to_send, ln);
            req->owned_by_client = 1;
        }
        if (c->pending_multiplex_requests == 0) {
            listRewind(conn->requests_pending, &rli);
            while ((rln = listNext(&rli))) {
                clientRequest *req = rln->value;
                if (req == NULL) continue;
                if (req->client == c) c->pending_multiplex_requests++;
            }
        }
        int count = listLength(node->connection->requests_to_send);
        if (count > 0) {
            proxyLogDebug("Moved %d request(s) to private connection to %s:%d "
                          "owned by client %d:%" PRId64,
                          count,
                          node->ip, node->port,
                          c->thread_id, c->id);
        }
        if (c->pending_multiplex_requests == 0)
            handleNextRequestsToCluster(node, NULL);
        else {
            proxyLogDebug("Client %d:%" PRId64 " has %d pending requests to "
                          " node %s:%d on the multiplexing context",
                          c->thread_id, c->id, c->pending_multiplex_requests,
                          node->ip, node->port);
        }
    }
    if (pool_connections != NULL) {
        raxFreeWithCallback(pool_connections,
            (void (*)(void*)) freeClusterConnection);
    }
    return 1;
}

static void closeClientPrivateConnection(client *c) {
    if (c->cluster == NULL) return;
    aeEventLoop *el = getClientLoop(c);
    listIter li;
    listNode *ln;
    listRewind(c->cluster->nodes, &li);
    while ((ln = listNext(&li))) {
        clusterNode *node = listNodeValue(ln);
        if (ln == NULL) continue;
        redisClusterConnection *conn = node->connection;
        if (conn == NULL) continue;
        if (conn->requests_to_send != NULL) {
            listIter nli;
            listNode *nln;
            listRewind(conn->requests_to_send, &nli);
            while ((nln = listNext(&nli))) {
                clientRequest *req = listNodeValue(nln);
                if (req == NULL) continue;
                if (req->has_write_handler) req->has_write_handler = 0;
                dequeueRequestToSend(req);
            }
            listRewind(conn->requests_pending, &nli);
            while ((nln = listNext(&nli))) {
                clientRequest *req = listNodeValue(nln);
                if (req == NULL) continue;
                dequeuePendingRequest(req);
            }
        }
        redisContext *ctx = conn->context;
        if (ctx == NULL) continue;
        if (ctx->fd >= 0 && el) {
            aeDeleteFileEvent(el, ctx->fd, AE_READABLE);
            aeDeleteFileEvent(el, ctx->fd, AE_WRITABLE);
        }
        conn->has_read_handler = 0;
        redisFree(ctx);
        conn->context = NULL;
    }
}

static void unlinkClient(client *c) {
    if (c->status == CLIENT_STATUS_UNLINKED) return;
    proxyLogDebug("Unlink client %d:%" PRId64, c->thread_id, c->id);
    aeEventLoop *el = getClientLoop(c);
    if (c->fd >= 0) {
        if (el != NULL) {
            aeDeleteFileEvent(el, c->fd, AE_READABLE);
            aeDeleteFileEvent(el, c->fd, AE_WRITABLE);
        }
        close(c->fd);
        c->fd = -1;
        proxy.numclients--;
        getThread(c)->process_clients--;
    }
    if (c->cluster != NULL) {
        if (recyclePrivateClusterConnection(c)) {
            proxyLogDebug("Recycled private connection from client %d:%" PRId64
                ", pool's size: %lu",
                c->thread_id, c->id,
                listLength(getThread(c)->connections_pool));
        } else closeClientPrivateConnection(c);
    }
    proxyThread *thread = getThread(c);
    int *p_ok = NULL;
    addObjectToList(c, thread, unlinked_clients, p_ok);
    removeObjectFromList(c, thread, clients);
    c->status = CLIENT_STATUS_UNLINKED;
}

static void freeAllClientRequests(client *c) {
    aeEventLoop *el = getClientLoop(c);
    int unlinked = (c->status == CLIENT_STATUS_UNLINKED);
    listIter li;
    listNode *ln;
    listRewind(c->requests, &li);
    while ((ln = listNext(&li))) {
        clientRequest *req = ln->value;
        if (req == NULL) {
            listDelNode(c->requests, ln);
            continue;
        }
        if (req->client != c) continue;
        if (el && unlinked && req->has_write_handler && req->written > 0) {
            /* If the request is still being written to a shared (multiplex)
             * connection to the client,  and the client disconnected,
             * defer freeing it since we want to wait for it to finish
             * writing in order to avoid messing up the shared connection's
             * query order. Otherwise, delete the write handler and free it
             * as normal. */
            if (req->owned_by_client) {
                redisClusterConnection *conn = getRequestConnection(req);
                if (conn && conn->context && conn->context->fd >= 0)
                    aeDeleteFileEvent(el, conn->context->fd, AE_WRITABLE);
                req->has_write_handler = 0;
            } else continue;
        }
        listDelNode(c->requests, ln);
        req->requests_lnode = NULL;
        freeRequest(req);
    }
}

static void freeClient(client *c) {
    unlinkClient(c);
    /* If the client still has requests handled by write handlers, it's not
     * possibile to free it soon, as those requests would be truncated and
     * they could break all other following requests in a multiplexing
     * context. */
    aeEventLoop *el = getClientLoop(c);
    if (c->requests_with_write_handler > 0 && el != NULL) return;
    proxyLogDebug("Freeing client %d:%" PRId64 " (thread: %d)",
                  c->thread_id, c->id, c->thread_id);
    int thread_id = c->thread_id;
    proxyThread *thread = proxy.threads[thread_id];
    assert(thread != NULL);
    removeObjectFromList(c, thread, clients);
    if (c->ip != NULL) sdsfree(c->ip);
    if (c->addr != NULL) sdsfree(c->addr);
    if (c->obuf != NULL) sdsfree(c->obuf);
    if (c->reply_array != NULL) listRelease(c->reply_array);
    if (c->current_request) freeRequest(c->current_request);
    freeAllClientRequests(c);
    listRelease(c->requests_to_reprocess);
    listRelease(c->requests);
    if (c->unordered_replies)
        raxFreeWithCallback(c->unordered_replies, (void (*)(void*))sdsfree);
    if (c->cluster != NULL) {
        freeCluster(c->cluster);
    }
    listNode *ln = c->unlinked_clients_lnode;
    if (ln) ln->value = NULL;
    if (c->auth_user != NULL) sdsfree(c->auth_user);
    if (c->auth_passw != NULL) sdsfree(c->auth_passw);
    zfree(c);
}

static int writeToClient(client *c) {
    if (c->status == CLIENT_STATUS_UNLINKED) return 0;
    int success = 1, buflen = sdslen(c->obuf), nwritten = 0;
    if (buflen == 0) return 1;
    while (c->written < (size_t) buflen) {
        nwritten = write(c->fd, c->obuf + c->written, buflen - c->written);
        if (nwritten <= 0) break;
        c->written += nwritten;
    }
    if (nwritten == -1) {
        if (errno == EAGAIN) {
            nwritten = 0;
        } else {
            proxyLogDebug("Error writing to client: %s", strerror(errno));
            unlinkClient(c);
            return 0;
        }
    }
    /* The whole buffer has been written, so reset everything. */
    if (c->written == (size_t) buflen) {
        sdsclear(c->obuf);
        c->written = 0;
        if (c->has_write_handler) {
            proxyThread *thread = proxy.threads[c->thread_id];
            assert(thread != NULL);
            aeEventLoop *el = thread->loop;
            assert(el != NULL);
            aeDeleteFileEvent(el, c->fd, AE_WRITABLE);
            c->has_write_handler = 0;
        }
        if (c->flags & CLIENT_CLOSE_AFTER_REPLY) unlinkClient(c);
    }
    return success;
}

static void writeToClusterHandler(aeEventLoop *el, int fd, void *privdata,
                                  int mask)
{
    UNUSED(mask);
    redisClusterConnection *connection = privdata;
    if (connection == NULL) return;
    clusterNode *node = connection->node;
    redisContext *ctx = connection->context;
    if (ctx == NULL) return;
    clientRequest *req = NULL;
    proxyThread *thread = el->privdata;
    int thread_id = thread->thread_id;
    char *ip = ctx->tcp.host;
    int port = ctx->tcp.port;
    if (node != NULL) req = getFirstRequestToSend(node, NULL);
    if (req != NULL && req->owned_by_client &&
        req->client->status == CLIENT_STATUS_UNLINKED)
    {
        /* If client disconnected and the request's target is on a private
         * connection owned bu the client itself, uninstall the write handler,
         * free the request and stop proceeding. */
        if (req->has_write_handler) req->has_write_handler = 0;
        aeDeleteFileEvent(el, fd, AE_WRITABLE);
        dequeueRequestToSend(req);
        freeRequest(req);
        return;
    }
    if (connection->authenticating) {
        if (redisBufferWrite(ctx, NULL) == REDIS_ERR) {
            if (req != NULL) {
                addReplyError(req->client, "AUTH failed", req->id);
                freeRequest(req);
            }
        }
        return;
    }
    if (ctx->err) {
        proxyLogErr("Failed to connect to node %s:%d", ip, port);
        if (req != NULL) {
            sds err = sdsnew(ERROR_NODE_DISCONNECTED);
            err = sdscatprintf(err, "%s:%d", ip, port);
            addReplyError(req->client, err, req->id);
            sdsfree(err);
            freeRequest(req);
        }
        return;
    }
    if (!connection->has_read_handler) {
        if (!installIOHandler(el, ctx->fd, AE_READABLE, readClusterReply,
                              connection, 0))
        {
            proxyLogErr("Failed to create read reply handler for node %s:%d",
                        ip, port);
            if (req != NULL) {
                addReplyError(req->client, ERROR_CLUSTER_READ_FAIL,
                              req->id);
                freeRequest(req);
            }
            return;
        } else  {
            connection->has_read_handler = 1;
            if (node != NULL) {
                proxyLogDebug("Read reply handler installed "
                              "for node %s:%d", ip, port);
            }
        }
    }
    if (config.loglevel == LOGLEVEL_DEBUG && !connection->connected) {
        if (node != NULL && node->cluster->owner) {
            client *c = node->cluster->owner;
            proxyLogDebug("Connected to node %s:%d (private connection "
                          "for client %d:%" PRId64,
                          ip, port, thread_id, c->id);
        } else if (node != NULL) {
            proxyLogDebug("Connected to node %s:%d (thread %d)",
                          ip, port, thread_id);
        }
    }
    connection->connected = 1;
    /* Try to automatically authenticate if config.auth has been set.
     * It the connection is private (no multiplexing), check if the client
     * tried to authenticate with different credentials from the ones
     * used by the proxy and, in this case, skip the automatic authentication
     * since the client will send its AUTH query by itself. */
    char *auth = config.auth;
    client *c = NULL;
    if (node && (c = node->cluster->owner) && (c->auth_user || c->auth_passw)) {
        if (clientRequiresAuth(c)) auth = NULL;
    }
    if (auth && !connection->authenticated && !connection->authenticating) {
        char *autherr = NULL;
        clusterNode tmpnode = {0};
        if (node == NULL) {
            /* Connections in the thread's connection pool have no node,
             * so create a temporary node to be used with clusterNodeAuth. */
            tmpnode.connection = connection;
            tmpnode.ip = ip;
            tmpnode.port = port;
            node = &tmpnode;
        }
        if (!clusterNodeAuth(node, config.auth, config.auth_user, &autherr)) {
            if (autherr) {
                proxyLogDebug("AUTH: %s", autherr);
                if (req) {
                    addReplyError(req->client, autherr, req->id);
                    freeRequest(req);
                }
                zfree(autherr);
            }
            connection->authenticating = 1;
            return;
        }
    }
    /* Delete the file handlers from the event loop if it's a connection in
     * the thread's connections pool (node == NULL). */
    if (node == NULL) {
        aeDeleteFileEvent(el, ctx->fd, AE_WRITABLE | AE_READABLE);
        connection->has_read_handler = 0;
        return;
    }
    if (req == NULL) return;
    /* Ensure that all there's no more pending_multiplex_requests left in case
     * of private (non-multiplex) connection (c->cluster != NULL). */
    c = req->client;
    if (c->cluster != NULL && req->owned_by_client &&
        c->pending_multiplex_requests > 0) return;
    int ok = writeToCluster(el, fd, req);
    /* Try to send next requests enqueued to the same cluster node if
     * the request failed or if the request have been completely written. */
    if (ok && req->has_write_handler) return;
    handleNextRequestsToCluster(node, NULL);
}


static int writeToCluster(aeEventLoop *el, int fd, clientRequest *req) {
    /* If client disconnected and the request's target is on a private
     * connection owned bu the client itself, uninstall the write handler,
     * free the request and stop proceeding. */
    if (req->client->status == CLIENT_STATUS_UNLINKED && req->owned_by_client) {
        if (req->has_write_handler) req->has_write_handler = 0;
        aeDeleteFileEvent(el, fd, AE_WRITABLE);
        dequeueRequestToSend(req);
        freeRequest(req);
        return 0;
    }
    size_t buflen = sdslen(req->buffer);
    int nwritten = 0;
    while (req->written < buflen) {
        nwritten = write(fd, req->buffer + req->written, buflen - req->written);
        if (nwritten <= 0) break;
        req->written += nwritten;
    }
    if (nwritten == -1) {
        if (errno == EAGAIN) {
            nwritten = 0;
        } else {
            proxyLogWarn("Error writing request " REQID_PRINTF_FMT
                "to cluster: %s", REQID_PRINTF_ARG(req), strerror(errno));
            if (errno == EPIPE) {
                clusterNodeDisconnect(req->node);
            } else {
                addReplyError(req->client, ERROR_CLUSTER_WRITE_FAIL, req->id);
                freeRequest(req);
            }
            return 0;
        }
    }
    int success = 1;
    if (req->written == buflen) {
        /* The whole query has been written, so install the read handler and
         * move the request from requests_to_send to requests_pending. */
        client *c = req->client;
        clusterNode *node = req->node;
        int thread_id = c->thread_id;
        proxyLogDebug("Request " REQID_PRINTF_FMT " written to node %s:%d, "
                      "adding it to pending requests",
                      REQID_PRINTF_ARG(req),
                      node->ip, node->port);
        aeDeleteFileEvent(el, fd, AE_WRITABLE);
        if (req->has_write_handler) {
            req->has_write_handler = 0;
            /* Only count requests_with_write_handler on shared connection. */
            if (!req->owned_by_client) c->requests_with_write_handler--;
        }
        dequeueRequestToSend(req);
        if (c->status == CLIENT_STATUS_UNLINKED) {
            /* Client has been disconnected, so we'll enqueue a NULL pointer
             * (a 'ghost rquest') to pending requests, so that the reply will
             * be just skipped during reply buffer processing. Without using
             * this NULL placeholder, the reply buffer processing order would
             * be broken. After enqueuing the ghost request, we can finally
             * free and the request itself and try to free the client
             * completely. */
            int owned_by_client = req->owned_by_client;
            list *pending_queue =
                node->connection->requests_pending;
            listAddNodeTail(pending_queue, NULL);
            req->requests_pending_lnode = NULL;
            freeRequest(req);
            if (owned_by_client) return 0;
            else success = 0;
        } else if (!enqueuePendingRequest(req)) {
            proxyLogDebug("Could not enqueue pending request "
                          REQID_PRINTF_FMT,
                          REQID_PRINTF_ARG(req));
            addReplyError(req->client, "Could not enqueue request", req->id);
            freeRequest(req);
            return 0;
        }
        if (config.dump_queues) dumpQueue(node, thread_id, QUEUE_TYPE_PENDING);
        if (!node->cluster->owner) {
            proxyLogDebug("Still have %lu request(s) to send to node %s:%d "
                          "on thread %d",
                          listLength(node->connection->requests_to_send),
                          node->ip, node->port, node->cluster->thread_id);
        } else if (node->cluster->owner == c) {
            proxyLogDebug("Still have %lu request(s) to send to node %s:%d "
                          "on private connection owned by %d:%" PRId64,
                          listLength(node->connection->requests_to_send),
                          node->ip, node->port, c->thread_id, c->id);
        }
    } else  {
        /* Request has not been completely written, so try to install the write
         * handler. */
        if (!installIOHandler(el, fd, AE_WRITABLE, writeToClusterHandler,
            req->node, 0))
        {
            addReplyError(req->client, ERROR_CLUSTER_WRITE_FAIL, req->id);
            proxyLogErr("Failed to create write handler for request "
                REQID_PRINTF_FMT, REQID_PRINTF_ARG(req));
            freeRequest(req);
            return 0;
        }
        req->has_write_handler = 1;
        /* Only count requests_with_write_handler on shared connection. */
        if (!req->owned_by_client) req->client->requests_with_write_handler++;
        proxyLogDebug("Write handler installed into request " REQID_PRINTF_FMT
                      " for node %s:%d", REQID_PRINTF_ARG(req),
                      req->node->ip, req->node->port);
    }
    return success;
}

/* This should be called every time a node connection is closed (ie. because
 * the connection has been closed by the proxy itself or because the node
 * instance went down.
 * This functions does the following:
 *   - Delete event loop's file events related to the node's socket
 *   - Check for requests that were still writing to the node's socket and
 *     dequeue and free them (after repying with an error to their client).
 *   - Check for requests that were still waiting to read replies from the
 *     node's socket, dequeue and free them (after repying with an error to
 *     their client). */
void onClusterNodeDisconnection(clusterNode *node) {
    redisClusterConnection *connection = node->connection;
    if (connection == NULL) return;
    redisContext *ctx = connection->context;
    if (ctx != NULL && ctx->fd >= 0) {
        int thread_id = node->cluster->thread_id;
        aeEventLoop *el = NULL;
        proxyThread *thread = proxy.threads[thread_id];
        if (thread != NULL && (el = thread->loop))
            aeDeleteFileEvent(el, ctx->fd, AE_WRITABLE | AE_READABLE);
        connection->has_read_handler = 0;
        redisCluster *cluster = node->cluster;
        assert(cluster != NULL);
        if (cluster->is_updating) return;
        sds err = sdsnew(ERROR_NODE_DISCONNECTED);
        err = sdscatprintf(err, "%s:%d", node->ip, node->port);
        listIter li;
        listNode *ln;
        /* If there are requests currently writing to the node, we must reply
         * to their client with a "node disconnected" error, free them and then
         * dequeue them. */
        listRewind(connection->requests_to_send, &li);
        while ((ln = listNext(&li))) {
            clientRequest *req = ln->value;
            if (req == NULL) continue;
            assert(req->node == node);
            /* If the request has a write handler installed, it means that
             * it could not have completely written its buffer to the node.
             * In this case, the client should receive the reply error and
             * the request itself should be dequeued and freed. */
            if (req->has_write_handler) {
                req->has_write_handler = 0;
                if (!req->owned_by_client)
                    req->client->requests_with_write_handler--;
                addReplyError(req->client, err, req->id);
                dequeueRequestToSend(req);
                freeRequest(req);
            }
        }
        /* If there are pending requests that are reading or waiting to read
         * from the node, we must reply to their client with a
         * "node disconnected" error, free them and dequeue them. */
        listRewind(connection->requests_pending, &li);
        while ((ln = listNext(&li))) {
            clientRequest *req = ln->value;
            if (req == NULL) continue;
            assert(req->node == node);
            addReplyError(req->client, err, req->id);
            dequeuePendingRequest(req);
            freeRequest(req);
        }
        sdsfree(err);
    }
}

static int listen(void) {
    int fd_idx = 0;
    /* Try to use both IPv6 and IPv4 */
    if (config.port != 0) {
        if (config.bindaddr_count == 0) {
            proxy.fds[fd_idx] = anetTcp6Server(proxy.neterr, config.port, NULL,
                                               proxy.tcp_backlog);
            if (proxy.fds[fd_idx] != ANET_ERR)
                anetNonBlock(NULL, proxy.fds[fd_idx++]);
            else if (errno == EAFNOSUPPORT)
                proxyLogWarn("Not listening to IPv6: unsupported");

            proxy.fds[fd_idx] = anetTcpServer(proxy.neterr, config.port, NULL,
                                              proxy.tcp_backlog);
            if (proxy.fds[fd_idx] != ANET_ERR)
                anetNonBlock(NULL, proxy.fds[fd_idx++]);
            else if (errno == EAFNOSUPPORT)
                proxyLogWarn("Not listening to IPv4: unsupported");
            if (fd_idx > 0)
                proxyLogInfo("Listening on *:%d", config.port);
            else {
                proxyLogErr("Failed to listen on *:%d", config.port);
                if (strlen(proxy.neterr))
                    proxyLogErr("%s", proxy.neterr);
            }
        } else {
            int i;
            for (i = 0; i < config.bindaddr_count; i++) {
                char *bindaddr = config.bindaddr[i];
                if (strchr(bindaddr, ':')) {
                    /* IPv6 address */
                    proxy.fds[fd_idx] = anetTcp6Server(proxy.neterr,
                        config.port, bindaddr, proxy.tcp_backlog);
                } else {
                    /* IPv4 address */
                    proxy.fds[fd_idx] = anetTcpServer(proxy.neterr,
                        config.port, bindaddr, proxy.tcp_backlog);
                }
                if (proxy.fds[fd_idx] != ANET_ERR) {
                    anetNonBlock(NULL, proxy.fds[fd_idx++]);
                    proxyLogInfo("Listening on %s:%d", bindaddr, config.port);
                } else if (errno == EAFNOSUPPORT)
                    proxyLogWarn("Not listening to %s: unsupported",
                                strchr(bindaddr, ':') ? "IPv6" : "IPv4");
                if (proxy.fds[fd_idx] == ANET_ERR) {
                    proxyLogErr("Failed to listen on %s:%d", bindaddr,
                        config.port);
                    if (strlen(proxy.neterr))
                        proxyLogErr("%s", proxy.neterr);
                }
            }
        }
    }
    /* UNIX socket listener */
    if (config.unixsocket != NULL) {
        unlink(config.unixsocket); /* Don't care if this fails */
        proxy.unixsocket_fd = anetUnixServer(proxy.neterr, config.unixsocket,
                                config.unixsocketperm, config.tcp_backlog);
        if (proxy.unixsocket_fd != ANET_ERR) {
            anetNonBlock(NULL, proxy.unixsocket_fd);
            proxy.fds[fd_idx++] = proxy.unixsocket_fd;
            proxyLogInfo("Listening on Unix socket '%s'", config.unixsocket);
        } else {
            proxyLogErr("Error opening Unix socket: '%s'", proxy.neterr);
        }
    }
    proxy.fd_count = fd_idx;
    return fd_idx;
}

static int requestMakeRoomForArgs(clientRequest *req, int argc) {
    if (argc >= req->offsets_size) {
        int new_size = argc + QUERY_OFFSETS_MIN_SIZE;
        size_t sz = new_size * sizeof(int);
        req->offsets = zrealloc(req->offsets, sz);
        req->lengths = zrealloc(req->lengths, sz);
        if (req->offsets == NULL || req->lengths == NULL) {
            proxyLogErr("Failed to reallocate request "
                        "offsets");
            return 0;
        }
        req->offsets_size = new_size;
    }
    return 1;
}

static int splitPipelinedQueries(clientRequest *req, char *p, int is_inline) {
    /* Multiple commands (queries) from a pipelined request.
     * Split current requestinto multiple requests. */
    proxyLogDebug("Multiple %squeries (pipelined) %d, "
                  "splitting request " REQID_PRINTF_FMT "...",
                  (is_inline ? "inline " : ""),
                  req->num_commands,
                  REQID_PRINTF_ARG(req));
    int buflen = sdslen(req->buffer);
    client *c = req->client;
    /* Truncate current request buffer */
    req->query_offset = p - req->buffer;
    sds newbuf = sdsnewlen(p, buflen - req->query_offset);
    sds reqbuf = sdsnewlen(req->buffer, req->query_offset);
    sdsfree(req->buffer);
    req->buffer = reqbuf;
    req->num_commands = 1;
    req->pending_bulks = 0;
    /* Create a new request with the remaining buffer */
    clientRequest *new = createRequest(c);
    new->buffer = sdscat(new->buffer, newbuf);
    sdsfree(newbuf);
    /* Set the new request as current in order to accept
     * new bytes read from client. */
    c->current_request = new;
    buflen = req->query_offset;
    return buflen;
}

static int parseRequest(clientRequest *req, sds *err) {
    int status = req->parsing_status, lf_len = 2, len, i, real_offset = 0;
    proxyLogDebug("Parsing request " REQID_PRINTF_FMT ", status: %d",
                  REQID_PRINTF_ARG(req), status);
    if (status != PARSE_STATUS_INCOMPLETE) return status;
    int buflen = sdslen(req->buffer);
    if (config.dump_buffer) {
        sds repr = sdscatrepr(sdsempty(), req->buffer, buflen);
        proxyLogDebug("Request " REQID_PRINTF_FMT " buffer:\n%s",
                      REQID_PRINTF_ARG(req), repr);
        sdsfree(repr);
    }
    char *p = req->buffer + req->query_offset, *nl = NULL;
    sds line = NULL;
    /* Ensure that parsing always start from a new line. */
    if (p > req->buffer) {
        if (*p == '\r') {
            p++;
            req->query_offset++;
        }
        if (req->query_offset < buflen && *p == '\n' && *(p - 1) == '\r') {
            p++;
            req->query_offset++;
        }
        if (req->query_offset >= buflen) {
            status = PARSE_STATUS_INCOMPLETE;
            goto cleanup;
        }
    }
    /* New request, so request type must be determinded. */
    if (req->is_multibulk == REQ_STATUS_UNKNOWN) {
        if (*p == '*') req->is_multibulk = 1;
        else req->is_multibulk = 0;
    }
    if (req->is_multibulk) {
        while (req->query_offset < buflen) {
            int parsing_bulks = (
                req->pending_bulks != REQ_STATUS_UNKNOWN &&
                req->current_bulk_length != REQ_STATUS_UNKNOWN
            );
            if (*p == '*' && !parsing_bulks) {
                if (req->num_commands > 0) {
                    /* Pipelined queries */
                    buflen = splitPipelinedQueries(req, p, 0);
                    break;
                } else {
                    /* New query */
                    req->num_commands++;
                    req->query_offset++;
                    p++;
                    req->pending_bulks = REQ_STATUS_UNKNOWN;
                    req->current_bulk_length = REQ_STATUS_UNKNOWN;
                }
            }
            if (req->query_offset >= buflen) {
                status = PARSE_STATUS_INCOMPLETE;
                goto cleanup;
            }
            long long lc = req->pending_bulks;
            /* If pending bulks count is still unkownn, try to determine
             * it by reading the number after the '*' char. */
            if (lc == REQ_STATUS_UNKNOWN) {
                nl = strchr(p, '\r');
                if (nl == NULL) {
                    if (((req->buffer+buflen) - p) > PROTO_INLINE_MAX_SIZE) {
                        if (err) {
                            *err = sdsnew("Protocol error: too big bulk count "
                                "string");
                        }
                        status = PARSE_STATUS_ERROR;
                    } else status = PARSE_STATUS_INCOMPLETE;
                    goto cleanup;
                }
                int len = nl - p;
                if (line != NULL) sdsfree(line);
                line = sdsnewlen(p, len);
                char *errptr = NULL;
                lc = strtoll(line, &errptr, 10);
                if (lc <= 0 || (errptr && errptr < (line + len))) {
                    if (err) {
                        *err = sdsnew("Protocol error: invalid multibulk "
                                      "length");
                        status = PARSE_STATUS_ERROR;
                        goto cleanup;
                    }
                }
                req->query_offset += (len + 2);
                req->pending_bulks = lc;
                if (req->query_offset >= buflen) {
                    status = PARSE_STATUS_INCOMPLETE;
                    goto cleanup;
                }
                p = req->buffer + req->query_offset;
            }
            /* Try to parse bulks */
            for (i = 0; i < lc; i++) {
                if (req->query_offset >= buflen) {
                    status = PARSE_STATUS_INCOMPLETE;
                    goto cleanup;
                }
                long long arglen = req->current_bulk_length;
                /* Try to determine the bulk length by parsing the number
                 * after the '$' char. */
                if (arglen == REQ_STATUS_UNKNOWN) {
                    if (*p != '$') {
                        proxyLogErr("Failed to parse multibulk query for "
                                    "request " REQID_PRINTF_FMT  ": '$' not "
                                    "found!", REQID_PRINTF_ARG(req));
                        if (err) {
                            *err = sdscatprintf(sdsempty(),
                                "Protocol error: expected '$', got '%c'", *p);
                        }
                        status = PARSE_STATUS_ERROR;
                        goto cleanup;
                    }
                    /* Search the '\r' at the end of the bulk line and
                     * advance the pointer from 1 position from '$'. */
                    nl = strchr(++p, '\r');
                    if (nl == NULL) {
                        status = PARSE_STATUS_INCOMPLETE;
                        goto cleanup;
                    }
                    len = nl - p;
                    if (line != NULL) sdsfree(line);
                    line = sdsnewlen(p, len);
                    char *errptr = NULL;
                    arglen = strtoll(line, &errptr, 10);
                    if (arglen <= 0 || (errptr && errptr < (line + len))) {
                        if (err) {
                            *err =
                                sdsnew("Protocol error: invalid bulk length");
                        }
                        status = PARSE_STATUS_ERROR;
                        goto cleanup;
                    }
                    req->current_bulk_length = arglen;
                    /* Increment the query offset by the bulk length + 3,
                     * since it still was pointing to '$', so '$' + '\r\n'
                     * is three bytes. */
                    req->query_offset += (len + 3);
                    if (req->query_offset >= buflen) {
                        status = PARSE_STATUS_INCOMPLETE;
                        goto cleanup;
                    }
                    p = req->buffer + req->query_offset;
                }
                /* Try to parse the argument. */
                if (arglen >= 0) {
                    int newargc = req->argc + 1;
                    if (!requestMakeRoomForArgs(req, newargc)) {
                        proxyLogDebug("Failed to allocate args for "
                            "request " REQID_PRINTF_FMT,
                            REQID_PRINTF_ARG(req));
                        status = PARSE_STATUS_ERROR;
                        goto cleanup;
                    }
                    int endarg = req->query_offset + arglen;
                    if (endarg >= buflen) {
                        status = PARSE_STATUS_INCOMPLETE;
                        goto cleanup;
                    } else if (*(req->buffer+endarg) != '\r') {
                        /* The first byte after the argument of the declared
                         * length must be '\r', otherwise the query format
                         * is invalid. */
                        proxyLogDebug("Failed to parse " REQID_PRINTF_FMT
                            ", expected '\\r' at the end of the argument of "
                            "declared length %lld at offset %d",
                            REQID_PRINTF_ARG(req),
                            arglen, req->query_offset);
                        if (err) {
                            *err = sdscatprintf(sdsempty(), "Protocol error: "
                                "expected '\\r' at the end of the bulk, got "
                                "'%c'", *(req->buffer+endarg));
                        }
                        status = PARSE_STATUS_ERROR;
                        goto cleanup;
                    }
                    int idx = req->argc++;
                    /* Set the argument offset and length into the request. */
                    req->offsets[idx] = p - req->buffer;
                    req->lengths[idx] = arglen;
                    if (config.dump_queries) {
                        sds tk = sdsnewlen(p, arglen);
                        proxyLogDebug("Req. " REQID_PRINTF_FMT
                                      " ARGV[%d]: '%s'",
                                      REQID_PRINTF_ARG(req), idx, tk);
                        sdsfree(tk);
                    }
                    req->pending_bulks--;
                    req->current_bulk_length = REQ_STATUS_UNKNOWN;
                    req->query_offset = endarg + 2;
                    p = req->buffer + req->query_offset;
                    if (req->pending_bulks == 0) break;
                }
            }
        }
    } else {
        while (req->query_offset < buflen) {
            if (req->num_commands > 0) {
                /* Pipelined query */
                buflen = splitPipelinedQueries(req, p, 1);
                break;
            }
            nl = strchr(p, '\n');
            if (nl == NULL) {
                status = PARSE_STATUS_INCOMPLETE;
                goto cleanup;
            }
            lf_len = 1;
            if (nl != p && *(nl - 1) == '\r') {
                lf_len++;
                nl--;
            }
            char *qry = p;
            int qrylen = nl - p;
            int remaining = qrylen;
            if (qrylen > PROTO_INLINE_MAX_SIZE) {
                if (err) *err = sdsnew("Protocol error: too big inline string");
                status = PARSE_STATUS_ERROR;
                goto cleanup;
            } else if (qrylen == 0) {
                if (err) *err = sdsnew("Protocol error: empry inline query");
                status = PARSE_STATUS_ERROR;
                goto cleanup;
            }
            while (remaining > 0) {
                int idx = req->argc++;
                if (!requestMakeRoomForArgs(req, idx)) {
                    status = PARSE_STATUS_ERROR;
                    goto cleanup;
                }
                char *sep = strchr(p, ' ');
                if (!sep) sep = nl;
                req->offsets[idx] = p - req->buffer;
                req->lengths[idx] = sep - p;
                p = sep + 1;
                remaining = nl - p;
            }
            status = PARSE_STATUS_OK;
            req->num_commands++;
            p = qry + qrylen + lf_len;
            req->query_offset = (p - req->buffer);
        }
    }
cleanup:
    real_offset = req->query_offset;
    if (req->query_offset > buflen) req->query_offset = buflen;
    int remaining = buflen - req->query_offset;
    if (status == PARSE_STATUS_INCOMPLETE) {
        /* If a multibulk query has no more pending bulks and no more bytes to
         * parse, set the status to PARSE_STATUS_OK since the parsing is
         * complete. Also ensure that the last byte is '\n', in order to
         * consume the whole query's buffer. */
        if (req->is_multibulk && req->pending_bulks == 0 && remaining == 0 &&
            req->buffer[buflen - 1] == '\n')
            status = PARSE_STATUS_OK;
    }
    req->parsing_status = status;
    if (line != NULL) sdsfree(line);
    if (status == PARSE_STATUS_ERROR) {
        proxyLogDebug("Failed to parse request " REQID_PRINTF_FMT,
                      REQID_PRINTF_ARG(req));
        if (err && *err) {
            proxyLogDebug("Request " REQID_PRINTF_FMT " parsing error: '%s'",
                REQID_PRINTF_ARG(req), *err);
        }
        if (!config.dump_buffer && config.loglevel == LOGLEVEL_DEBUG) {
            sds repr = sdscatrepr(sdsempty(), req->buffer, buflen);
            proxyLogDebug("Request " REQID_PRINTF_FMT " buffer:\n%s",
                          REQID_PRINTF_ARG(req), repr);
            sdsfree(repr);
            proxyLogDebug("Request " REQID_PRINTF_FMT
                " offset=%d, real_offset=%d",
                REQID_PRINTF_ARG(req), req->query_offset, real_offset);
        }
    }
    return status;
}

static sds getRequestCommand(clientRequest *req) {
    if (req->argc == 0) return NULL;
    assert(req->buffer != NULL);
    int start = req->offsets[0], len = req->lengths[0],
        buflen = sdslen(req->buffer);
    assert(start < buflen);
    assert((start + len)  < buflen);
    sds cmd = sdsnewlen(req->buffer + start, len);
    sdstolower(cmd);
    return cmd;
}

static int duplicateRequestForAllMasters(clientRequest *req) {
    int ok = 1, i;
    if (req->child_requests == NULL) {
        req->child_requests = listCreate();
        if (req->child_requests == NULL) return 0;
    }
    if (req->child_replies == NULL) {
        req->child_replies = raxNew();
        if (req->child_replies == NULL) return 0;
    }
    listIter li;
    listNode *ln;
    redisCluster *cluster = getCluster(req->client);
    listRewind(cluster->nodes, &li);
    clientRequest *cur = req->client->current_request;
    proxyLogDebug("Duplicating request " REQID_PRINTF_FMT " for all masters",
                  REQID_PRINTF_ARG(req));
    while ((ln = listNext(&li)) != NULL) {
        clusterNode *node = ln->value;
        if (node->is_replica) continue;
        if (req->node == NULL) req->node = node;
        else {
            clientRequest *child = createRequest(req->client);
            ok = (child != NULL);
            if (!ok) break;
            child->parent_request = req;
            child->buffer = sdscat(child->buffer, req->buffer);
            child->argc = req->argc;
            child->offsets_size = req->offsets_size;
            ok = requestMakeRoomForArgs(child, child->argc);
            if (!ok) break;
            for (i = 0; i < req->argc; i++) {
                child->offsets[i] = req->offsets[i];
                child->lengths[i] = req->lengths[i];
            }
            child->node = node;
            child->command = req->command;
            child->parsing_status = req->parsing_status;
            listAddNodeHead(req->child_requests, child);
            if (child->id > req->max_child_reply_id)
                req->max_child_reply_id = child->id;
        }
    }
    req->client->current_request = cur;
    return ok;
}

static int splitMultiSlotRequest(clientRequest *req, int idx) {
    if (idx >= req->offsets_size) return 0;
    if (req->command == NULL) return 0;
    if (!req->is_multibulk) return 0;
    clientRequest *parent = req->parent_request;
    if (parent == NULL) parent = req;
    if (parent->child_requests == NULL) {
        parent->child_requests = listCreate();
        if (parent->child_requests == NULL) return 0;
    }
    if (parent->child_replies == NULL) {
        parent->child_replies = raxNew();
        if (parent->child_replies == NULL) return 0;
    }
    clientRequest *new = NULL;
    int offset = req->offsets[idx], original_argc = req->argc, success = 1,
                 len, llen, diff, i;
    proxyLogDebug("Splitting multi-slot request " REQID_PRINTF_FMT
                  " at index %d (offset: %d)",
                  REQID_PRINTF_ARG(req), idx, offset);
    req->argc = idx;
    /* Keep a pointer to the original buffer. */
    sds oldbuf = req->buffer;
    /* Search backwards for '$' since it's 'part' of the argument. */
    while (--offset >= 0) {
        if (*(oldbuf + offset) == '$') break;
    }
    success = (offset >= 0);
    sds newbuf = NULL;
    if (!success) goto cleanup;
    /* Trim the buffer up to the offset. */
    req->buffer = sdsnewlen(req->buffer, offset);
    success = (req->buffer != NULL);
    if (!success) goto cleanup;
    len = sdslen(req->buffer);
    /* Append "\r\n" if missing */
    if (req->buffer[len - 1] != '\n') {
        if (req->buffer[len - 1] != '\r')
            req->buffer = sdscat(req->buffer, "\r");
        req->buffer = sdscat(req->buffer, "\n");
    }
    /* Search for the argument count in the query (ie. '*2\r\n') */
    char *p = strchr(req->buffer, '*');
    if (p != NULL) p = strchr(p, '\r');
    success = (p != NULL);
    if (!success) goto cleanup;
    llen = (p - req->buffer);
    /* Create a new query header containing the updated argument count */
    newbuf = sdscatfmt(sdsempty(), "*%u", req->argc);
    diff = sdslen(newbuf) - llen;
    sdsrange(req->buffer, llen, -1);
    newbuf = sdscatfmt(newbuf, "%S", req->buffer);
    sdsfree(req->buffer);
    req->buffer = newbuf;
    /* Update offsets for current request's new buffer. */
    if (diff != 0)
        for (i = 0; i < req->argc; i++) req->offsets[i] += diff;
    /* Trim the original buffer from offset to end to create a new request. */
    sdsrange(oldbuf, offset, -1);
    clientRequest *cur = req->client->current_request;
    new = createRequest(req->client);
    success = (new != NULL);
    if (!success) goto cleanup;
    req->client->current_request = cur;
    new->argc = (original_argc - req->argc) + 1;
    new->parsed = 1;
    success = requestMakeRoomForArgs(new, new->argc);
    if (!success) goto cleanup;
    char *command_name = req->command->name;
    int cmdlen = strlen(command_name), first_offset = 0;
    newbuf = sdscatfmt(sdsempty(), "*%u\r\n$%u\r\n", new->argc, cmdlen);
    first_offset = sdslen(newbuf);
    newbuf = sdscatfmt(newbuf, "%s\r\n", req->command->name);
    len = sdslen(newbuf);
    new->offsets[0] = first_offset;
    new->lengths[0] = cmdlen;
    for (i = idx; i < original_argc; i++) {
        int new_idx = i - idx + 1;
        new->offsets[new_idx] = req->offsets[i] - offset + len;
        new->lengths[new_idx] = req->lengths[i];
    }
    newbuf = sdscat(newbuf, oldbuf);
    sdsfree(oldbuf);
    oldbuf = NULL;
    new->buffer = sdscat(new->buffer, newbuf);
    new->command = req->command;
    new->parent_request = parent;
    sdsfree(newbuf);
    newbuf = NULL;
    if (!getRequestNode(new, NULL)) {
        success = 0;
        goto cleanup;
    }
    listAddNodeHead(parent->child_requests, new);
    if (new->id > parent->max_child_reply_id)
        parent->max_child_reply_id = new->id;
    proxyLogDebug("Added child request " REQID_PRINTF_FMT
                  " to parent " REQID_PRINTF_FMT,
                  REQID_PRINTF_ARG(new), REQID_PRINTF_ARG(parent));
    if (config.dump_buffer) {
        sds bufrepr = sdsRepr(req->buffer);
        proxyLogDebug("Req. " REQID_PRINTF_FMT " buffer:\n%s",
                      REQID_PRINTF_ARG(req),
                      bufrepr);
        sdsfree(bufrepr);
    }
cleanup:
    if (!success) {
        proxyLogDebug("Failed to split multiple request " REQID_PRINTF_FMT,
                      REQID_PRINTF_ARG(req));
        if (oldbuf && oldbuf != req->buffer) sdsfree(oldbuf);
        if (newbuf) sdsfree(newbuf);
        if (new != NULL) {
            listNode *ln = listSearchKey(parent->child_requests, new);
            if (ln) listDelNode(parent->child_requests, ln);
            freeRequest(new);
        }
    }
    return success;
}

static clusterNode *getRequestNode(clientRequest *req, sds *err) {
    clusterNode *node = NULL;
    if (req->node && req->command == scanCommandDef) return req->node;
    int first_key = req->command->first_key,
        last_key = req->command->last_key,
        key_step = req->command->key_step, i;
    redisCluster *cluster = getCluster(req->client);
    int slot = UNDEFINED_SLOT, last_slot = UNDEFINED_SLOT, no_args = 0;
    if (req->argc == 1) {
        if (req->client->multi_transaction) {
            if (req->client->multi_transaction_node != NULL)
                node = req->node = req->client->multi_transaction_node;
            else if (req->closes_transaction)
                node = req->node = getFirstMappedNode(cluster);
            else no_args = 1;
            if (node != NULL) return node;
        } else if (req->command->handleReply != NULL) {
            if (!duplicateRequestForAllMasters(req)) {
                if (err) *err = sdsnew("Failed to create multiple requests");
                req->node = NULL;
                return NULL;
            }
            return req->node;
        } else no_args = 1;
    }
    if (no_args) {
        if (err != NULL)
            *err = sdsnew(ERROR_COMMAND_NO_ARGS);
        req->node = NULL;
        return NULL;
    }
    int *skip = NULL;
    int skiplen = 0;
    if (req->command->get_keys) {
        char *keys_err = NULL;
        int numkeys = req->command->get_keys(req, &first_key, &last_key,
            &key_step, &skip, &skiplen, &keys_err);
        if (numkeys < 0) {
            if (err != NULL)
                *err = sdsnew(keys_err ? keys_err : "Failed to get keys");
            if (skip != NULL) zfree(skip);
            req->node = NULL;
            return NULL;
        }
    }
    if (first_key >= req->argc) {
        /* Some commands have optional keys. In this case, route the query to
         * the first master node. */
        req->node = getFirstMappedNode(cluster);
        return req->node;
    } else if (first_key == 0) {
        /* If there's no first key defined and  command is not flagged to be
         * duplicated for all masters, jsut use the first master node. */
        if (req->command->proxy_flags & CMDFLAG_DUPLICATE) {
            if (!duplicateRequestForAllMasters(req)) {
                if (err) *err = sdsnew("Failed to create multiple requests");
                req->node = NULL;
                return NULL;
            }
        } else req->node = getFirstMappedNode(cluster);
        return req->node;
    }
    if (last_key >= req->argc) last_key = req->argc - 1;
    if (last_key < 0) last_key = req->argc + last_key;
    if (last_key < first_key) last_key = first_key;
    if (key_step < 1) key_step = 1;
    int skipped_count = 0;
    for (i = first_key; i <= last_key; i += key_step) {
        if (skip != NULL && skipped_count < skiplen) {
            if (i == skip[skipped_count]) {
                skipped_count++;
                continue;
            }
        }
        char *key = req->buffer + req->offsets[i];
        clusterNode *n = getNodeByKey(cluster, key, req->lengths[i], &slot);
        if (n == NULL) break;
        if (node == NULL) {
            node = n;
            last_slot = slot;
        } else {
            if (node != n || (last_slot != slot && slot != UNDEFINED_SLOT)) {
                char *errmsg = NULL;
                if (!config.cross_slot_enabled) {
                    errmsg = "Cross-slot queries are disabled. They can be "
                             "enabled by using the --enable-cross-slot option,"
                             " or by calling "
                             "`PROXY CONFIG SET enable-cross-slot 1`. "
                             "WARN: cross-slot queries can break the atomicity"
                             " of the query itself.";
                } else if (req->command->proxy_flags &
                           CMDFLAG_MULTISLOT_UNSUPPORTED ||
                           req->command->handleReply == NULL)
                {
                    errmsg = ERROR_COMMAND_UNSUPPORTED_CROSSSLOT;
                } else if (!splitMultiSlotRequest(req, i)) {
                    errmsg = "Failed to handle cross-slot request";
                }
                if (errmsg != NULL) {
                    if (err != NULL) {
                        if (*err != NULL) sdsfree(*err);
                        *err = sdsnew(errmsg);
                    }
                    node = NULL;
                    slot = UNDEFINED_SLOT;
                }
                break;
            }
        }
    }
    if (skip != NULL) zfree(skip);
    req->node = node;
    req->slot = slot;
    return node;
}

void freeRequest(clientRequest *req) {
    if (req == NULL) return;
    aeEventLoop *el = getClientLoop(req->client);
    if (req->need_reprocessing && el != NULL) return;
    proxyLogDebug("Free Request " REQID_PRINTF_FMT, REQID_PRINTF_ARG(req));
    /* If request is still writing to a shared (multiplex) connection, defer
     * freeing it later in order to avoid messing up the sharted connection
     * query order. */
    if (req->has_write_handler && req->written > 0 && el != NULL &&
        !req->owned_by_client)
    {
        proxyLogDebug("Request " REQID_PRINTF_FMT " is still writing, "
                      "cannot free it now...", REQID_PRINTF_ARG(req));
        return;
    }
    if (req->buffer != NULL) sdsfree(req->buffer);
    if (req->offsets != NULL) zfree(req->offsets);
    if (req->lengths != NULL) zfree(req->lengths);
    if (req->client->current_request == req)
        req->client->current_request = NULL;
    if (req->client->multi_request == req)
        req->client->multi_request = NULL;
    redisContext *ctx = NULL;
    if (req->node) ctx = getClusterNodeContext(req->node);
    if (ctx != NULL && req->has_write_handler && el != NULL)
        aeDeleteFileEvent(el, ctx->fd, AE_WRITABLE);
    if (req->child_requests != NULL) {
        if (listLength(req->child_requests) > 0) {
            listIter li;
            listNode *ln;
            listRewind(req->child_requests, &li);
            while ((ln = listNext(&li))) {
                clientRequest *r = ln->value;
                listNode *requests_lnode = r->requests_lnode;
                if (requests_lnode) {
                    r->requests_lnode = NULL;
                    requests_lnode->value = NULL;
                }
                freeRequest(r);
            }
        }
        listRelease(req->child_requests);
    }
    if (req->child_replies != NULL)
        raxFreeWithCallback(req->child_replies, (void (*)(void*))sdsfree);
    if (req->node != NULL) {
        redisClusterConnection *conn = req->node->connection;
        assert(conn != NULL);
        listNode *ln = req->requests_to_send_lnode;
        if (ln) listDelNode(conn->requests_to_send, ln);
        req->requests_to_send_lnode = NULL;
        /* We cannot delete the request's list node from the requests_pending
         * queue, since this would break the reply processing order. So we just
         * set its value to NULL. The resulting NULL placeholder (we can call
         * it a 'ghost request') will be simply skipped during reply buffer
         * processing. */
        ln = req->requests_pending_lnode;
        if (ln) ln->value = NULL;
        req->requests_pending_lnode = NULL;
    }
    listNode *ln = listSearchKey(req->client->requests_to_reprocess, req);
    if (ln) listDelNode(req->client->requests_to_reprocess, ln);
    redisCluster *cluster = getCluster(req->client);
    if (cluster) clusterRemoveRequestToReprocess(cluster, req);
    if (config.dump_queues)
        dumpQueue(req->node, req->client->thread_id, QUEUE_TYPE_PENDING);
    ln = req->requests_lnode;
    if (ln) listDelNode(req->client->requests, ln);
    zfree(req);
}

void freeRequestList(list *request_list) {
    if (request_list == NULL) return;
    listIter li;
    listNode *ln;
    listRewind(request_list, &li);
    while ((ln = listNext(&li))) {
        clientRequest *req = ln->value;
        listDelNode(request_list, ln);
        if (req == NULL) continue;
        if (ln == req->requests_lnode) req->requests_lnode = NULL;
        else if (ln == req->requests_to_send_lnode)
            req->requests_to_send_lnode = NULL;
        else if (ln == req->requests_pending_lnode)
            req->requests_pending_lnode = NULL;
        freeRequest(req);
    }
    listRelease(request_list);
}

/* Return the first queued request from thr specified queue. It can return
 * NULL if the queue is empty or if the first list node is just a NULL
 * placeholder (a 'ghost request') used in place of a request created by a
 * freed client (ie. a disconnected client). The is_empty pointer can be
 * used to check if the queue is actually empty or if the first node is a
 * NULL placeholder for a ghost request. */
static clientRequest *getFirstQueuedRequest(list *queue, int *is_empty) {
    if (is_empty != NULL) *is_empty = 0;
    listNode *ln = listFirst(queue);
    if (ln == NULL) {
        if (is_empty != NULL) *is_empty = 1;
        return NULL;
    }
    return (clientRequest *) ln->value;
}

static redisClusterConnection *getRequestConnection(clientRequest *req) {
    clusterNode *node = req->node;
    if (node == NULL) return NULL;
    return node->connection;
}

static int enqueueRequest(clientRequest *req, int queue_type) {
    redisClusterConnection *conn = getRequestConnection(req);
    if (conn == NULL) return 0;
    int success = 0;
    int *sp = &success;
    if (queue_type == QUEUE_TYPE_SENDING) {
        addObjectToList(req, conn, requests_to_send, sp);
    } else if (queue_type == QUEUE_TYPE_PENDING) {
        addObjectToList(req, conn, requests_pending, sp);
    }
    return success;
}

static void dequeueRequest(clientRequest *req, int queue_type) {
    redisClusterConnection *conn = getRequestConnection(req);
    if (conn == NULL) return;
    if (queue_type == QUEUE_TYPE_SENDING) {
        removeObjectFromList(req, conn, requests_to_send);
    } else if (queue_type == QUEUE_TYPE_PENDING) {
        removeObjectFromList(req, conn, requests_pending);
    }
}

static clientRequest *createRequest(client *c) {
    clientRequest *req = zcalloc(sizeof(*req));
    if (req == NULL) goto alloc_failure;
    int added = 0;
    int *p_added = &added;
    addObjectToList(req, c, requests, p_added);
    if (!added) goto alloc_failure;
    req->client = c;
    req->buffer = sdsempty();
    if (req->buffer ==  NULL) goto alloc_failure;
    req->query_offset = 0;
    req->is_multibulk = REQ_STATUS_UNKNOWN;
    req->pending_bulks = REQ_STATUS_UNKNOWN;
    req->current_bulk_length = REQ_STATUS_UNKNOWN;
    req->parsing_status = PARSE_STATUS_INCOMPLETE;
    req->has_write_handler = 0;
    req->owned_by_client = 0;
    req->written = 0;
    size_t offsets_size = sizeof(int) * QUERY_OFFSETS_MIN_SIZE;
    req->offsets = zmalloc(offsets_size);
    req->lengths = zmalloc(offsets_size);
    if (!req->offsets || !req->lengths) goto alloc_failure;
    req->argc = 0;
    req->offsets_size = QUERY_OFFSETS_MIN_SIZE;
    req->command = NULL;
    req->node = NULL;
    req->slot = UNDEFINED_SLOT;
    c->current_request = req;
    req->id = c->next_request_id++;
    /* Avoid overflow */
    /* TODO: this could lead to issues in reply sequencing */
    if (c->next_request_id >= UINT64_MAX)
        c->next_request_id = 0;
    req->need_reprocessing = 0;
    req->parsed = 0;
    req->owned_by_client = (c->cluster != NULL);
    req->closes_transaction = 0;
    req->parent_request = NULL;
    req->child_requests = NULL;
    req->child_replies = NULL;
    req->requests_pending_lnode = NULL;
    req->requests_to_send_lnode = NULL;
    req->max_child_reply_id = req->id;
    proxyLogDebug("Created Request " REQID_PRINTF_FMT  " with address %p",
                  REQID_PRINTF_ARG(req), (void *)req);
    return req;
alloc_failure:
    proxyLogErr("ERROR: Failed to allocate request!");
    if (!req) return NULL;
    freeRequest(req);
    return NULL;
}

/* Try to call aeCreateFileEvent, and if ERANGE error has been issued, try to
 * resize event loop's setsize to fd + proxy.min_reserved_fds.
 * The 'retried' argument is used to ensure that resizing will be tried only
 * once. */
static int installIOHandler(aeEventLoop *el, int fd, int mask, aeFileProc *proc,
                            void *data, int retried)
{
    if (aeCreateFileEvent(el, fd, mask, proc, data) != AE_ERR) {
        return 1;
    } else {
        if (!retried && errno == ERANGE) {
            proxyThread *thread = el->privdata;
            assert(thread != NULL);
            int newsize = fd + proxy.min_reserved_fds;
            proxyLogDebug("Resizing event loop on thread %d to %d",
                thread->thread_id, newsize);
            if (aeResizeSetSize(el, newsize) == AE_ERR)
                return 0;
            return installIOHandler(el, fd, mask, proc, data, 1);
        } else if (errno != ERANGE) {
            proxyLogErr("Could not create read handler: %s", strerror(errno));
        }
        return 0;
    }
}

/* Fetch the cluster node connection (redisContext *) related to the
 * request and try to connect to it if not already connected.
 * Then install the write handler on the request.
 * Return 1 if the request already has a write handler or if the
 * write handler has been correctly installed.
 * Return 0 if the connection to the cluster node is missing and cannot be
 * established or if the write handler installation fails. */
static int sendRequestToCluster(clientRequest *req, sds *errmsg)
{
    if (errmsg != NULL) *errmsg = NULL;
    if (req->has_write_handler) return 1;
    /* If client has disconnected and request's tagret is on a private
     * connection owned by the client itself, just free the request and stop
     * proceeding. */
    if (req->client->status == CLIENT_STATUS_UNLINKED && req->owned_by_client) {
        dequeueRequestToSend(req);
        freeRequest(req);
        return 0;
    }
    assert(req->node != NULL);
    redisCluster *cluster = getCluster(req->client);
    assert(cluster != NULL);
    if (cluster->owner && cluster->owner == req->client)
        req->owned_by_client = 1;
    aeEventLoop *el = getClientLoop(req->client);
    redisContext *ctx = getClusterNodeContext(req->node);
    if (ctx == NULL) {
        if ((ctx = clusterNodeConnect(req->node)) == NULL) {
            sds err = sdsnew("Could not connect to node ");
            err = sdscatfmt(err, "%s:%u", req->node->ip, req->node->port);
            addReplyError(req->client, err, req->id);
            proxyLogDebug("%s", err);
            if (errmsg != NULL) {
                /* Remember to free the string outside this function*/
                *errmsg = err;
            } else sdsfree(err);
            freeRequest(req);
            return 0;
        }
        /* Install the write handler since the connection to the cluster node
         * is asynchronous. */
        if (!installIOHandler(el, ctx->fd, AE_WRITABLE, writeToClusterHandler,
                              req->node->connection, 0))
        {
            addReplyError(req->client, ERROR_CLUSTER_WRITE_FAIL, req->id);
            proxyLogErr("Failed to create write handler for request "
                REQID_PRINTF_FMT, REQID_PRINTF_ARG(req));
            freeRequest(req);
            return 0;
        }
        req->has_write_handler = 1;
        /* Only count requests_with_write_handler on shared connection. */
        if (!req->owned_by_client) req->client->requests_with_write_handler++;
        proxyLogDebug("Write handler installed into request " REQID_PRINTF_FMT
                      " for node %s:%d", REQID_PRINTF_ARG(req),
                      req->node->ip, req->node->port);
        return 1;
    } else if (!isClusterNodeConnected(req->node)) {
        return 1;
    }
    redisClusterConnection *conn = req->node->connection;
    assert(conn != NULL);
    if (!conn->has_read_handler) {
        if (!installIOHandler(el, ctx->fd, AE_READABLE, readClusterReply,
                              conn, 0))
        {
            proxyLogErr("Failed to create read reply handler for node %s:%d",
                          req->node->ip, req->node->port);
            addReplyError(req->client, ERROR_CLUSTER_READ_FAIL,
                          req->id);
            freeRequest(req);
            return 0;
        } else  {
            conn->has_read_handler = 1;
            proxyLogDebug("Read reply handler installed "
                          "for node %s:%d", req->node->ip, req->node->port);
        }
    }
    if (!writeToCluster(el, ctx->fd, req)) return 0;
    return 1;
}

/* Try to send the next request in requests_to_send list, by calling
 * sendRequestToCluster. If an error occurs (sendRequestToCluster returns 0)
 * keep cycling the queue until sendRequestsToCluster returns 1.
 * If **failed is not NULL, set the first failed request pointer to **failed,
 * and it can be used to check if the request has already been freed.
 * Return the handled request, if any. */

static clientRequest *handleNextRequestsToCluster(clusterNode *node,
                                                  clientRequest **failed)
{

    clientRequest *req = NULL;
    while ((req = getFirstRequestToSend(node, NULL))) {
        int sent = sendRequestToCluster(req, NULL);
        if (!sent) {
            /* Sending request failed and request has already been freed. */
            if (failed != NULL && *failed == NULL) *failed = req;
            continue;
        }
        /* If request has not been completely written (it has a write handler
         * installed, just break for now. */
        if (req->has_write_handler) break;
    }
    return req;
}

/* Check whether a client with private connection (multiplexing disabled) still
 * has pending requests in the multiplexed context. After all pending requests
 * have been consumed, start sending requests to the private connection. */
static void checkForMultiplexingRequestsToBeConsumed(clientRequest *req) {
    if (req->client->cluster != NULL && !req->owned_by_client) {
        if (--req->client->pending_multiplex_requests <= 0) {
            proxyLogDebug("Client %d:%" PRIu64 " has no more pending requests "
                          "on the multiplexing context. Sending requests "
                          "to the private cluster",
                          req->client->thread_id, req->client->id);
            listIter li;
            listNode *ln;
            listRewind(req->client->cluster->nodes, &li);
            while ((ln = listNext(&li))) {
                clusterNode *node = ln->value;
                handleNextRequestsToCluster(node, NULL);
            }
        } else {
            proxyLogDebug("Client %d:%" PRIu64 " still has %d pending requests "
                          "on the multiplexing context.",
                          req->client->thread_id, req->client->id,
                          req->client->pending_multiplex_requests);
        }
    }
}

int processRequest(clientRequest *req, int *parsing_status,
                   clientRequest **next)
{
    uint64_t req_id = req->id;
    int invalid_request_replied = 0;
    client *c = req->client;
    if (next != NULL) *next = NULL;
    if (!req->parsed) {
        sds parsing_err = NULL;
        int status = parseRequest(req, &parsing_err);
        if (parsing_status != NULL) *parsing_status = status;
        if (status == PARSE_STATUS_INCOMPLETE) return 1;
        else if (status == PARSE_STATUS_ERROR) {
            if (parsing_err != NULL) {
                /* Reply with the parsing error and set the client to be
                 * closed after the reply. Return 1 just like if processing
                 * were complete. */
                addReplyError(c, parsing_err, req->id);
                c->flags |= CLIENT_CLOSE_AFTER_REPLY;
                sdsfree(parsing_err);
                return 1;
            }
            return 0;
        }
        req->parsed = 1;
        proxyLogDebug("Parsing complete for request " REQID_PRINTF_FMT,
            REQID_PRINTF_ARG(req));
    }
    if (next != NULL && req->requests_lnode != NULL) {
        listNode *next_node = listNextNode(req->requests_lnode);
        if (next_node == NULL) *next = NULL;
        else *next = next_node->value;
    }
    if (req == c->current_request) c->current_request = NULL;
    if (req->id < c->min_reply_id) c->min_reply_id = req->id;
    sds command_name = NULL;
    sds errmsg = NULL;
    proxyLogDebug("Processing request " REQID_PRINTF_FMT,
                  REQID_PRINTF_ARG(req));
    if (req->argc == 0) {
        proxyLogDebug("Request with zero arguments");
        errmsg = sdsnew("Invalid request");
        goto invalid_request;
    }
    command_name = getRequestCommand(req);
    if (command_name == NULL) {
        proxyLogDebug("Missing command name");
        errmsg = sdsnew("Invalid request");
        goto invalid_request;
    }
    if (!config.dump_queries) {
        proxyLogDebug("Command for request " REQID_PRINTF_FMT  ": '%s' "
                      "(use --dump-queries for full query dump)",
                      REQID_PRINTF_ARG(req), command_name);
    }
    redisCommandDef *cmd = getRedisCommand(command_name);
    /* Unsupported commands:
     * - Commands not defined in redisCommandTable
     * - Commands explictly having unsupported to 1 */
    if (cmd == NULL || cmd->unsupported){
        errmsg = sdscatprintf(sdsempty(),
                            "%s command `%.*s`",
                             (cmd == NULL ? "unknown" : "unsupported"),
                             128, command_name);
        proxyLogDebug("%s", errmsg);
        goto invalid_request;
    }
    req->command = cmd;
    if (cmd->handle && cmd->handle(req) == PROXY_COMMAND_HANDLED) {
        if (command_name) sdsfree(command_name);
        return 1;
    }
    redisCluster *cluster = getCluster(c);
    assert(cluster != NULL);
    if (cluster->broken) {
        errmsg = sdsnew(ERROR_CLUSTER_RECONFIG);
        goto invalid_request;
    } else if (cluster->is_updating) {
        clusterAddRequestToReprocess(cluster, req);
        if (command_name) sdsfree(command_name);
        return 1;
    }
    clusterNode *node = getRequestNode(req, &errmsg);
    if (node == NULL) {
        if (errmsg == NULL)
            errmsg = sdsnew(ERROR_NO_NODE);
        proxyLogDebug("%s " REQID_PRINTF_FMT, errmsg,
                      REQID_PRINTF_ARG(req));
        goto invalid_request;
    }
    /* In we are under a MULTI transaction and there's no target node yet
     * we use the current request's node as the node for the whole transaction.
     * In this case we'll enqueue the client's MULTI request (multi_request),
     * after setting the node to it. The same node will be used for all queries
     * under that transaction. */
    if (req->client->multi_transaction) {
        clientRequest *multi_req = req->client->multi_request;
        if (multi_req != NULL) {
            if (multi_req->node == NULL) {
                multi_req->node = node;
                if (!enqueueRequestToSend(multi_req))
                    goto invalid_request;
                /* The client's min_reply_id must be also set to this first
                 * query inside the transaction, since the 'multi_request'
                 * request reply will be skipped. */
                req->client->min_reply_id = req->id;
            }
        }
        if (req->client->multi_transaction_node == NULL)
            req->client->multi_transaction_node = node;
        else node = req->node = req->client->multi_transaction_node;
    }
    if (!enqueueRequestToSend(req)) goto invalid_request;
    clientRequest *failed_req = NULL;
    uint64_t min_reply_id = c->min_reply_id;
    handleNextRequestsToCluster(req->node, &failed_req);
    /* If requests has failed, it has been already freed, so jump directly to
     * invalid_request. */
    if (failed_req == req) {
        req = NULL;
        /* If no reply has been added during failed sending, add an error
         * reply. */
        int replied = (c->min_reply_id != min_reply_id ||
                       getUnorderedReplyForRequestWithID(c, req_id) != NULL);
        if (!replied)
            errmsg = sdsnew(ERROR_CLUSTER_WRITE_FAIL);
        else invalid_request_replied = 1;
        goto invalid_request;
    }
    if (req->child_requests && listLength(req->child_requests) > 0) {
        listIter li;
        listNode *ln;
        listRewind(req->child_requests, &li);
        clusterNode *last_node = req->node;
        while ((ln = listNext(&li))) {
            clientRequest *r = ln->value;
            if (!enqueueRequestToSend(r)) goto invalid_request;
            clientRequest *next_req = getFirstRequestToSend(r->node, NULL);
            if (r->node != last_node || next_req == r) {
                last_node = r->node;
                clientRequest *failed_req = NULL;
                handleNextRequestsToCluster(r->node, &failed_req);
                if (failed_req == r) goto invalid_request;
            }
        }
    }
    if (command_name) sdsfree(command_name);
    return 1;
invalid_request:
    if (command_name) sdsfree(command_name);
    if (errmsg != NULL) {
        addReplyError(c, (char *) errmsg, req_id);
        sdsfree(errmsg);
        if (req) freeRequest(req);
        return 1;
    }
    if (req) freeRequest(req);
    return (invalid_request_replied ? 1 : 0);
}

void readQuery(aeEventLoop *el, int fd, void *privdata, int mask){
    UNUSED(el);
    UNUSED(mask);
    client *c = (client *) privdata;
    /* Do not process incoming queries anymore if client is set to be closed
     * after all replies are written. */
    if (c->flags & CLIENT_CLOSE_AFTER_REPLY) return;
    int nread, readlen = (1024*16);
    clientRequest *req = c->current_request;
    if (req == NULL) {
        req = createRequest(c);
        if (req == NULL) {
            proxyLogErr("Failed to create request");
            freeClient(c);
            return;
        }
    }
    size_t iblen = sdslen(req->buffer);
    req->buffer = sdsMakeRoomFor(req->buffer, readlen);
    nread = read(fd, req->buffer + iblen, readlen);
    if (nread == -1) {
        if (errno == EAGAIN) {
            return;
        } else {
            proxyLogDebug("Error reading from client %d:%" PRId64 " %s : %s",
                          c->thread_id, c->id, c->addr, strerror(errno));
            unlinkClient(c); /* TODO: Free? */
            return;
        }
    } else if (nread == 0) {
        proxyLogDebug("Client %d:%" PRId64 " from %s closed connection "
                      "(thread: %d)",
                      c->thread_id, c->id, c->addr, c->thread_id);
        freeClient(c);
        return;
    }
    sdsIncrLen(req->buffer, nread);
    proxyLogDebug("Read %d bytes into req. " REQID_PRINTF_FMT ", buffer is "
                  "%zu bytes", nread, REQID_PRINTF_ARG(req),
                  sdslen(req->buffer));
    /*TODO: support max query buffer length */
    int parsing_status = PARSE_STATUS_OK;
    clientRequest *next = req;
    while (next != NULL) {
        if (!processRequest(next, &parsing_status, &next)) {
            unlinkClient(c);
            break;
        }
        /* If parsing status of the current request is incomplete or the
         * client is set to be closed after reply, just stop here. */
        if (parsing_status == PARSE_STATUS_INCOMPLETE ||
            c->flags & CLIENT_CLOSE_AFTER_REPLY) return;
    }
}

static void acceptHandler(int fd, char *ip, int port) {
    if (proxy.numclients >= (uint64_t) config.max_clients) {
        char *err = "-ERR max number of clients reached\r\n";
        static int errlen = 0;
        if (errlen == 0) errlen = strlen(err);
        write(fd, err, errlen);
        close(fd);
        return;
    }
    client *c = createClient(fd, ip);
    if (c == NULL) {
        proxyLogDebug("Failed to allocate memory for client from %s",
            ip ? ip : config.unixsocket);
        sds err = sdscatprintf(sdsempty(), "-ERR %s\r\n", ERROR_OOM);
        write(fd, err, sdslen(err));
        close(fd);
        sdsfree(err);
        return;
    }
    c->port = port;
    if (ip) c->addr = sdscatprintf(sdsempty(), "%s:%d", ip, port);
    else c->addr = sdscatprintf(sdsempty(), "unix://%s", config.unixsocket);
    proxyLogDebug("Client %d:%" PRId64 " connected from %s (thread: %d)",
                  c->thread_id, c->id, c->addr, c->thread_id);
    proxyThread *thread = proxy.threads[c->thread_id];
    assert(thread != NULL);
    if (!awakeThreadForNewClient(thread, c)) {
        proxyLogDebug("Failed to awake thread %d for client %" PRId64,
                      thread->thread_id, c->id);
        char *err = "-ERR failed to awake thread\r\n";
        write(fd, err, strlen(err));
        freeClient(c);
    }
}

void acceptTcpHandler(aeEventLoop *el, int fd, void *privdata, int mask)
{
    UNUSED(el);
    UNUSED(mask);
    UNUSED(privdata);
    int client_port, client_fd, max = MAX_ACCEPTS;
    char client_ip[NET_IP_STR_LEN];
    while (max--) {
        client_fd = anetTcpAccept(proxy.neterr, fd, client_ip,
                                  sizeof(client_ip), &client_port);
        if (client_fd == ANET_ERR) {
            if (errno != EWOULDBLOCK)
                proxyLogWarn("Error accepting client connection: %s",
                             proxy.neterr);
            return;
        }
        proxyLogDebug("Accepted connection from %s:%d", client_ip,
                      client_port);
        acceptHandler(client_fd, client_ip, client_port);
    }
}

void acceptUnixHandler(aeEventLoop *el, int fd, void *privdata, int mask) {
    UNUSED(el);
    UNUSED(mask);
    UNUSED(privdata);
    int client_fd, max = MAX_ACCEPTS;
    while (max--) {
        client_fd = anetUnixAccept(proxy.neterr, fd);
        if (client_fd == ANET_ERR) {
            if (errno != EWOULDBLOCK)
                proxyLogWarn("Accepting client connection: %s",
                             proxy.neterr);
            return;
        }
        proxyLogDebug("Accepted connection to %s", config.unixsocket);
        acceptHandler(client_fd, NULL, 0);
    }
}

/* Add the reply received by request `*req` to the `child_replies` rax.
 * When all child requests received their replies, call the command's
 * reply handler (handleReply), and free all the requests (both parent and
 * children).
 * Return 1 if all replies were completed, elsewhere 0. */
static int addChildRequestReply(clientRequest *req, char *replybuf, int len) {
    clientRequest *parent = req->parent_request;
    /* If the request has no parent, then the request itself is the parent. */
    if (parent == NULL) parent = req;
    assert(parent->child_replies != NULL);
    proxyLogDebug("Adding child reply for " REQID_PRINTF_FMT,
                  REQID_PRINTF_ARG(req));
    uint64_t be_id = htonu64(req->id); /* Big-endian request ID */
    sds reply = sdsnewlen(replybuf, len);
    raxInsert(parent->child_replies, (unsigned char *) &be_id,
              sizeof(be_id), reply, NULL);
    /* Check if all the requests (all child requests plus the parent request)
     * received the reply. */
    uint64_t numrequests = listLength(parent->child_requests) + 1;
    int completed = (raxSize(parent->child_replies) == numrequests);
    if (completed) {
        proxyLogDebug("All %" PRId64 " child requests of " REQID_PRINTF_FMT
                      " received replies",
                      numrequests, REQID_PRINTF_ARG(req));
        redisCommandDef *cmd = parent->command;
        assert(cmd != NULL);
        if (cmd->handleReply != NULL) {
            cmd->handleReply(NULL, parent, NULL, 0);
        } else {
            addReplyError(parent->client, ERROR_COMMAND_UNSUPPORTED_CROSSSLOT,
                          parent->id);
        }
        freeRequest(parent);
    }
    return completed;
}

static int processClusterReplyBuffer(redisContext *ctx, clusterNode *node,
                                     int thread_id)
{
    if (node == NULL) return 0;
    char *errmsg = NULL;
    void *_reply = NULL;
    redisReply *reply = NULL;
    int replies = 0;
    while (ctx->reader->len > 0) {
        int ok =
            (__hiredisReadReplyFromBuffer(ctx->reader, &_reply) == REDIS_OK);
        int do_break = 0, is_cluster_err = 0;
        redisCluster *cluster = NULL;
        if (!ok) {
            proxyLogErr("Error reading from node %s:%d on thread %d: %s",
                        node->ip, node->port, thread_id, ctx->errstr);
            errmsg = ERROR_CLUSTER_READ_FAIL;
        }
        reply = (redisReply *) _reply;
        /* Reply not yet available, just return */
        if (ok && reply == NULL) break;
        replies++;
        clientRequest *req = getFirstRequestPending(node, NULL);
        int free_req = 1;
        /* If request is NULL, it's a ghost request that is a NULL
         * placeholder in place of a request created by a freed client
         * (ie. a disconnected client). In this case, just dequeue the list
         * node containing the NULL placeholder and directly skip to
         * 'consume_buffer' in order to process the remaining reply buffer. */
        if (req == NULL) {
            list *queue = node->connection->requests_pending;
            /* It should never happen that the request is NULL because of an
             * empty queue while we still have reply buffer to process */
            if (listLength(queue) == 0) {
                logClusterReplyFailure("listLength(queue) > 0", reply, node);
                assert(listLength(queue) > 0);
            }
            listNode *ln = listFirst(queue);
            listDelNode(queue, ln);
            goto consume_buffer;
        } else if (req == req->client->multi_request) {
            /* Since we've already replied "OK" to the first handled MULTI
             * query received, we'll skip the actual MULTI's reply received
             * from the node. */
            /* TODO: if (errmsg != NULL) ; */
            dequeuePendingRequest(req);
            goto consume_buffer;
        }
        proxyLogDebug("Reply read complete for request " REQID_PRINTF_FMT
                      ", %s%s", REQID_PRINTF_ARG(req),
                      errmsg ? " ERR: " : "OK!",
                      errmsg ? errmsg : "");
        dequeuePendingRequest(req);
        cluster = getCluster(req->client);
        assert(cluster != NULL);
        if (reply->type == REDIS_REPLY_ERROR) {
            assert(reply->str != NULL);
            /* In case of ASK|MOVED reply the cluster need to be
             * reconfigured.
             * In this case we suddenly set `cluster->is_updating` and
             * we add the request to the clusters' `requests_to_reprocess`
             * pool (the request will be also added to a
             * `requests_to_reprocess` list on the client). */
            if ((strstr(reply->str, "ASK") == reply->str ||
                strstr(reply->str, "MOVED") == reply->str))
            {
                proxyLogDebug("Cluster configuration changed! "
                              "(request " REQID_PRINTF_FMT ")",
                              REQID_PRINTF_ARG(req));
                cluster->update_required = 1;
                /* Automatic cluster update is posticipated when the client
                 * is under a MULTI transaction. */
                if (!req->client->multi_transaction) {
                    cluster->is_updating = 1;
                    is_cluster_err = 1;
                    clusterAddRequestToReprocess(cluster, req);
                    /* We directly jump to `consume_buffer` since we won't
                     * reply to the client now, but after the reconfiguration
                     * ends. */
                    goto consume_buffer;
                }
            }
        }
        if (errmsg != NULL) addReplyError(req->client, errmsg, req->id);
        else {
            char *obuf = ctx->reader->buf;
            /*size_t len = ctx->reader->len;*/
            size_t len = ctx->reader->pos;
            if (len > ctx->reader->len) len = ctx->reader->len;
            if (config.dump_buffer) {
                sds rstr = sdscatrepr(sdsempty(), obuf, len);
                proxyLogDebug("Reply for request " REQID_PRINTF_FMT
                             ":\n%s", REQID_PRINTF_ARG(req), rstr);
                sdsfree(rstr);
            }
            if (req->child_requests != NULL || req->parent_request != NULL) {
                free_req = 0;
                /* Add the child/parent reply. If all child requests and their
                 * parente received their replies, all requests have been
                 * already freed. So directly jump to consume_buffer in the
                 * latter case. */
                if (addChildRequestReply(req, obuf, len)) {
                    req = NULL;
                    goto consume_buffer;
                }
            } else {
                proxyLogDebug("Writing reply for request " REQID_PRINTF_FMT
                              " to client buffer...",
                              REQID_PRINTF_ARG(req));
                redisCommandDef *cmd = req->command;
                int cmdflags = cmd->proxy_flags;
                if (cmd->handleReply && (cmdflags & CMDFLAG_HANDLE_REPLY)) {
                    if (req->command->handleReply(reply, req, obuf, len) !=
                        PROXY_REPLY_UNHANDLED)
                    {
                        req = NULL;
                        goto consume_buffer;
                    }
                }
                addReplyRaw(req->client, obuf, len, req->id);
            }
        }
        if (req->closes_transaction) {
            req->client->multi_transaction = 0;
            if (req->client->multi_request != NULL) {
                freeRequest(req->client->multi_request);
                req->client->multi_request = NULL;
            }
            req->client->multi_transaction_node = NULL;
            if (cluster->update_required) cluster->is_updating = 1;
        }
consume_buffer:
        if (config.dump_queues) dumpQueue(node, thread_id, QUEUE_TYPE_PENDING);
        /* If cluster has been set is reconfiguring state, we call the
         * startClusterReconfiguration function. */
        if (cluster && cluster->is_updating) {
            int reconfig_status = updateCluster(cluster);
            do_break = (reconfig_status == CLUSTER_RECONFIG_ENDED);
            if (!do_break) {
                /* If reconfiguration failed, reply the error to the client,
                 * elsewhere we're still waiting for all requests pending
                 * to finish before reconfigration actually starts.
                 * In the latter case, we don't do anything. */
                if (reconfig_status == CLUSTER_RECONFIG_ERR) {
                    proxyLogErr("Cluster reconfiguration failed! (thread %d)",
                                thread_id);
                    if (req) {
                        addReplyError(req->client,
                                      ERROR_CLUSTER_RECONFIG,
                                      req->id);
                    }
                    do_break = 1;
                }
            } else {
                /* Reconfiguration ended with success. If reply was ASK or
                 * MOVED, the request has been enqueued again and, since
                 * need_reprocessing is now 0, we set it to NULL in order to
                 * avoid freeing it.
                 * For all other requests, their reply has been added so
                 * they can be freed, but since their node was related to
                 * the old configuration and it has now been freed,
                 * we just set their node to NULL. */
                if (is_cluster_err) req = NULL;
                else if (req) req->node = NULL;
            }
            if (do_break) goto clean;
        }
        /* Consume reader buffer */
        consumeRedisReaderBuffer(ctx);
clean:
        freeReplyObject(reply);
        if (req && free_req) freeRequest(req);
        if (!ok || do_break) break;
    }
    return replies;
}

static void readClusterReply(aeEventLoop *el, int fd,
                             void *privdata, int mask)
{
    UNUSED(mask);
    UNUSED(fd);
    proxyThread *thread = el->privdata;
    int thread_id = thread->thread_id;
    redisClusterConnection *connection = privdata;
    if (connection == NULL) return;
    redisContext *ctx = connection->context;
    clusterNode *node = connection->node;
    clientRequest *req = NULL;
    list *queue = connection->requests_pending;
    if (node != NULL)
        req = getFirstRequestPending(node, NULL);
    sds errmsg = NULL;
    char *ip = ctx->tcp.host;
    int port = ctx->tcp.port;
    proxyLogDebug("Reading reply from %s:%d on thread %d...",
                  ip, port, thread_id);
    int success = (redisBufferRead(ctx) == REDIS_OK), replies = 0,
                  node_disconnected = 0;
    if (!success) {
        proxyLogDebug("Failed redisBufferRead from %s:%d on thread %d",
                      ip, port, thread_id);
        int err = ctx->err;
        node_disconnected = (err & (REDIS_ERR_IO | REDIS_ERR_EOF));
        if (node_disconnected) errmsg = sdsnew(ERROR_NODE_DISCONNECTED);
        else {
            proxyLogErr("Error from node %s:%d: %s", ip, port,
                        ctx->errstr);
            errmsg = sdsnew("Failed to read reply from ");
        }
        errmsg = sdscatfmt(errmsg, "%s:%u", ip, port);
        /* An error occurred, so dequeue the request. If the request is not
         * NULL, send an error reply to the client and then free the requests
         * itself. If the node is down (node_disconnected), call
         * clusterNodeDisconnect in order to close the socket, close the file
         * event handlers and close all the other requests waiting for a
         * reply on the same node's socket. */
        if (req != NULL) {
            dequeuePendingRequest(req);
            addReplyError(req->client, errmsg, req->id);
            client *c = req->client;
            if (c->cluster && c->pending_multiplex_requests > 0)
                checkForMultiplexingRequestsToBeConsumed(req);
            freeRequest(req);
        } else {
            listNode *first = listFirst(queue);
            if (first) listDelNode(queue, first);
        }
        if (connection->authenticating) {
            connection->authenticating = 0;
            connection->authenticated = 0;
        }
        if (node_disconnected) {
            proxyLogDebug("%s", errmsg);
            if (node) clusterNodeDisconnect(node);
        }
        sdsfree(errmsg);
        /* Exit, since an error occurred. */
        return;
    } else if (connection->authenticating) {
        /* Read the reply for the AUTH command sent to the node */
        void *r = NULL;
        int ok = (__hiredisReadReplyFromBuffer(ctx->reader, &r) != REDIS_ERR);
        redisReply *authreply = r;
        if (!ok) {
            proxyLogErr("Failed to get reply for the AUTH command to node "
                        "%s:%d. Error: '%s'",
                        ip, port, ctx->err ? ctx->errstr : "");
            connection->authenticating = 0;
            connection->authenticated = 0;
            if (node) clusterNodeDisconnect(node);
        } else if (authreply) {
            if (authreply->type != REDIS_REPLY_ERROR) {
                connection->authenticating = 0;
                connection->authenticated = 1;
            } else {
                connection->authenticating = 0;
                connection->authenticated = 0;
            }
            consumeRedisReaderBuffer(ctx);
            if (node) handleNextRequestsToCluster(node, NULL);
        }
        if (authreply) freeReplyObject(authreply);
        return;
    } else replies = processClusterReplyBuffer(ctx, node, thread_id);
    UNUSED(replies);
    if (errmsg != NULL) sdsfree(errmsg);
}

static void *execProxyThread(void *ptr) {
    proxyThread *thread = (proxyThread *) ptr;
    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
    pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS, NULL);
    thread_id = thread->thread_id;
    aeMain(thread->loop);
    proxyLogInfo("Thread %d ended", thread->thread_id);
    return NULL;
}

static void sigShutdownHandler(int sig) {
    UNUSED(sig);
    char *msg;
    switch (sig) {
    case SIGINT:
        msg = "Received SIGINT";
        break;
    case SIGTERM:
        msg = "Received SIGTERM";
        break;
    default:
        msg = "Received shutdown signal";
    };
    int is_main_thread = (pthread_self() == proxy.main_thread);
    if (is_main_thread) proxyLogHdr("%s on main thread", msg);
    else proxyLogHdr("%s on thread %d", msg, getCurrentThreadID());
    if (!proxy.exit_asap) {
        if (is_main_thread) {
            proxy.exit_asap = 1;
            aeStop(proxy.main_loop);
        } else {
            proxyLogHdr("Forwarding signal to main thread...");
            pthread_kill(proxy.main_thread, sig);
        }
    } else {
        proxyLogHdr("Dirty exit!");
        exit(0);
    }
}

static void setupSignalHandlers(void) {
    struct sigaction act;

    /* When the SA_SIGINFO flag is set in sa_flags then sa_sigaction is used.
     * Otherwise, sa_handler is used. */
    sigemptyset(&act.sa_mask);
    act.sa_flags = 0;
    act.sa_handler = sigShutdownHandler;
    sigaction(SIGTERM, &act, NULL);
    sigaction(SIGINT, &act, NULL);

#ifdef HAVE_BACKTRACE
    sigemptyset(&act.sa_mask);
    act.sa_flags = SA_NODEFER | SA_RESETHAND | SA_SIGINFO;
    act.sa_sigaction = sigsegvHandler;
    sigaction(SIGSEGV, &act, NULL);
    sigaction(SIGBUS, &act, NULL);
    sigaction(SIGFPE, &act, NULL);
    sigaction(SIGILL, &act, NULL);
#endif
    return;
}

void daemonize(void) {
    int fd;

    if (fork() != 0) exit(0); /* parent exits */
    setsid(); /* create a new session */

    /* Every output goes to /dev/null. If the proxy is daemonized but
     * the 'logfile' is set to 'stdout' in the configuration file
     * it will not log at all. */
    if ((fd = open("/dev/null", O_RDWR, 0)) != -1) {
        dup2(fd, STDIN_FILENO);
        dup2(fd, STDOUT_FILENO);
        dup2(fd, STDERR_FILENO);
        if (fd > STDERR_FILENO) close(fd);
    }
}

void createPidFile(void) {
    if (config.pidfile == NULL) config.pidfile = zstrdup(DEFAULT_PID_FILE);
    FILE *f = fopen(config.pidfile, "w");
    if (!f) {
        proxyLogErr("Failed to create pidfile: '%s'", config.pidfile);
        return;
    }
    fprintf(f, "%d\n", (int) getpid());
    fclose(f);
    proxyLogInfo("Pidfile: '%s'", config.pidfile);
}

int main(int argc, char **argv) {
    int exit_status = 0, i;
    signal(SIGPIPE, SIG_IGN);
    setupSignalHandlers();
    uname(&proxy_os);
    ae_api_kqueue = (strcmp("kqueue", aeGetApiName()) == 0);
    thread_id = PROXY_MAIN_THREAD_ID;
    initConfig();
    proxy.configfile = NULL;
    proxy.threads = NULL;
    int parsed_opts = parseOptions(argc, argv);
    checkConfig();
    while (parsed_opts < argc) {
        if (config.entry_points_count >= MAX_ENTRY_POINTS) break;
        char *addr = argv[parsed_opts++];
        redisClusterEntryPoint *entry_point =
            &(config.entry_points[config.entry_points_count++]);
        if (!parseAddress(addr, entry_point)) {
            proxyLogWarn("Invalid address '%s'", addr);
            config.entry_points_count--;
        }
    }
    if (config.entry_points_count == 0) {
        fprintf(stderr, "FATAL: Missing cluster address.\n\n");
        printHelp();
        return 1;
    }
    char *versiontype = "";
    if (strcmp("999.999.999", REDIS_CLUSTER_PROXY_VERSION) == 0)
        versiontype = " (unstable)";
    proxyLogHdr("Redis Cluster Proxy v%s%s",
        REDIS_CLUSTER_PROXY_VERSION, versiontype);
    proxyLogHdr("Commit: (%s/%d)", redisClusterProxyGitSHA1(),
        strtol(redisClusterProxyGitDirty(), NULL, 10) > 0);
    char *gitbranch = redisClusterProxyGitBranch();
    if (strlen(gitbranch) > 0)
        proxyLogHdr("Git Branch: %s", gitbranch);
    proxyLogHdr("PID: %d", (int) getpid());
    proxyLogHdr("OS: %s %s %s",
        proxy_os.sysname, proxy_os.release, proxy_os.machine);
    proxyLogHdr("Bits: %d", (sizeof(long) == 4 ? 32 : 64));
    proxyLogHdr("Log level: %s", redisProxyLogLevels[config.loglevel]);
    if (config.disable_multiplexing == CFG_DISABLE_MULTIPLEXING_ALWAYS)
        proxyLogHdr("Multiplexing disabled by default for every client.");
    if (config.connections_pool.size == 0)
        proxyLogHdr("Connections pool: disabled");
    else {
        proxyLogHdr("Connections pool size: %d (respawn %d every %dms "
                    "if below %d)",
                    config.connections_pool.size,
                    config.connections_pool.spawn_rate,
                    config.connections_pool.spawn_every,
                    config.connections_pool.min_size);
    }
    if (config.auth_user != NULL) {
        if (strcmp("default", config.auth_user) == 0) config.auth_user = NULL;
        else if (config.auth == NULL) config.auth = "";
    }
    proxy.unixsocket_fd = -1;
    proxy.tcp_backlog = config.tcp_backlog;
    checkTcpBacklogSettings();
    if (!listen()) {
        exit_status = 1;
        goto cleanup;
    }
    if (config.daemonize) {
        daemonize();
        createPidFile();
    } else if (config.pidfile != NULL) createPidFile();
    initProxy();
    for (i = 0; i < proxy.fd_count; i++) {
        if (!installIOHandler(proxy.main_loop, proxy.fds[i], AE_READABLE,
                proxy.fds[i] == proxy.unixsocket_fd ? acceptUnixHandler :
                              acceptTcpHandler, NULL, 0))
        {
            proxyLogErr("FATAL: Failed to create %s accept handlers, "
                        "aborting...", proxy.fds[i] == proxy.unixsocket_fd ?
                        "Unix socket" : "TCP");
            exit_status = 1;
            goto cleanup;
        }
    }
    proxy.main_thread = pthread_self();
    proxy.start_time = time(NULL);
    aeMain(proxy.main_loop);
cleanup:
    proxyLogHdr("Redis Cluster Proxy is going to exit...");
    if (proxy.threads != NULL) {
        for (i = 0; i < config.num_threads; i++)
            sendStopMessageToThread(proxy.threads[i]);
        for (i = 0; i < config.num_threads; i++)
            pthread_join(proxy.threads[i]->thread, NULL);
        proxyLogHdr("All thread(s) stopped.");
    }
    releaseProxy();
    proxyLogHdr("Bye bye!");
    if (config.unixsocket) zfree(config.unixsocket);
    if (config.pidfile) zfree(config.pidfile);
    if (config.logfile) zfree(config.logfile);
    if (config.auth) zfree(config.auth);
    if (config.auth_user) zfree(config.auth_user);
    freeEntryPoints(config.entry_points, config.entry_points_count);
    return exit_status;
}
