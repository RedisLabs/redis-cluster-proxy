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

#include "proxy.h"
#include "config.h"
#include "logger.h"
#include "zmalloc.h"
#include "protocol.h"
#include "atomicvar.h"
#include "commands.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <assert.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>

#define DEFAULT_PORT            7777
#define DEFAULT_MAX_CLIENTS     10000000
#define MAX_THREADS             500
#define DEFAULT_THREADS         8
#define DEFAULT_TCP_KEEPALIVE   300
#define QUERY_OFFSETS_MIN_SIZE  10
#define UNSUPPORTED_QUERY_ERR   "This query is currently unsupported by "\
                                "Redis Cluster Proxy"

#define MAX_ACCEPTS         1000
#define NET_IP_STR_LEN      46

#define UNUSED(V) ((void) V)

struct redisClusterConnection;

typedef struct _proxyThread {
    int thread_id;
    pthread_t thread;
    aeEventLoop *loop;
    list *clients;
    struct redisClusterConnection *cluster_connection;
    pthread_mutex_t new_client_mutex;
} proxyThread;

typedef struct {
    client *client;
    sds buffer;
    int argc;
    int num_commands;
    int *offsets;
    int *lengths;
    int offsets_size;
    clusterNode *node;
    redisCommandDef *command;
    size_t written;
} clientRequest;

typedef struct redisClusterConnection {
    list *requests_to_send;
    list *requests_pending;
} redisClusterConnection;


redisClusterProxy proxy;
redisClusterProxyConfig config;

/* Forward declarations. */

static proxyThread *createProxyThread(int index);
static void freeProxyThread(proxyThread *thread);
static void *execProxyThread(void *ptr);
static client *createClient(int fd, char *ip);
static void freeClient(client *c);
void readQuery(aeEventLoop *el, int fd, void *privdata, int mask);
static int writeToClient(client *c);
static void writeToCluster(aeEventLoop *el, int fd, void *privdata, int mask);
static void readClusterReply(aeEventLoop *el, int fd, void *privdata, int mask);
static clusterNode *getRequestNode(clientRequest *req, sds *err);
static void freeRequest(clientRequest *req);

/* Dict Helpers */

static uint64_t dictSdsHash(const void *key);
static int dictSdsKeyCompare(void *privdata, const void *key1,
    const void *key2);
static void dictSdsDestructor(void *privdata, void *val);
static void dictListDestructor(void *privdata, void *val);

static dictType redisCommandsDictType = {
    dictSdsHash,               /* hash function */
    NULL,                      /* key dup */
    NULL,                      /* val dup */
    dictSdsKeyCompare,         /* key compare */
    dictSdsDestructor,         /* key destructor */
    NULL                       /* val destructor */
};

static uint64_t dictSdsHash(const void *key) {
    return dictGenHashFunction((unsigned char*)key, sdslen((char*)key));
}

static int dictSdsKeyCompare(void *privdata, const void *key1, const void *key2)
{
    int l1,l2;
    DICT_NOTUSED(privdata);

    l1 = sdslen((sds)key1);
    l2 = sdslen((sds)key2);
    if (l1 != l2) return 0;
    return memcmp(key1, key2, l1) == 0;
}

static void dictSdsDestructor(void *privdata, void *val)
{
    DICT_NOTUSED(privdata);
    sdsfree(val);
}

void dictListDestructor(void *privdata, void *val)
{
    DICT_NOTUSED(privdata);
    listRelease((list*)val);
}

static int parseAddress(char *address, char **ip, int *port, char **hostsocket)
{
    *ip = NULL;
    *hostsocket = NULL;
    *port = 0;
    char *p = strchr(address, ':');
    if (!p) *hostsocket = address;
    else {
        if (p == address) *ip = "localhost";
        else {
            *p = '\0';
            *ip = address;
        }
        *port = atoi(++p);
        if (!port) return 0;
    }
    return 1;
}

static void printHelp(void) {
    fprintf(stderr, "Usage: redis-cluster-proxy [OPTIONS] "
            "cluster_host:cluster_port\n"
            "  -p, --port <port>    Port (default: %d)\n"
            "  --max-clients <n>    Max clients (default: %d)\n"
            "  --threads <n>        Thread number (default: %d, max: %d)\n"
            "  --tcpkeepalive       TCP Keep Alive (default: %d)\n"
            "  --daemonize          Execute the proxy in background\n"
            "  -a, --auth <passw>   Authentication password\n"
            "  --disable-colors     Disable colorized output\n"
            "  --log-level <level>  Minimum log level: (default: info)\n"
            "                       (debug|info|success|warning|error)\n"
            "  -h, --help         Print this help\n",
            DEFAULT_PORT, DEFAULT_MAX_CLIENTS, DEFAULT_THREADS,
            DEFAULT_TCP_KEEPALIVE, MAX_THREADS);
}

static int parseOptions(int argc, char **argv) {
    int i;
    for (i = 1; i < argc; i++) {
        int lastarg = (i == (argc - 1));
        char *arg = argv[i];
        if ((!strcmp("-p", arg) || !strcmp("--port", arg)) && !lastarg)
            config.port = atoi(argv[++i]);
        else if ((!strcmp(argv[i],"-a") || !strcmp("--auth", arg))&& !lastarg)
            config.auth = argv[++i];
        else if (!strcmp("--disable-colors", arg))
            config.use_colors = 0;
        else if (!strcmp("--daemonize", arg))
            config.daemonize = 1;
        else if (!strcmp("--maxclients", arg) && !lastarg)
            config.maxclients = atoi(argv[++i]);
        else if (!strcmp("--tcpkeepalive", arg) && !lastarg)
            config.tcpkeepalive = atoi(argv[++i]);
        else if (!strcmp("--threads", arg) && !lastarg) {
            config.num_threads = atoi(argv[++i]);
            if (config.num_threads > MAX_THREADS) {
                fprintf(stderr, "Warning: maximum threads allowed: %d\n",
                                MAX_THREADS);
                config.num_threads = MAX_THREADS;
            } else if (config.num_threads < 1) config.num_threads = 1;
        } else if (!strcmp("--log-level", arg) && !lastarg) {
            char *level_name = argv[++i];
            int j = 0, level = -1;
            for (; j < LOGLEVEL_ERROR; j++) {
                if (!strcasecmp(level_name, redisProxyLogLevels[j])) {
                    level = j;
                    break;
                }
            }
            if (level < 0) {
                fprintf(stderr, "Invalid log level '%s', valid levels:\n", arg);
                for (j = 0; j < LOGLEVEL_ERROR; j++) {
                    if (j > 0) fprintf(stderr, ", ");
                    fprintf(stderr, "%s", redisProxyLogLevels[j]);
                }
                fprintf(stderr, "\n");
                exit(1);
            }
            config.loglevel = level;
        } else if (!strcmp("--help", arg)) {
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

static void initConfig(void) {
    config.port = DEFAULT_PORT;
    config.tcpkeepalive = DEFAULT_TCP_KEEPALIVE;
    config.maxclients = DEFAULT_MAX_CLIENTS;
    config.num_threads = DEFAULT_THREADS;
    config.daemonize = 0;
    config.loglevel = LOGLEVEL_INFO;
    config.use_colors = 1;
    config.auth = NULL;
}

static void initProxy(void) {
    int i;
    proxy.numclients = 0;
    /* Populate commands table. */
    proxy.commands = dictCreate(&redisCommandsDictType, NULL);
    int command_count = sizeof(redisCommandTable) / sizeof(redisCommandDef);
    for (i = 0; i < command_count; i++) {
        redisCommandDef *cmd = redisCommandTable + i;
        dictAdd(proxy.commands, sdsnew(cmd->name), cmd);
    }
    proxy.main_loop = aeCreateEventLoop(config.maxclients);
    proxy.threads = zmalloc(config.num_threads *
                            sizeof(proxyThread *));
    if (proxy.threads == NULL) {
        fprintf(stderr, "FATAL: failed to allocate memory for threads.\n");
        exit(1);
    }
    proxyLogInfo("Starting %d threads...\n", config.num_threads);
    for (i = 0; i < config.num_threads; i++) {
        proxyLogDebug("Creating thread %d...\n", i);
        proxy.threads[i] = createProxyThread(i);
        if (proxy.threads[i] == NULL) {
            fprintf(stderr, "FATAL: failed to allocate thread %d.\n", i);
            exit(1);
        }
        pthread_t *t = &(proxy.threads[i]->thread);
        if (pthread_create(t, NULL, execProxyThread, proxy.threads[i])){
            fprintf(stderr, "FATAL: Failed to start thread %d.\n", i);
            exit(1);
        }
        pthread_mutex_init(&(proxy.threads[i]->new_client_mutex), NULL);
    }
    pthread_mutex_init(&(proxy.numclients_mutex), NULL);
}

static void releaseProxy(void) {
    int i;
    if (proxy.main_loop != NULL) {
        aeStop(proxy.main_loop);
        aeDeleteEventLoop(proxy.main_loop);
    }
    if (proxy.threads != NULL) {
        for (i = 0; i < config.num_threads; i++) {
            proxyThread *thread =  proxy.threads[i];
            if (thread) freeProxyThread(thread);
            proxy.threads[i] = NULL;
        }
        zfree(proxy.threads);
    }
    freeCluster(proxy.cluster);
    dictRelease(proxy.commands);
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
    pthread_mutex_lock(&thread->new_client_mutex);
    while ((ln = listNext(&li)) != NULL) {
        client *c = ln->value;
        if (!writeToClient(c)) continue;
        if (c->written > 0 && c->written < sdslen(c->obuf)) {
            if (aeCreateFileEvent(el, c->fd, AE_WRITABLE, writeHandler, c) ==
                AE_OK) {
                c->has_write_handler = 1;
            } else {
                c->has_write_handler = 0;
                proxyLogDebug("Failed to create write handler for client.\n");
            }
        }
    }
    pthread_mutex_unlock(&thread->new_client_mutex);
}

void sendRequestsToCluster(struct aeEventLoop *eventLoop) {
    proxyThread *thread = eventLoop->privdata;
    assert(thread != NULL);
    redisClusterConnection *conn = thread->cluster_connection;
    if (conn == NULL || conn->requests_to_send == NULL ||
        conn->requests_pending == NULL) return;
    listIter li;
    listNode *ln;
    listRewind(conn->requests_to_send, &li);
    while ((ln = listNext(&li)) != NULL) {
        clientRequest *req = ln->value;
    }
}

/* This function gets called every time threads' lopps are entering the
 * main loop of the event driven library, that is, before to sleep
 * for ready file descriptors. */
void beforeThreadSleep(struct aeEventLoop *eventLoop) {
    UNUSED(eventLoop);
    /*sendRequestsToCluster(eventLoop);*/
    writeRepliesToClients(eventLoop);
}


/* Only used to let threads' event loops process new file events. */
static int proxyThreadCron(aeEventLoop *eventLoop, long long id, void *data) {
    UNUSED(eventLoop);
    UNUSED(id);
    UNUSED(data);
    return 1;
}

static redisClusterConnection *createClusterConnection(void) {
    redisClusterConnection *conn = zmalloc(sizeof(*conn));
    if (conn == NULL) return NULL;
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
    listIter li;
    listNode *ln;
    if (conn->requests_pending != NULL) {
        listRewind(conn->requests_pending, &li);
        while ((ln = listNext(&li)) != NULL) {
            clientRequest *req = ln->value;
            freeRequest(req);
        }
        listRelease(conn->requests_pending);
    }
    if (conn->requests_to_send != NULL) {
        listRewind(conn->requests_to_send, &li);
        while ((ln = listNext(&li)) != NULL) {
            clientRequest *req = ln->value;
            freeRequest(req);
        }
        listRelease(conn->requests_to_send);
    }
    zfree(conn);
}

static proxyThread *createProxyThread(int index) {
    proxyThread *thread = zmalloc(sizeof(*thread));
    if (thread == NULL) return NULL;
    thread->thread_id = index;
    thread->clients = listCreate();
    if (thread->clients == NULL) {
        freeProxyThread(thread);
        return NULL;
    }
    thread->cluster_connection = createClusterConnection();
    if (thread->cluster_connection == NULL) {
        freeProxyThread(thread);
        return NULL;
    }
    thread->loop = aeCreateEventLoop(config.maxclients);
    if (thread->loop == NULL) {
        freeProxyThread(thread);
        return NULL;
    }
    thread->loop->privdata = thread;
    aeSetBeforeSleepProc(thread->loop, beforeThreadSleep);
    aeCreateTimeEvent(thread->loop, 1, proxyThreadCron, NULL,NULL);
    return thread;
}

static void freeProxyThread(proxyThread *thread) {
    if (thread->loop != NULL) aeDeleteEventLoop(thread->loop);
    if (thread->cluster_connection != NULL)
        freeClusterConnection(thread->cluster_connection);
    if (thread->clients != NULL) {
        listIter li;
        listNode *ln;
        listRewind(thread->clients, &li);
        while ((ln = listNext(&li)) != NULL) {
            client *c = ln->value;
            freeClient(c);
        }
        listRelease(thread->clients);
        thread->clients = NULL;
    }
    zfree(thread);
}

static client *createClient(int fd, char *ip) {
    client *c = zcalloc(sizeof(*c));
    if (c == NULL) {
        proxyLogErr("Failed to allocate memory for client: %s\n", ip);
        close(fd);
        return NULL;
    }
    c->status = CLIENT_STATUS_NONE;
    c->fd = fd;
    c->ip = sdsnew(ip);
    c->ibuf = sdsempty();
    c->obuf = sdsempty();
    anetNonBlock(NULL, fd);
    anetEnableTcpNoDelay(NULL, fd);
    if (config.tcpkeepalive)
        anetKeepAlive(NULL, fd, config.tcpkeepalive);
    size_t numclients = 0;
    atomicGetIncr(proxy.numclients, numclients, 1);
    c->thread_id = (numclients % config.num_threads);
    proxyThread *thread = proxy.threads[c->thread_id];
    assert(thread != NULL);
    aeEventLoop *el = thread->loop;
    assert(el != NULL);
    pthread_mutex_lock(&thread->new_client_mutex);
    listAddNodeTail(thread->clients, c);
    proxyLogDebug("Client added to thread %d\n", c->thread_id);
    atomicIncr(proxy.numclients, 1);
    if (aeCreateFileEvent(el, fd, AE_READABLE, readQuery, c) == AE_ERR) {
        proxyLogErr("ERROR: Failed to create read query handler for client "
                    "%s\n", ip);
        freeClient(c);
        return NULL;
    }
    c->status = CLIENT_STATUS_LINKED;
    pthread_mutex_unlock(&thread->new_client_mutex);
    return c;
}

static void unlinkClient(client *c) {
    if (c->fd) {
        aeEventLoop *el = getClientLoop(c);
        if (el != NULL) {
            aeDeleteFileEvent(el, c->fd, AE_READABLE);
            aeDeleteFileEvent(el, c->fd, AE_WRITABLE);
            close(c->fd);
        }
    }
    c->status = CLIENT_STATUS_UNLINKED;
}

static void freeClient(client *c) {
    if (c->status != CLIENT_STATUS_UNLINKED) unlinkClient(c);
    if (c->ip != NULL) sdsfree(c->ip);
    if (c->ibuf != NULL) sdsfree(c->ibuf);
    if (c->obuf != NULL) sdsfree(c->obuf);
    zfree(c);
}

static int writeToClient(client *c) {
    int success = 1, buflen = sdslen(c->obuf), nwritten  =0;
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
            freeClient(c);
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
            aeDeleteFileEvent(el, c->fd, AE_READABLE);
            c->has_write_handler = 0;
        }
    }
    return success;
}

static void writeToCluster(aeEventLoop *el, int fd, void *privdata, int mask) {
    UNUSED(mask);
    clientRequest *req = privdata;
    proxyThread *thread = el->privdata;
    redisClusterConnection *conn = thread->cluster_connection;
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
            proxyLogDebug("Error writing to cluster: %s", strerror(errno));
            listNode *ln = listSearchKey(conn->requests_to_send, req);
            if (ln) listDelNode(conn->requests_to_send, ln);
            freeRequest(req);
            return;
        }
    }
    /* The whole query has been written, so create the read handler and
     * move the request from requests_to_send to requests_pending. */
    if (req->written == buflen) {
        listNode *ln = listSearchKey(conn->requests_to_send, req);
        aeDeleteFileEvent(el, fd, AE_WRITABLE);
        if (aeCreateFileEvent(el, fd, AE_READABLE, readClusterReply, req) !=
            AE_ERR) {
            listAddNodeTail(conn->requests_pending, req);
        } else {
            /* TODO: handle */ 
        }
        if (ln != NULL) listDelNode(conn->requests_to_send, ln);
    }
}

/* TODO: implement also UNIX socket listener */
static int listen(void) {
    int fd_idx = 0;
    /* Try to use both IPv6 and IPv4 */
    proxy.fds[fd_idx] = anetTcp6Server(proxy.neterr, config.port, NULL,
                                       proxy.tcp_backlog);
    if (proxy.fds[fd_idx] != ANET_ERR)
        anetNonBlock(NULL, proxy.fds[fd_idx++]);
    else if (errno == EAFNOSUPPORT)
        proxyLogWarn("Not listening to IPv6: unsupported\n");

    proxy.fds[fd_idx] = anetTcpServer(proxy.neterr, config.port, NULL,
                                      proxy.tcp_backlog);
    if (proxy.fds[fd_idx] != ANET_ERR)
        anetNonBlock(NULL, proxy.fds[fd_idx++]);
    else if (errno == EAFNOSUPPORT)
        proxyLogWarn("Not listening to IPv4: unsupported\n");
    proxy.fd_count = fd_idx;
    return fd_idx;
}

static int parseRequest(clientRequest *req) {
    int is_multibulk = 0, lf_len = 2, qry_offset = 0, ok = 1, i;
    int buflen = sdslen(req->buffer);
    char *p = req->buffer + qry_offset, *nl = NULL;
    sds line = NULL;
    if (*p == '*') is_multibulk = 1;
    if (req->offsets != NULL) zfree(req->offsets);
    if (req->lengths != NULL) zfree(req->lengths);
    req->offsets = zmalloc(sizeof(int) * QUERY_OFFSETS_MIN_SIZE);
    req->lengths = zmalloc(sizeof(int) * QUERY_OFFSETS_MIN_SIZE);
    req->argc = 0;
    req->offsets_size = QUERY_OFFSETS_MIN_SIZE;
    if (is_multibulk) {
        while (qry_offset < buflen) {
            if (*p == '*') {
                req->num_commands++;
            }
            nl = strchr(++p, '\r');
            if (nl == NULL) {
                proxyLogErr("Failed to parse multibulk query: newline not "
                            "found!\n");
                /* TODO: reply err? */
                ok = 0;
                goto cleanup;
            }
            qry_offset++;
            int len = nl - p;
            if (line != NULL) sdsfree(line);
            line = sdsnewlen(p, len);
            long long lc = atoll(line);
            qry_offset += (len + 2);
            if (qry_offset >= buflen) break; /* TODO: err? */
            p = req->buffer + qry_offset;
            for (i = 0; i < lc; i++) {
                if (*p != '$') {
                    proxyLogErr("Failed to parse multibulk query: '$' not "
                                "found!\n");
                    /* TODO: reply err? */
                    ok = 0;
                    goto cleanup;
                }
                nl = strchr(++p, '\r');
                if (nl == NULL) {
                    proxyLogErr("Failed to parse multibulk query: newline not "
                                "found!\n");
                    /* TODO: reply err? */
                    ok = 0;
                    goto cleanup;
                }
                qry_offset++;
                len = nl - p;
                if (line != NULL) sdsfree(line);
                line = sdsnewlen(p, len);
                int arglen = atoi(line);
                qry_offset += (len + 2);
                if (qry_offset >= buflen) break; /* TODO: err? */
                p = req->buffer + qry_offset;
                if (arglen > 0) {
                    int idx = req->argc++;
                    if (req->argc >= req->offsets_size) {
                        int new_size = req->argc + QUERY_OFFSETS_MIN_SIZE;
                        size_t sz = new_size * sizeof(int);
                        req->offsets = zrealloc(req->offsets, sz);
                        req->lengths = zrealloc(req->lengths, sz);
                        if (req->offsets == NULL || req->lengths == NULL) {
                            ok = 0;
                            proxyLogErr("Failed to reallocate request "
                                        "offsets\n");
                            goto cleanup;
                        }
                        req->offsets_size = new_size;
                    }
                    req->offsets[idx] = p - req->buffer;
                    req->lengths[idx] = arglen;
                    /*DELME*/sds tk = sdsnewlen(p, arglen);printf("ARGV[%d]: '%s'\n", idx, tk); sdsfree(tk);
                    qry_offset += (arglen + 2);
                    if (qry_offset >= buflen) break; /* TODO: err? */
                    p = req->buffer + qry_offset;
                }
            }
        }
        /*addReplyError(req->client, UNSUPPORTED_QUERY_ERR);*/
    } else {
    /*TODO: */
        nl = strchr(p, '\n');
        lf_len = 1;
        if (nl != p && *(nl - 1) == '\r') lf_len++;
    }
cleanup:
    if (line != NULL) sdsfree(line);
    return ok;
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

static clusterNode *getRequestNode(clientRequest *req, sds *err) {
    clusterNode *node = NULL;
    if (req->argc == 1) {
        node = getFirstMappedNode(proxy.cluster);
        req->node = node;
        return node;
    }
    int first_key = req->command->first_key,
        last_key = req->command->last_key,
        key_step = req->command->key_step, i;
    if (first_key == 0) return NULL;
    else if (first_key >= req->argc) first_key = req->argc - 1;
    if (last_key < 0 || last_key >= req->argc) last_key = req->argc - 1;
    if (last_key < first_key) last_key = first_key;
    if (key_step < 1) key_step = 1;
    for (i = first_key; i <= last_key; i += key_step) {
        char *key = req->buffer + req->offsets[i];
        clusterNode *n = getNodeByKey(proxy.cluster, key, req->lengths[i]);
        if (n == NULL) break;
        if (node == NULL) node = n;
        else {
            if (node != n) {
                if (err != NULL) {
                    if (*err != NULL) sdsfree(*err);
                    *err = sdsnew("Queries with keys belonging to "
                                  "different nodes are not supported");
                }
                node = NULL;
                break;
            }
        }
    }
    req->node = node;
    return node;
}

static void freeRequest(clientRequest *req) {
    if (req->buffer != NULL) sdsfree(req->buffer);
    if (req->offsets != NULL) zfree(req->offsets);
    if (req->lengths != NULL) zfree(req->lengths);
    zfree(req);
}

static clientRequest *createRequest(client *c) {
    clientRequest *req = zcalloc(sizeof(*req));
    if (req == NULL) {
        proxyLogErr("ERROR: Failed to allocate request!\n");
        return NULL;
    }
    req->client = c;
    assert(c->ibuf != NULL);
    req->buffer = sdsdup(c->ibuf);
    if (req->buffer == NULL) {
        proxyLogErr("ERROR: Failed to allocate request buffer!\n");
        zfree(req);
        return NULL;
    }
    sdsclear(c->ibuf);
    if (!parseRequest(req)) {
        proxyLogErr("Failed to parse request!\n");
        /* TODO: reply error */
        freeRequest(req);
        return NULL;
    }
    req->command = NULL;
    return req;
}

void readQuery(aeEventLoop *el, int fd, void *privdata, int mask){
    UNUSED(el);
    UNUSED(mask);
    client *c = (client *) privdata;
    int nread, readlen = (1024*16);
    size_t iblen = sdslen(c->ibuf);
    c->ibuf = sdsMakeRoomFor(c->ibuf, readlen);
    nread = read(fd, c->ibuf + iblen, readlen);
    if (nread == -1) {
        if (errno == EAGAIN) {
            return;
        } else {
            proxyLogDebug("Error reading from client %s: %s\n", c->ip,
                          strerror(errno));
            unlinkClient(c);
            return;
        }
    } else if (nread == 0) {
        proxyLogDebug("Client %s closed connection\n", c->ip);
        unlinkClient(c);
        return;
    }
    /* TODO: better implement unlinkClient */
    sdsIncrLen(c->ibuf, nread);
    /*TODO: support max query buffer length */
    clientRequest *req = createRequest(c);
    if (req == NULL) {
        proxyLogErr("Failed to create request\n");
        unlinkClient(c);
        return;
    }
    sds errmsg = NULL;
    if (req->argc == 0) {
        proxyLogDebug("Request with zero arguments\n");
        errmsg = sdsnew("Invalid request");
        goto invalid_request;
    }
    /* Multi command requests are currently unsupported. */
    if (req->num_commands > 1) {
        errmsg = sdsnew("Multi-command requests are not currenlty supported");
        goto invalid_request;
    }
    sds command_name = getRequestCommand(req);
    if (command_name == NULL) {
        proxyLogDebug("Missing command name\n");
        errmsg = sdsnew("Invalid request");
        goto invalid_request;
    }
    redisCommandDef *cmd = dictFetchValue(proxy.commands, command_name);
    /* Unsupported commands:
     * - Commands not defined in redisCommandTable
     * - Commands explictly having unsupported to 1
     * - Commands without explicit first_key offset */
    if (cmd == NULL || cmd->unsupported ||
        (cmd->arity != 1 && !cmd->first_key)){
        errmsg = sdsnew("Unsupported command: ");
        errmsg = sdscatfmt(errmsg, "'%s'", command_name);
        goto invalid_request;
    }
    req->command = cmd;
    clusterNode *node = getRequestNode(req, &errmsg);
    if (node == NULL) {
        if (errmsg == NULL)
            errmsg = sdsnew("Failed to get node for query");
        proxyLogDebug("%s\n", errmsg);
        goto invalid_request;
    }
    proxyThread *thread = el->privdata;
    assert(thread != NULL);
    redisClusterConnection *conn = thread->cluster_connection;
    assert(conn != NULL);
    if (node->context == NULL) {
        if (!clusterNodeConnectAtomic(req->node)) {
            addReplyError(c, "Could not connect to node");
            errmsg = sdsnew("Failed to connect to node ");
            errmsg = sdscatprintf(errmsg, "%s:%d", node->ip, node->port);
            proxyLogDebug("%s\n", errmsg);
            goto invalid_request;
        }
    }
    if (aeCreateFileEvent(el, node->context->fd, AE_WRITABLE,
                          writeToCluster, req) == AE_ERR) {
        addReplyError(c, "Failed to write to cluster\n");
        proxyLogErr("Failed to create write handler for request\n");
        freeRequest(req);
        return;
    }
    listAddNodeTail(conn->requests_to_send, req);
    return;
invalid_request:
    if (errmsg != NULL) {
        addReplyError(c, (char *) errmsg);
        sdsfree(errmsg);
    }
    freeRequest(req);
}

static void acceptHandler(int fd, char *ip) {
    client *c = createClient(fd, ip);
    if (c == NULL) return;
    proxyLogDebug("Client connected from %s\n", ip);
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
                proxyLogWarn("Accepting client connection: %s\n",
                             proxy.neterr);
            return;
        }
        proxyLogDebug("Accepted connection from %s:%d\n", client_ip,
                      client_port);
        acceptHandler(client_fd, client_ip);
    }
}

static void readClusterReply(aeEventLoop *el, int fd,
                             void *privdata, int mask)
{
    UNUSED(mask);
    clientRequest *req = privdata;
    proxyThread *thread = el->privdata;
    redisClusterConnection *conn = thread->cluster_connection;
    redisContext *ctx = req->node->context;
    char *errmsg = NULL;
    void *_reply = NULL;
    redisReply *reply = NULL;
    int success = (redisBufferRead(ctx) == REDIS_OK), retry = 0;
    if (!success) {
        int err = ctx->err;
        if (err & (REDIS_ERR_IO | REDIS_ERR_EOF)) {
            /* Try to reconnect to the node */
            if (!clusterNodeConnectAtomic(req->node))
                errmsg = "Cluster node disconnected";
            else {
                req->written = 0;
                retry = 1;
            }
        } else {
            proxyLogErr("Error from node %s:%d: %s\n", req->node->ip,
                        req->node->port, ctx->errstr);
            errmsg = "Failed to read reply";
        }
    } else {
        success = (redisGetReply(ctx, &_reply) == REDIS_OK);
        if (success && _reply == (void*)REDIS_REPLY_ERROR) success = 0;
        if (!success) {
            proxyLogErr("Error: %s\n", ctx->errstr);
            errmsg = "Failed to get reply";
        } else reply = (redisReply *) _reply;
    }
    /* Reply not yet available */
    if (success && reply == NULL) return;
    listNode *ln = listSearchKey(conn->requests_pending, req);
    if (ln) listDelNode(conn->requests_pending, ln);
    aeDeleteFileEvent(el, fd, AE_READABLE);
    if (retry) {
        if (aeCreateFileEvent(el, fd, AE_WRITABLE, writeToCluster, req) !=
            AE_ERR) {
            listAddNodeHead(conn->requests_to_send, req);
        }
    }
    if (errmsg != NULL) addReplyError(req->client, errmsg);
    else {
        char *obuf = ctx->reader->buf;
        size_t len = ctx->reader->len;
        addReplyRaw(req->client, obuf, len);
        /* Consume reader buffer */
        // TODO: atomic
        sdsrange(ctx->reader->buf, ctx->reader->pos, -1);
        ctx->reader->pos = 0;
        ctx->reader->len = sdslen(ctx->reader->buf);
    }
    if (!retry) freeRequest(req);
    freeReplyObject(reply);
}

static void *execProxyThread(void *ptr) {
    proxyThread *thread = (proxyThread *) ptr;
    /* proxyLogDebug("Starting thread %d...\n", thread->thread_id); */
    aeMain(thread->loop);
    return NULL;
}

void daemonize(void) {
    int fd;

    if (fork() != 0) exit(0); /* parent exits */
    setsid(); /* create a new session */

    /* Every output goes to /dev/null. If Redis is daemonized but
     * the 'logfile' is set to 'stdout' in the configuration file
     * it will not log at all. */
    if ((fd = open("/dev/null", O_RDWR, 0)) != -1) {
        dup2(fd, STDIN_FILENO);
        dup2(fd, STDOUT_FILENO);
        dup2(fd, STDERR_FILENO);
        if (fd > STDERR_FILENO) close(fd);
    }
}

int main(int argc, char **argv) {
    int exit_status = 0, i;
    printf("Redis Cluster Proxy v%s\n", REDIS_CLUSTER_PROXY_VERSION);
    initConfig();
    int parsed_opts = parseOptions(argc, argv);
    if (parsed_opts >= argc) {
        fprintf(stderr, "Missing cluster address.\n\n");
        printHelp();
        return 1;
    }
    config.cluster_address = argv[parsed_opts];
    printf("Cluster Address: %s\n", config.cluster_address);
    if (!parseAddress(config.cluster_address, &config.entry_node_host,
                      &config.entry_node_port, &config.entry_node_socket)) {
        fprintf(stderr, "Invalid address '%s'\n", config.cluster_address);
        return 1;
    }
    proxy.cluster = createCluster();
    if (proxy.cluster == NULL) {
        fprintf(stderr, "Failed to allocate memory!\n");
        return 1;
    }
    if (!fetchClusterConfiguration(proxy.cluster, config.entry_node_host,
                                   config.entry_node_port,
                                   config.entry_node_socket)) {
        fprintf(stderr, "Failed to fetch cluster configuration!\n");
        return 1;
    }
    if (config.loglevel == LOGLEVEL_DEBUG) {
        int j;
        clusterNode *last_n = NULL;
        for (j = 0; j < CLUSTER_SLOTS; j++) {
            clusterNode *n = searchNodeBySlot(proxy.cluster, j);
            if (n == NULL) {
                proxyLogErr("NULL node for slot %d\n", j);
                break;
            }
            if (n != last_n) {
                last_n = n;
                proxyLogDebug("Slot %d -> node %d\n", j, n->port);
            }
        }
    }
    int master_count = 0, replica_count = 0;
    listIter li;
    listNode *ln;
    listRewind(proxy.cluster->nodes, &li);
    while ((ln = listNext(&li)) != NULL) {
        clusterNode *n = ln->value;
        if (n->is_replica) replica_count++;
        else master_count++;
    }
    printf("Cluster has %d masters and %d replica(s)\n", master_count,
           replica_count);
    if (!listen()) {
        proxyLogErr("Failed to listen on port %d\n", config.port);
        exit_status = 1;
        goto cleanup;
    }
    printf("Listening on port %d\n", config.port);
    if (config.daemonize) daemonize();
    initProxy();
    for (i = 0; i < proxy.fd_count; i++) {
        if (aeCreateFileEvent(proxy.main_loop, proxy.fds[i], AE_READABLE,
                              acceptTcpHandler, NULL) == AE_ERR) {
            proxyLogErr("FATAL: Failed to create TCP accept handlers, "
                        "aborting...\n");
            exit_status = 1;
            goto cleanup;
        }
    }
    aeMain(proxy.main_loop);
cleanup:
    releaseProxy();
    return exit_status;
}
