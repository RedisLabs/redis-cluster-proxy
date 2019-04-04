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
#define EL_INSTALL_HANDLER_FAIL 9999
#define REQ_STATUS_UNKNOWN      -1
#define PARSE_STATUS_INCOMPLETE -1
#define PARSE_STATUS_ERROR      0
#define PARSE_STATUS_OK         1

#define MAX_ACCEPTS             1000
#define NET_IP_STR_LEN          46

#define THREAD_IO_READ    0
#define THREAD_IO_WRITE   1

#define THREAD_MSG_NEW_CLIENT   'c'

#define UNUSED(V) ((void) V)

struct redisClusterConnection;

typedef struct _threadMessage {
    char type;
    void *data;
} threadMessage;

typedef struct proxyThread {
    int thread_id;
    int io[2];
    pthread_t thread;
    aeEventLoop *loop;
    list *clients;
    list *pending_messages;
    struct redisClusterConnection *cluster_connection;
    pthread_mutex_t new_message_mutex;
} proxyThread;

typedef struct clientRequest{
    client *client;
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
    clusterNode *node;
    redisCommandDef *command;
    size_t written;
    int parsing_status;
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

/* Hiredis helpers */

/*int processItem(redisReader *r);*/

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
            fprintf(stderr, "FATAL: failed to create thread %d.\n", i);
            exit(1);
        }
        pthread_t *t = &(proxy.threads[i]->thread);
        if (pthread_create(t, NULL, execProxyThread, proxy.threads[i])){
            fprintf(stderr, "FATAL: Failed to start thread %d.\n", i);
            exit(1);
        }
        pthread_mutex_init(&(proxy.threads[i]->new_message_mutex), NULL);
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
    if (proxy.commands)
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

static int processThreadMessage(proxyThread *thread, threadMessage *msg) {
    if (msg->type == THREAD_MSG_NEW_CLIENT) {
        client *c = msg->data;
        aeEventLoop *el = thread->loop;
        assert(el != NULL);
        listAddNodeTail(thread->clients, c);
        proxyLogDebug("Client added to thread %d\n", c->thread_id);
        errno = 0;
        if (aeCreateFileEvent(el, c->fd, AE_READABLE, readQuery, c) == AE_ERR) {
            proxyLogErr("ERROR: Failed to create read query handler for client "
                        "%s\n", c->ip);
            errno = EL_INSTALL_HANDLER_FAIL;
            freeClient(c);
            return 0;
        }
        c->status = CLIENT_STATUS_LINKED;
    }
    return 1;
}

static int processThreadMessages(proxyThread *thread) {
    listIter li;
    listNode *ln;
    int processed = 0;
    pthread_mutex_lock(&(thread->new_message_mutex));
    listRewind(thread->pending_messages, &li);
    while ((ln = listNext(&li)) != NULL) {
        threadMessage *msg = ln->value;
        if (!processThreadMessage(thread, msg)) {
            if (errno != EL_INSTALL_HANDLER_FAIL) continue;
        }
        listDelNode(thread->pending_messages, ln);
        processed++;
    }
    pthread_mutex_unlock(&(thread->new_message_mutex));
    return processed;
}

static void readThreadPipe(aeEventLoop *el, int fd, void *privdata, int mask) {
    UNUSED(el);
    UNUSED(mask);
    proxyThread *thread = privdata;
    int msgcount = 0, processed = 0, nread = 0;
    char buf[2048];
    nread = read(fd, buf + nread, sizeof(buf));
    if (nread == -1) {
        if (errno == EAGAIN) {
            return;
        } else {
            proxyLogDebug("Error reading from thread pipe: %s\n",
                          strerror(errno));
            return;
        }
    }
    msgcount += nread;
    if (msgcount == 0) return;
    processed = processThreadMessages(thread);
}

static proxyThread *createProxyThread(int index) {
    proxyThread *thread = zmalloc(sizeof(*thread));
    if (thread == NULL) return NULL;
    if (pipe(thread->io) == -1) {
        proxyLogErr("ERROR: failed to open pipe for thread!\n");
        zfree(thread);
        return NULL;
    }
    thread->thread_id = index;
    thread->clients = listCreate();
    if (thread->clients == NULL) {
        freeProxyThread(thread);
        return NULL;
    }
    thread->pending_messages = listCreate();
    if (thread->pending_messages == NULL) {
        freeProxyThread(thread);
        return NULL;
    }
    listSetFreeMethod(thread->pending_messages, zfree);
    thread->cluster_connection = createClusterConnection();
    if (thread->cluster_connection == NULL) {
        freeProxyThread(thread);
        return NULL;
    }
    thread->loop = aeCreateEventLoop(config.maxclients + 2);
    if (thread->loop == NULL) {
        freeProxyThread(thread);
        return NULL;
    }
    thread->loop->privdata = thread;
    aeSetBeforeSleepProc(thread->loop, beforeThreadSleep);
    /*aeCreateTimeEvent(thread->loop, 1, proxyThreadCron, NULL,NULL);*/
    if (aeCreateFileEvent(thread->loop, thread->io[THREAD_IO_READ],
                          AE_READABLE, readThreadPipe, thread) == AE_ERR) {
        freeProxyThread(thread);
        return NULL;
    }
    return thread;
}

static threadMessage *createThreadMessage(char type, void *data) {
    threadMessage *msg = zmalloc(sizeof(*msg));
    if (msg == NULL) return NULL;
    msg->type = type;
    msg->data = data;
    return msg;
}

static int awakeThread(proxyThread *thread, char msgtype, void *data) {
    threadMessage *msg = createThreadMessage(msgtype, data);
    if (msg == NULL) return 0;
    pthread_mutex_lock(&(thread->new_message_mutex));
    listAddNodeTail(thread->pending_messages, msg);
    pthread_mutex_unlock(&(thread->new_message_mutex));
    int fd = thread->io[THREAD_IO_WRITE];
    int nwritten = write(fd, &msgtype, sizeof(msgtype));
    if (nwritten == -1) {
        /* TODO: try again later */
        return 0;
    }
    return 1;
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
    if (thread->pending_messages != NULL) {
        listRelease(thread->pending_messages);
    }
    if (thread->io[0]) close(thread->io[0]);
    if (thread->io[1]) close(thread->io[1]);
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
    c->obuf = sdsempty();
    anetNonBlock(NULL, fd);
    anetEnableTcpNoDelay(NULL, fd);
    if (config.tcpkeepalive)
        anetKeepAlive(NULL, fd, config.tcpkeepalive);
    size_t numclients = 0;
    atomicGetIncr(proxy.numclients, numclients, 1);
    /* TODO: select thread with less clients */
    c->thread_id = (numclients % config.num_threads);
    c->id = numclients;
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
    int thread_id = c->thread_id;
    proxyThread *thread = proxy.threads[thread_id];
    assert(thread != NULL);
    listNode *ln = listSearchKey(thread->clients, c);
    if (ln != NULL) listDelNode(thread->clients, ln);
    if (c->ip != NULL) sdsfree(c->ip);
    if (c->obuf != NULL) sdsfree(c->obuf);
    if (c->current_request) freeRequest(c->current_request);
    zfree(c);
    atomicIncr(proxy.numclients, -1);
}

static int writeToClient(client *c) {
    int success = 1, buflen = sdslen(c->obuf), nwritten = 0;
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

static int requestMakeRoomForArgs(clientRequest *req, int argc) {
    if (argc >= req->offsets_size) {
        int new_size = argc + QUERY_OFFSETS_MIN_SIZE;
        size_t sz = new_size * sizeof(int);
        req->offsets = zrealloc(req->offsets, sz);
        req->lengths = zrealloc(req->lengths, sz);
        if (req->offsets == NULL || req->lengths == NULL) {
            proxyLogErr("Failed to reallocate request "
                        "offsets\n");
            return 0;
        }
        req->offsets_size = new_size;
    }
    return 1;
}

static int parseRequest(clientRequest *req) {
    int lf_len = 2, status = req->parsing_status, len, i;
    if (status != PARSE_STATUS_INCOMPLETE) return status;
    int buflen = sdslen(req->buffer);
    char *p = req->buffer + req->query_offset, *nl = NULL;
    sds line = NULL;
    /* New request, so request type must be determinded. */
    if (req->is_multibulk == REQ_STATUS_UNKNOWN) {
        if (*p == '*') req->is_multibulk = 1;
        else req->is_multibulk = 0;
    }
    if (req->is_multibulk) {
        while (req->query_offset < buflen) {
            if (*p == '*') {
                req->num_commands++;
                req->query_offset++;
                p++;
                req->pending_bulks = REQ_STATUS_UNKNOWN;
                req->current_bulk_length = REQ_STATUS_UNKNOWN;
            }
            if (req->query_offset >= buflen) {
                status = PARSE_STATUS_INCOMPLETE;
                goto cleanup;
            }
            long long lc = req->pending_bulks;
            if (lc == REQ_STATUS_UNKNOWN) {
                nl = strchr(p, '\r');
                if (nl == NULL) {
                    status = PARSE_STATUS_INCOMPLETE;
                    goto cleanup;
                }
                int len = nl - p;
                if (line != NULL) sdsfree(line);
                line = sdsnewlen(p, len);
                lc = atoll(line);
                if (lc < 0) lc = 0;
                req->query_offset += (len + 2);
                req->pending_bulks = lc;
                if (req->query_offset >= buflen) {
                    status = PARSE_STATUS_INCOMPLETE;
                    goto cleanup;
                }
                p = req->buffer + req->query_offset;
            }
            for (i = 0; i < lc; i++) {
                int arglen = req->current_bulk_length;
                if (arglen == REQ_STATUS_UNKNOWN) {
                    if (*p != '$') {
                        proxyLogErr("Failed to parse multibulk query: '$' not "
                                    "found!\n");
                        status = PARSE_STATUS_ERROR;
                        goto cleanup;
                    }
                    if ((req->query_offset + 1) >= buflen) {
                        status = PARSE_STATUS_INCOMPLETE;
                        goto cleanup;
                    }
                    nl = strchr(++p, '\r');
                    if (nl == NULL) {
                        status = PARSE_STATUS_INCOMPLETE;
                        goto cleanup;
                    }
                    len = nl - p;
                    if (line != NULL) sdsfree(line);
                    line = sdsnewlen(p, len);
                    arglen = atoi(line);
                    if (arglen < 0) arglen = 0;
                    req->current_bulk_length = arglen;
                    req->query_offset += (len + 3);
                    if (req->query_offset >= buflen) {
                        status = PARSE_STATUS_INCOMPLETE;
                        goto cleanup;
                    }
                    p = req->buffer + req->query_offset;
                }
                if (arglen > 0) {
                    int newargc = req->argc + 1;
                    if (!requestMakeRoomForArgs(req, newargc)) {
                        status = PARSE_STATUS_ERROR;
                        goto cleanup;
                    }
                    nl = strchr(p, '\r');
                    if (nl == NULL) {
                        status = PARSE_STATUS_INCOMPLETE;
                        goto cleanup;
                    }
                    int endarg = req->query_offset + arglen;
                    if (endarg >= buflen || *(req->buffer+endarg) != '\r') {
                        status = PARSE_STATUS_INCOMPLETE;
                        goto cleanup;
                    }
                    int idx = req->argc++;
                    req->offsets[idx] = p - req->buffer;
                    req->lengths[idx] = arglen;
                    /*DELME*/sds tk = sdsnewlen(p, arglen);printf("ARGV[%d]: '%s'\n", idx, tk); sdsfree(tk);
                    req->pending_bulks--;
                    req->current_bulk_length = REQ_STATUS_UNKNOWN;
                    req->query_offset = endarg + 2;
                    p = req->buffer + req->query_offset;
                }
            }
        }
    } else {
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
        int qrylen = nl - p;
        int remaining = qrylen;
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
    }
cleanup:
    if (req->query_offset > buflen) req->query_offset = buflen;
    int remaining = buflen - req->query_offset;
    if (status == PARSE_STATUS_INCOMPLETE) {
        if (req->is_multibulk && req->pending_bulks <= 0 && remaining == 0)
            status = PARSE_STATUS_OK;
    }
    req->parsing_status = status;
    if (line != NULL) sdsfree(line);
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
    if (req->client->current_request == req)
        req->client->current_request = NULL;
    zfree(req);
}

static clientRequest *createRequest(client *c) {
    clientRequest *req = zcalloc(sizeof(*req));
    if (req == NULL) goto alloc_failure;
    req->client = c;
    req->buffer = sdsempty();
    if (req->buffer ==  NULL) goto alloc_failure;
    req->query_offset = 0;
    req->is_multibulk = REQ_STATUS_UNKNOWN;
    req->pending_bulks = REQ_STATUS_UNKNOWN;
    req->current_bulk_length = REQ_STATUS_UNKNOWN;
    req->parsing_status = REQ_STATUS_UNKNOWN;
    size_t offsets_size = sizeof(int) * QUERY_OFFSETS_MIN_SIZE;
    req->offsets = zmalloc(offsets_size);
    req->lengths = zmalloc(sizeof(int) * QUERY_OFFSETS_MIN_SIZE);
    if (!req->offsets || !req->lengths) goto alloc_failure;
    req->argc = 0;
    req->offsets_size = QUERY_OFFSETS_MIN_SIZE;
    req->command = NULL;
    c->current_request = req;
    return req;
alloc_failure:
    proxyLogErr("ERROR: Failed to allocate request!\n");
    if (!req) return NULL;
    freeRequest(req);
    return NULL;
}

static int processRequest(aeEventLoop *el, clientRequest *req) {
    int status = parseRequest(req);
    if (status == PARSE_STATUS_ERROR) return 0;
    else if (status == PARSE_STATUS_INCOMPLETE) return 1;
    client *c = req->client;
    sds command_name = NULL;
    sds errmsg = NULL;
    if (req->argc == 0) {
        proxyLogDebug("Request with zero arguments\n");
        errmsg = sdsnew("Invalid request");
        goto invalid_request;
    }
    /* Multi command requests are currently unsupported. */
    if (req->num_commands > 1) {
        errmsg = sdsnew("Multi-command requests are not currently supported");
        goto invalid_request;
    }
    command_name = getRequestCommand(req);
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
    redisContext *ctx = getClusterNodeConnection(node, c->thread_id);
    if (ctx == NULL) {
        if ((ctx = clusterNodeConnect(node, c->thread_id)) == NULL) {
            addReplyError(c, "Could not connect to node");
            errmsg = sdsnew("Failed to connect to node ");
            errmsg = sdscatprintf(errmsg, "%s:%d", node->ip, node->port);
            proxyLogDebug("%s\n", errmsg);
            goto invalid_request;
        }
    }
    if (aeCreateFileEvent(el, ctx->fd, AE_WRITABLE,
                          writeToCluster, req) == AE_ERR) {
        addReplyError(c, "Failed to write to cluster\n");
        proxyLogErr("Failed to create write handler for request\n");
        freeRequest(req);
        if (command_name) sdsfree(command_name);
        return 0;
    }
    listAddNodeTail(conn->requests_to_send, req);
    if (command_name) sdsfree(command_name);
    return 1;
invalid_request:
    if (command_name) sdsfree(command_name);
    freeRequest(req);
    if (errmsg != NULL) {
        addReplyError(c, (char *) errmsg);
        sdsfree(errmsg);
        return 1;
    }
    return 0;
}

void readQuery(aeEventLoop *el, int fd, void *privdata, int mask){
    UNUSED(el);
    UNUSED(mask);
    client *c = (client *) privdata;
    int nread, readlen = (1024*16);
    clientRequest *req = c->current_request;
    if (req == NULL) {
        req = createRequest(c);
        if (req == NULL) {
            proxyLogErr("Failed to create request\n");
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
            proxyLogDebug("Error reading from client %s: %s\n", c->ip,
                          strerror(errno));
            unlinkClient(c);
            return;
        }
    } else if (nread == 0) {
        proxyLogDebug("Client %s closed connection\n", c->ip);
        freeClient(c);
        return;
    }
    /* TODO: better implement unlinkClient */
    sdsIncrLen(req->buffer, nread);
    /*TODO: support max query buffer length */
    if (!processRequest(el, req)) freeClient(c);
}

static void acceptHandler(int fd, char *ip) {
    client *c = createClient(fd, ip);
    if (c == NULL) return;
    proxyLogDebug("Client connected from %s\n", ip);
    proxyThread *thread = proxy.threads[c->thread_id];
    assert(thread != NULL);
    if (!awakeThread(thread, THREAD_MSG_NEW_CLIENT, c)) {
        /* TODO: append client to a list of pending clients to be handled
         * by a beforeSleep (which should call awakeThread again*/
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
    redisContext *ctx = getClusterNodeConnection(req->node, thread->thread_id);
    char *errmsg = NULL;
    void *_reply = NULL;
    redisReply *reply = NULL;
    int success = (redisBufferRead(ctx) == REDIS_OK), retry = 0;
    if (!success) {
        int err = ctx->err;
        if (err & (REDIS_ERR_IO | REDIS_ERR_EOF)) {
            /* Try to reconnect to the node */
            if (!(ctx = clusterNodeConnect(req->node, thread->thread_id)))
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
        success =
            (__hiredisReadReplyFromBuffer(ctx->reader, &_reply) == REDIS_OK);
        /*if (success && _reply == (void*)REDIS_REPLY_ERROR) success = 0;*/
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
    proxy.cluster = createCluster(config.num_threads + 1);
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
