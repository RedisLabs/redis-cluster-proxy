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

#ifndef __REDIS_CLUSTER_PROXY_CONFIG_H__
#define __REDIS_CLUSTER_PROXY_CONFIG_H__

#include "redis_config.h"

#define CFG_DISABLE_MULTIPLEXING_AUTO       1
#define CFG_DISABLE_MULTIPLEXING_ALWAYS     2
#define BINDADDR_MAX                        16
#define DEFAULT_PID_FILE                    "/var/run/redis-cluster-proxy.pid"
#define DEFAULT_PORT                        7777
#define DEFAULT_UNIXSOCKETPERM              0
#define DEFAULT_MAX_CLIENTS                 10000
#define DEFAULT_THREADS                     8
#define DEFAULT_TCP_KEEPALIVE               300
#define DEFAULT_TCP_BACKLOG                 511
#define DEFAULT_CONNECTIONS_POOL_SIZE       10

typedef struct {
    int port;
    char *unixsocket;
    mode_t unixsocketperm;
    char *cluster_address;
    char *entry_node_host;
    int entry_node_port;
    char *entry_node_socket; /* UNIX Socket */
    int tcpkeepalive;
    int maxclients;
    int num_threads;
    int tcp_backlog;
    int daemonize;
    int loglevel;
    int use_colors;
    int dump_queries;
    int dump_buffer;
    int dump_queues;
    char *auth;
    char *auth_user;
    int disable_multiplexing;
    int cross_slot_enabled;
    int bindaddr_count;
    char *bindaddr[BINDADDR_MAX];
    char *pidfile;
    char *logfile;
    int connections_pool_size;
} redisClusterProxyConfig;

extern redisClusterProxyConfig config;
void initConfig(void);
int parseOptions(int argc, char **argv);
int parseOptionsFromFile(const char *filename);

#endif /* __REDIS_CLUSTER_PROXY_CONFIG_H__ */
