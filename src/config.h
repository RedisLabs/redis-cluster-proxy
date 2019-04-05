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

typedef struct {
    int port;
    char *cluster_address;
    char *entry_node_host;
    int entry_node_port;
    char *entry_node_socket; /* UNIX Socket */
    int tcpkeepalive;
    int maxclients;
    int num_threads;
    int daemonize;
    int loglevel;
    int use_colors;
    int dump_queries;
    int dump_buffer;
    char *auth;
} redisClusterProxyConfig;

extern redisClusterProxyConfig config;

#endif /* __REDIS_CLUSTER_PROXY_CONFIG_H__ */
