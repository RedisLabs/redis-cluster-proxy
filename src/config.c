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

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <strings.h>
#include "config.h"
#include "sds.h"
#include "zmalloc.h"
#include "logger.h"

#define CONFIG_MAX_LINE 1024

void initConfig(void) {
    config.entry_points_count = 0;
    config.port = DEFAULT_PORT;
    config.unixsocket = NULL;
    config.unixsocketperm = DEFAULT_UNIXSOCKETPERM;
    config.tcpkeepalive = DEFAULT_TCP_KEEPALIVE;
    config.max_clients = DEFAULT_MAX_CLIENTS;
    config.num_threads = DEFAULT_THREADS;
    config.tcp_backlog = DEFAULT_TCP_BACKLOG;
    config.daemonize = 0;
    config.loglevel = LOGLEVEL_INFO;
    config.use_colors = 0;
    config.dump_queries = 0;
    config.dump_buffer = 0;
    config.dump_queues = 0;
    config.auth = NULL;
    config.auth_user = NULL;
    config.cross_slot_enabled = 0;
    config.bindaddr_count = 0;
    config.pidfile = NULL;
    config.logfile = NULL;
    config.connections_pool.size = DEFAULT_CONNECTIONS_POOL_SIZE;
    config.connections_pool.min_size = DEFAULT_CONNECTIONS_POOL_MINSIZE;
    config.connections_pool.spawn_every = DEFAULT_CONNECTIONS_POOL_INTERVAL;
    config.connections_pool.spawn_rate = DEFAULT_CONNECTIONS_POOL_SPAWNRATE;
}

int parseAddress(char *address, redisClusterEntryPoint *entry_point) {
    entry_point->host = NULL;
    entry_point->socket = NULL;
    entry_point->port = 0;
    entry_point->address = zstrdup(address);
    int size = strlen(address);
    char *p = strchr(address, ':');
    if (!p) entry_point->socket = zstrdup(address);
    else {
        if (p == address) entry_point->host = zstrdup("localhost");
        else {
            *p = '\0';
            entry_point->host = zstrdup(address);
        }
        if (p - address != size) entry_point->port = atoi(++p);
        if (!entry_point->port) return 0;
    }
    return 1;
}

int parseOptionsFromFile(const char *filename) {
    FILE *f;
    if (filename[0] == '-' || filename[0] == '\0') f = stdin;
    else {
        f = fopen(filename, "r");
        if (f == NULL) {
            fprintf(stderr, "Failed to open config file: '%s'\n", filename);
            return 0;
        }
    }
    int buflen = CONFIG_MAX_LINE + 1;
    char buf[buflen];
    int argc = 1, i = 0, linenum = 0, success = 1;
    sds *argv = zmalloc(sizeof(sds));
    success = (argv != NULL);
    if (!success) goto cleanup;
    /* Insert an empty string since parseOptions always starts from index 1 */
    argv[0] = sdsempty();
    while (fgets(buf, buflen, f) != NULL) {
        linenum++;
        int numtokens = 0;
        sds *tokens = NULL;
        sds line = sdstrim(sdsnew(buf),"\r\n\t ");
        /* Search for comments */
        char *comment_start = strchr(line, '#');
        if (comment_start != NULL) *comment_start = '\0';
        sdsupdatelen(line);
        if (sdslen(line) == 0) goto next_line;
        tokens = sdssplitargs(line, &numtokens);
        if (tokens == NULL || numtokens == 0) {
            proxyLogWarn("Failed to parse line %d in config file '%s'",
                         linenum, filename);
            goto next_line;
        }
        sdstolower(tokens[0]);
        /* Ignore single char options (ie. 'p' for '-p')*/
        if (sdslen(tokens[0]) <= 0) goto next_line;
        int handled = 0;
        if (strcmp("include", tokens[0]) == 0) {
            success = numtokens > 1;
            if (!success) {
                fprintf(stderr, "Error in config file '%s', at line %d:\n"
                        "Mandatory FILENAME argument for "
                        "'include' directive\n", filename, linenum);
                goto cleanup;
            }
            char *configfile = tokens[1];
            success = parseOptionsFromFile(configfile);
            if (!success) goto cleanup;
            handled = 1;
        } else if (strcmp("cluster", tokens[0]) == 0 ||
                   strcmp("entry-point", tokens[0]) == 0)
        {
            success = numtokens > 1;
            if (!success) {
                fprintf(stderr, "Error in config file '%s', at line %d:\n"
                        "Mandatory ADDRESS argument for "
                        "'%s' option\n", filename, linenum, tokens[0]);
                goto cleanup;
            }
            if (config.entry_points_count >= MAX_ENTRY_POINTS) {
                proxyLogWarn(MAX_ENTRY_POINTS_WARN_MSG,
                    MAX_ENTRY_POINTS, tokens[1]);
                goto next_line;
            }
            redisClusterEntryPoint *entry_point =
                &(config.entry_points[config.entry_points_count++]);
            if (!parseAddress(tokens[1], entry_point)) {
                fprintf(stderr, "Error in config file '%s', at line %d:\n"
                        "Invalid address for '%s' option\n",
                        filename, linenum, tokens[0]);
                config.entry_points_count--;
                goto next_line;
            }
            handled = 1;
        } else if (strcmp("help", tokens[0]) == 0) goto next_line;
        if (handled) goto next_line;
        int from = argc;
        if (numtokens > 1) {
            int yesno = 0;
            if (strcasecmp("yes", tokens[1]) == 0) {
                argc += 1;
                argv = zrealloc(argv, argc * sizeof(sds));
                argv[from] = sdscat(sdsnew("--"), tokens[0]);
                yesno = 1;
            } else if (strcasecmp("no", tokens[1]) == 0) yesno = 1;
            if (yesno) goto next_line;
        }
        argc += numtokens;
        argv = zrealloc(argv, argc * sizeof(sds));
        for (i = 0; i < numtokens; i++) {
            sds token = tokens[i];
            if (i == 0) {
                /* Prepend the "--" to the first token, since it will be
                 * parsed by the standard pardeOptions function */
                token = sdscat(sdsnew("--"), token);
            } else token = sdsdup(token);
            argv[from + i] = token;
        }
next_line:
        if (line) sdsfree(line);
        if (tokens) {
            for (i = 0; i < numtokens; i++) sdsfree(tokens[i]);
            zfree(tokens);
        }
    }
    if (argc > 1) parseOptions(argc, argv);
cleanup:
    if (f != stdin) fclose(f);
    if (argv) {
        for (i = 0; i < argc; i++) sdsfree(argv[i]);
        zfree(argv);
    }
    return success;
}

void checkConfig(void) {
    if (config.logfile != NULL) {
        /* If the logfile is an empty string, set it NULL and use STDOUT */
        if (config.logfile[0] == '\0') {
            zfree(config.logfile);
            config.logfile = NULL;
        } else config.use_colors = 0;
    }
    if (config.connections_pool.size > MAX_POOL_SIZE) {
        config.connections_pool.size = MAX_POOL_SIZE;
        proxyLogWarn("Limiting connections-pool-size to max. %d\n",
            MAX_POOL_SIZE);
    }
    if (config.connections_pool.min_size > config.connections_pool.size)
        config.connections_pool.min_size = config.connections_pool.size;
    if (config.connections_pool.spawn_every < 0)
        config.connections_pool.spawn_every = 0;
}
