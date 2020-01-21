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

#ifndef __REDIS_CLUSTER_PROXY_LOGGER_H__
#define __REDIS_CLUSTER_PROXY_LOGGER_H__

#define LOGLEVEL_DEBUG      0
#define LOGLEVEL_INFO       1
#define LOGLEVEL_SUCCESS    2
#define LOGLEVEL_WARNING    3
#define LOGLEVEL_ERROR      4

#define LOG_COLOR_RED       31
#define LOG_COLOR_GREEN     32
#define LOG_COLOR_YELLOW    33
#define LOG_COLOR_BLUE      34
#define LOG_COLOR_MAGENTA   35
#define LOG_COLOR_CYAN      36
#define LOG_COLOR_GRAY      37
#define LOG_COLOR_DEFAULT   39 /* Default foreground color */

#define proxyLogDebug(...) proxyLog(LOGLEVEL_DEBUG, __VA_ARGS__)
#define proxyLogInfo(...) proxyLog(LOGLEVEL_INFO, __VA_ARGS__)
#define proxyLogSuccess(...) proxyLog(LOGLEVEL_SUCCESS, __VA_ARGS__)
#define proxyLogWarn(...) proxyLog(LOGLEVEL_WARNING, __VA_ARGS__)
#define proxyLogErr(...) proxyLog(LOGLEVEL_ERROR, __VA_ARGS__)
#define proxyLogHdr(...) proxyLog(LOGLEVEL_ERROR, __VA_ARGS__)

extern const char *redisProxyLogLevels[5];

void proxyLog(int level, const char* format, ...);

#endif /* __REDIS_CLUSTER_PROXY_LOGGER_H__ */
