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
#include <stdarg.h>
#include <time.h>
#include <sys/time.h>

#include "logger.h"
#include "config.h"

const char *redisProxyLogLevels[5] = {
    "debug",
    "info",
    "success",
    "warning",
    "error"
};

void proxyLog(int level, const char* format, ...) {
    if (level < config.loglevel) return;
    FILE *out = NULL;
    if (config.logfile != NULL) {
        out = fopen(config.logfile, "a");
        if (out == NULL) return;
    }
    if (out == NULL) out = stdout;
    fflush(out);
    if (config.use_colors) {
        int color = LOG_COLOR_DEFAULT;
        switch (level) {
        case LOGLEVEL_DEBUG: color = LOG_COLOR_GRAY; break;
        case LOGLEVEL_INFO: color = LOG_COLOR_DEFAULT; break;
        case LOGLEVEL_SUCCESS: color = LOG_COLOR_GREEN; break;
        case LOGLEVEL_WARNING: color = LOG_COLOR_YELLOW; break;
        case LOGLEVEL_ERROR: color = LOG_COLOR_RED; break;
        }
        fprintf(out, "\033[%dm", color);
    }
    time_t t;
    char ts[64];
    struct tm* tm_info;
    struct timeval tv;
    gettimeofday(&tv,NULL);
    time(&t);
    tm_info = localtime(&t);
    int offset = strftime(ts, 26, "%Y-%m-%d %H:%M:%S.", tm_info);
    snprintf(ts + offset, sizeof(ts) - offset, "%03d", (int)tv.tv_usec/1000);
    fprintf(out, "[%s] ", ts);
    va_list ap;
    va_start(ap, format);
    vfprintf(out, format, ap);
    va_end(ap);
    if (config.use_colors) fprintf(out, "\33[0m");
    if (config.logfile != NULL) fclose(out);
}
