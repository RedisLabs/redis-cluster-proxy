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
#include "sds.h"
#include "proxy.h"

const char *redisProxyLogLevels[5] = {
    "debug",
    "info",
    "success",
    "warning",
    "error"
};

void proxyLog(int level, const char* format, ...) {
    if (level < config.loglevel) return;
    int is_raw = 0, use_colors = config.use_colors;
    if ((is_raw = (level & LOG_RAW))) {
        level &= ~(LOG_RAW);
        use_colors = 0;
    }
    FILE *out = NULL;
    if (config.logfile != NULL) {
        out = fopen(config.logfile, "a");
        if (out == NULL) return;
    }
    if (out == NULL) out = stdout;
    time_t t;
    char smallbuf[256];
    char *buf = smallbuf;
    struct tm* tm_info;
    struct timeval tv;
    int offset = 0, before_format_offset;
    size_t maxlen = sizeof(smallbuf) - 1;
    gettimeofday(&tv,NULL);
    time(&t);
    tm_info = localtime(&t);
    if (use_colors) {
        int color = LOG_COLOR_DEFAULT;
        switch (level) {
        case LOGLEVEL_DEBUG: color = LOG_COLOR_GRAY; break;
        case LOGLEVEL_INFO: color = LOG_COLOR_DEFAULT; break;
        case LOGLEVEL_SUCCESS: color = LOG_COLOR_GREEN; break;
        case LOGLEVEL_WARNING: color = LOG_COLOR_YELLOW; break;
        case LOGLEVEL_ERROR: color = LOG_COLOR_RED; break;
        }
        offset = sprintf(buf, "\033[%dm", color);
        maxlen -= 4; /* Keep enough space for final "\33[0m"  */
    }
    if (!is_raw) {
        int thread_id = getCurrentThreadID();
        offset += strftime(buf + offset, maxlen - offset,
                           "[%Y-%m-%d %H:%M:%S.", tm_info);
        offset += snprintf(buf + offset, maxlen - offset, "%03d",
                           (int) tv.tv_usec/1000);
        if (thread_id != PROXY_MAIN_THREAD_ID) {
            offset += snprintf(buf + offset, maxlen - offset, "/%d] ",
                thread_id);
        } else {
            offset += snprintf(buf + offset, maxlen - offset, "/M] ");
        }
    }
    before_format_offset = offset;
    va_list ap;
    va_start(ap, format);
    offset += vsnprintf(buf + offset, maxlen - offset, format, ap);
    va_end(ap);
    if ((is_raw || level == LOGLEVEL_DEBUG) && (size_t) offset >= maxlen) {
        offset = before_format_offset;
        buf = sdsnewlen(buf, offset);
        va_start(ap, format);
        buf = sdscatvprintf(buf, format, ap);
        va_end(ap);
        if (use_colors) buf = sdscat(buf, "\33[0m");
    } else if (use_colors) {
        snprintf(buf + offset, maxlen - offset, "\33[0m");
    }
    fprintf(out, "%s", buf);
    fflush(out);
    if (config.logfile != NULL) fclose(out);
    if (buf != smallbuf) sdsfree(buf);
}
