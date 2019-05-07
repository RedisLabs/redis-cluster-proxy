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

#include <string.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include "protocol.h"
#include "logger.h"
#include "reply_order.h"
#include "sds.h"

void addReplyStringLen(client *c, const char *str, int len, uint64_t req_id) {
    sds r = sdsnew("+");
    r = sdscatlen(r, str, len);
    r = sdscat(r, "\r\n");
    addReplyRaw(c, (const char*) r, sdslen(r), req_id);
    sdsfree(r);
}

void addReplyString(client *c, const char *str, uint64_t req_id) {
    addReplyStringLen(c, str, strlen(str), req_id);
}

void addReplyInt(client *c, int64_t integer, uint64_t req_id) {
    sds r = sdsnew(":");
    r = sdscatfmt(r, "%U\r\n", integer);
    c->obuf = sdscat(c->obuf, r);
    addReplyRaw(c, (const char*) r, sdslen(r), req_id);
    sdsfree(r);
}

void addReplyErrorLen(client *c, const char *err, int len, uint64_t req_id) {
    sds r = sdsnew("-ERR");
    if (len) {
        r = sdscat(r, " ");
        r = sdscatlen(r, err, len);
    }
    r = sdscat(r, "\r\n");
    addReplyRaw(c, (const char*) r, sdslen(r), req_id);
    sdsfree(r);
}

void addReplyError(client *c, const char *err, uint64_t req_id) {
    addReplyErrorLen(c, err, strlen(err), req_id);
}

void addReplyRaw(client *c, const char *buf, size_t len, uint64_t req_id) {
    /* If the smallest request ID written is smaller than reply's request ID,
     *  replies are not ordered, so add the reply to the unordered_replies rax
     * using the request ID as the key. */
    if (req_id > c->min_reply_id) {
        addUnorderedReply(c, sdsnewlen(buf, len), req_id);
        return;
    }
    c->obuf = sdscatlen(c->obuf, buf, len);
    c->min_reply_id = req_id + 1;
    appendUnorderedRepliesToBuffer(c);
}
