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

#include <stdint.h>
#include <inttypes.h>
#include "reply_order.h"
#include "logger.h"
#include "endianconv.h"

void addUnorderedReply(client *c, sds reply, uint64_t req_id) {
    uint64_t be_id = htonu64(req_id); /* Big-endian request ID */
    sds oldreply = NULL;
    raxInsert(c->unordered_replies, (unsigned char *) &be_id,
              sizeof(be_id), reply, (void **)&oldreply);
    if (oldreply != NULL) {
        proxyLogDebug("WARN: Unordered reply for request %d:%" PRId64 ":%"
            PRId64 " was already set to: '%s', and new reply " " is '%s'",
            c->thread_id, c->id, req_id, oldreply, reply);
        sdsfree(oldreply);
    }
}

int appendUnorderedRepliesToBuffer(client *c) {
    raxIterator iter;
    raxStart(&iter, c->unordered_replies);
    uint64_t min_id = htonu64(c->min_reply_id);
    if (!raxSeek(&iter, ">=", (unsigned char*) &min_id, sizeof(min_id))) {
        proxyLogDebug("Failed to seek client %" PRIu64 " unordered requests "
                      ">= %" PRIu64 ".", c->id, c->min_reply_id);
        raxStop(&iter);
        return -1;
    }
    int count = 0;
    while (raxNext(&iter)) {
        uint64_t req_id = ntohu64(*((uint64_t *)iter.key));
        if (req_id == c->min_reply_id) {
            sds reply = (sds) iter.data;
            c->obuf = sdscatsds(c->obuf, reply);
            c->min_reply_id++;
            count++;
            if (raxRemove(c->unordered_replies, iter.key, iter.key_len, NULL)){
                raxSeek(&iter, ">", iter.key, iter.key_len);
                sdsfree(reply);
            }
        } else break;
    }
    raxStop(&iter);
    return count;
}

sds getUnorderedReplyForRequestWithID(client *c, uint64_t req_id) {
    uint64_t be_id = htonu64(req_id); /* Big-endian request ID */
    sds reply = raxFind(c->unordered_replies, (unsigned char *) &be_id,
        sizeof(be_id));
    if (reply == raxNotFound) return NULL;
    return reply;
}
