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

#include "reply_order.h"
#include "logger.h"
#include "endianconv.h"

void addUnorderedReply(client *c, sds reply, uint64_t req_id) {
    uint64_t be_id = htonu64(req_id); /* Big-endian request ID */
    raxInsert(c->unordered_requests, (unsigned char *) &be_id,
              sizeof(be_id), reply, NULL);
}

int appendUnorderedRepliesToBuffer(client *c) {
    raxIterator iter;
    raxStart(&iter, c->unordered_requests);
    uint64_t min_id = htonu64(c->min_reply_id);
    if (!raxSeek(&iter, ">=", (unsigned char*) &min_id, sizeof(min_id))) {
        proxyLogDebug("Failed to seek client %llu unordered requests >= "
                      "%llu.\n", c->id, c->min_reply_id);
        raxStop(&iter);
        return -1;
    }
    int count = 0;
    while (raxNext(&iter)) {
        uint64_t req_id = ntohu64(*((uint64_t *)iter.key));
        if (req_id == c->min_reply_id) {
            sds reply = sdsdup((sds) iter.data);
            c->obuf = sdscat(c->obuf, reply);
            c->min_reply_id++;
            count++;
           /* raxRemove(c->unordered_requests, iter.key, iter.key_len, NULL);*/
            sdsfree(reply);
        } else break;
    }
    raxStop(&iter);
    return count;
}
