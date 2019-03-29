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
#include "protocol.h"
#include "sds.h"

void addReplyStringLen(client *c, const char *str, int len) {
    sds r = sdsnew("+");
    r = sdscatlen(r, str, len);
    r = sdscat(r, "\r\n");
    c->obuf = sdscat(c->obuf, r);
    sdsfree(r);
}

void addReplyString(client *c, const char *str) {
    addReplyStringLen(c, str, strlen(str));
}

void addReplyInt(client *c, int64_t integer) {
    sds r = sdsnew(":");
    r = sdscatfmt(r, "%U\r\n", integer);
    c->obuf = sdscat(c->obuf, r);
    sdsfree(r);
}

void addReplyErrorLen(client *c, const char *err, int len) {
    sds r = sdsnew("-ERR");
    if (len) {
        r = sdscat(r, " ");
        r = sdscatlen(r, err, len);
    }
    r = sdscat(r, "\r\n");
    c->obuf = sdscat(c->obuf, r);
    sdsfree(r);
}

void addReplyError(client *c, const char *err) {
    addReplyErrorLen(c, err, strlen(err));
}

void addReplyRaw(client *c, const char *buf, size_t len) {
    c->obuf = sdscatlen(c->obuf, buf, len);
}
