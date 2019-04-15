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


#ifndef __REDIS_CLUSTER_PROXY_COMMANDS_H__
#define __REDIS_CLUSTER_PROXY_COMMANDS_H__

typedef struct redisCommandDef {
    char *name;
    int arity;
    int first_key;
    int last_key;
    int key_step;
    int unsupported;
} redisCommandDef;


extern struct redisCommandDef redisCommandTable[202];

#endif /* __REDIS_CLUSTER_PROXY_COMMANDS_H__  */
