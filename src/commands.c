/*
 * Copyright (C) 2019-2020  Giuseppe Fabio Nicotra <artix2 at gmail dot com>
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


#include "commands.h"
#include "zmalloc.h"

/* Command Handlers */
int proxyCommand(void *req);
int multiCommand(void *req);
int execOrDiscardCommand(void *req);
int commandWithPrivateConnection(void *req);
int xreadCommand(void *req);
int securityWarningCommand(void *req);
int pingCommand(void *req);
int authCommand(void *req);
int scanCommand(void *req);

/* Reply Handlers */
int mergeReplies(void *reply, void *request, char *buf, int len);
int getFirstMultipleReply(void *reply, void *request, char *buf, int len);
int sumReplies(void *reply, void *request, char *buf, int len);
int handleScanReply(void *reply, void *request, char *buf, int len);
int getRandomReply(void *reply, void *request, char *buf, int len);

/* Get Keys Callbacks */
int zunionInterGetKeys(void *req, int *first_key, int *last_key,
   int *key_step, int **skip, int *skiplen, char **err);
int xreadGetKeys(void *req, int *first_key, int *last_key,
   int *key_step, int **skip, int *skiplen, char **err);
int sortGetKeys(void *req, int *first_key, int *last_key,
   int *key_step, int **skip, int *skiplen, char **err);
int evalGetKeys(void *req, int *first_key, int *last_key,
   int *key_step, int **skip, int *skiplen, char **err);

struct redisCommandDef redisCommandTable[203] = {
    {"bitpos", -3, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"cluster", -2, 0, 0, 0, 0, 1, NULL, NULL, NULL},
    {"randomkey", 1, 0, 0, 0, 0, 0, NULL, NULL, getRandomReply},
    {"georadius", -6, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"sdiff", -2, 1, -1, 1, 0, 0, NULL, NULL, NULL},
    {"flushdb", -1, 0, 0, 0, 0, 0, NULL, NULL, getFirstMultipleReply},
    {"pfmerge", -2, 1, -1, 1, 0, 0, NULL, NULL, NULL},
    {"strlen", 2, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"shutdown", -1, 0, 0, 0, 0, 1, NULL, NULL, NULL},
    {"hello", -2, 0, 0, 0, 0, 1, NULL, NULL, NULL},
    {"hincrby", 4, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"multi", 1, 0, 0, 0, 0, 0, NULL, multiCommand, NULL},
    {"script", -2, 0, 0, 0, 0, 1, NULL, NULL, NULL},
    {"unwatch", 1, 0, 0, 0,
     CMDFLAG_DUPLICATE,
     0, NULL, commandWithPrivateConnection, getFirstMultipleReply},
    {"setbit", 4, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"srem", -3, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"lrem", 4, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"getbit", 3, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"lpop", 2, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"bzpopmin", -3, 1, -2, 1, 0, 0, NULL, commandWithPrivateConnection, NULL},
    {"set", -3, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"persist", 2, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"module", -2, 0, 0, 0, 0, 1, NULL, NULL, NULL},
    {"monitor", 1, 0, 0, 0, 0, 1, NULL, NULL, NULL},
    {"geohash", -2, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"psubscribe", -2, 0, 0, 0, 0, 1, NULL, NULL, NULL},
    {"hget", 3, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"psetex", 4, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"eval", -3, 0, 0, 0, 0, 0, evalGetKeys, NULL, NULL},
    {"rename", 3, 1, 2, 1, 0, 0, NULL, NULL, NULL},
    {"dump", 2, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"pubsub", -2, 0, 0, 0, 0, 1, NULL, NULL, NULL},
    {"unsubscribe", -1, 0, 0, 0, 0, 1, NULL, NULL, NULL},
    {"slowlog", -2, 0, 0, 0, 0, 1, NULL, NULL, NULL},
    {"smove", 4, 1, 2, 1, 0, 0, NULL, NULL, NULL},
    {"xdel", -3, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"ttl", 2, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"zincrby", 4, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"msetnx", -3, 1, -1, 2, 0, 0, NULL, NULL, NULL},
    {"rpoplpush", 3, 1, 2, 1, 0, 0, NULL, NULL, NULL},
    {"setrange", 4, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"bgrewriteaof", 1, 0, 0, 0, 0, 0, NULL, NULL, NULL},
    {"bitcount", -2, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"getset", 3, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"llen", 2, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"zrange", -4, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"xinfo", -2, 2, 2, 1, 0, 0, NULL, NULL, NULL},
    {"zremrangebyscore", 4, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"config", -2, 0, 0, 0, 0, 1, NULL, NULL, NULL},
    {"mget", -2, 1, -1, 1, 0, 0, NULL, NULL, mergeReplies},
    {"host:", -1, 0, 0, 0, 0, 0, NULL, securityWarningCommand, NULL},
    {"restore", -4, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"zlexcount", 4, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"hdel", -3, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"brpoplpush", 4, 1, 2, 1, 0, 0, NULL, NULL, NULL},
    {"sismember", 3, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"bzpopmax", -3, 1, -2, 1, 0, 0, NULL, commandWithPrivateConnection, NULL},
    {"sscan", -3, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"unlink", -2, 1, -1, 1, 0, 0, NULL, NULL, sumReplies},
    {"hsetnx", 4, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"substr", 4, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"hscan", -3, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"watch", -2, 1, -1, 1, 0, 0, NULL, commandWithPrivateConnection, getFirstMultipleReply},
    {"append", 3, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"xadd", -5, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"sinter", -2, 1, -1, 1, 0, 0, NULL, NULL, NULL},
    {"slaveof", 3, 0, 0, 0, 0, 1, NULL, NULL, NULL},
    {"zpopmin", -2, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"lolwut", -1, 0, 0, 0, 0, 0, NULL, NULL, getFirstMultipleReply},
    {"xack", -4, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"get", 2, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"hmset", -4, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"dbsize", 1, 0, 0, 0, 0, 0, NULL, NULL, sumReplies},
    {"sync", 1, 0, 0, 0, 0, 1, NULL, NULL, NULL},
    {"xgroup", -2, 2, 2, 1, 0, 0, NULL, NULL, NULL},
    {"pfcount", -2, 1, -1, 1, 0, 0, NULL, NULL, NULL},
    {"georadiusbymember_ro", -5, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"hmget", -3, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"geodist", -4, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"replicaof", 3, 0, 0, 0, 0, 1, NULL, NULL, NULL},
    {"auth", -2, 0, 0, 0,
     CMDFLAG_DUPLICATE,
     0, NULL, authCommand, getFirstMultipleReply},
    {"incrbyfloat", 3, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"info", -1, 0, 0, 0, 0, 1, NULL, NULL, NULL},
    {"lpush", -3, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"select", 2, 0, 0, 0, 0, 0, NULL, NULL, NULL},
    {"pfadd", -2, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"hkeys", 2, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"sinterstore", -3, 1, -1, 1,
     CMDFLAG_MULTISLOT_UNSUPPORTED,
     0, NULL, NULL, NULL},
    {"migrate", -6, 0, 0, 0, 0, 1, NULL, NULL, NULL},
    {"rpushx", -3, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"pfdebug", -3, 0, 0, 0, 0, 1, NULL, NULL, NULL},
    {"command", -1, 0, 0, 0, 0, 0, NULL, NULL, getFirstMultipleReply},
    {"xpending", -3, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"spop", -2, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"echo", 2, 0, 0, 0, 0, 0, NULL, NULL, getFirstMultipleReply},
    {"exec", 1, 0, 0, 0, 0, 0, NULL, execOrDiscardCommand, NULL},
    {"geoadd", -5, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"readwrite", 1, 0, 0, 0, 0, 1, NULL, NULL, NULL},
    {"touch", -2, 1, -1, 1, 0, 0, NULL, NULL, sumReplies},
    {"expireat", 3, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"zinterstore", -4, 0, 0, 0,
     CMDFLAG_MULTISLOT_UNSUPPORTED,
     0, zunionInterGetKeys, NULL, NULL},
    {"ltrim", 4, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"xtrim", -2, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"move", 3, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"object", -2, 2, 2, 1, 0, 0, NULL, NULL, NULL},
    {"zpopmax", -2, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"zcount", 4, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"hset", -4, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"pexpireat", 3, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"zrem", -3, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"ping", -1, 0, 0, 0, 0, 0, NULL, pingCommand, NULL},
    {"zrevrangebylex", -4, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"flushall", -1, 0, 0, 0, 0, 0, NULL, NULL, getFirstMultipleReply},
    {"subscribe", -2, 0, 0, 0, 0, 1, NULL, NULL, NULL},
    {"evalsha", -3, 0, 0, 0, 0, 0, evalGetKeys, NULL, NULL},
    {"zremrangebyrank", 4, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"publish", 3, 0, 0, 0, 0, 1, NULL, NULL, NULL},
    {"zrevrangebyscore", -4, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"swapdb", 3, 0, 0, 0, 0, 0, NULL, NULL, NULL},
    {"latency", -2, 0, 0, 0, 0, 1, NULL, NULL, NULL},
    {"zscore", 3, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"lset", 4, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"scan", -2, 0, 0, 0,
     CMDFLAG_HANDLE_REPLY,
     0, NULL, scanCommand, handleScanReply},
    {"debug", -2, 0, 0, 0, 0, 1, NULL, NULL, NULL},
    {"zrevrank", 3, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"asking", 1, 0, 0, 0, 0, 1, NULL, NULL, NULL},
    {"zremrangebylex", 4, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"georadiusbymember", -5, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"hlen", 2, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"renamenx", 3, 1, 2, 1, 0, 0, NULL, NULL, NULL},
    {"acl", -2, 0, 0, 0, 0, 1, NULL, NULL, NULL},
    {"hgetall", 2, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"incr", 2, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"punsubscribe", -1, 0, 0, 0, 0, 1, NULL, NULL, NULL},
    {"setnx", 3, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"del", -2, 1, -1, 1, 0, 0, NULL, NULL, sumReplies},
    {"xrange", -4, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"sunionstore", -3, 1, -1, 1,
     CMDFLAG_MULTISLOT_UNSUPPORTED,
     0, NULL, NULL, NULL},
    {"pfselftest", 1, 0, 0, 0, 0, 1, NULL, NULL, NULL},
    {"smembers", 2, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"bitop", -4, 2, -1, 1, 0, 0, NULL, NULL, NULL},
    {"zrank", 3, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"keys", 2, 0, 0, 0,
     CMDFLAG_DUPLICATE,
     0, NULL, NULL, mergeReplies},
    {"pttl", 2, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"xlen", 2, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"georadius_ro", -6, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"xread", -4, 1, 1, 1,
     CMDFLAG_MULTISLOT_UNSUPPORTED,
     0, xreadGetKeys, xreadCommand, NULL},
    {"sunion", -2, 1, -1, 1, 0, 0, NULL, NULL, NULL},
    {"psync", 3, 0, 0, 0, 0, 1, NULL, NULL, NULL},
    {"xrevrange", -4, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"lrange", 4, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"xreadgroup", -7, 1, 1, 1,
     CMDFLAG_MULTISLOT_UNSUPPORTED,
     0, xreadGetKeys, xreadCommand, NULL},
    {"zcard", 2, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"rpop", 2, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"hstrlen", 3, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"lastsave", 1, 0, 0, 0, 0, 0, NULL, NULL, NULL},
    {"setex", 4, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"brpop", -3, 1, -2, 1, 0, 0, NULL, commandWithPrivateConnection, NULL},
    {"time", 1, 0, 0, 0, 0, 1, NULL, NULL, NULL},
    {"zunionstore", -4, 0, 0, 0,
     CMDFLAG_MULTISLOT_UNSUPPORTED,
     0, zunionInterGetKeys, NULL, NULL},
    {"scard", 2, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"role", 1, 0, 0, 0, 0, 1, NULL, NULL, NULL},
    {"expire", 3, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"sadd", -3, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"sdiffstore", -3, 1, -1, 1,
     CMDFLAG_MULTISLOT_UNSUPPORTED,
     0, NULL, NULL, NULL},
    {"post", -1, 0, 0, 0, 0, 0, NULL, securityWarningCommand, NULL},
    {"hincrbyfloat", 4, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"hvals", 2, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"zscan", -3, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"geopos", -2, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"bitfield", -2, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"decrby", 3, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"replconf", -1, 0, 0, 0, 0, 1, NULL, NULL, NULL},
    {"wait", 3, 0, 0, 0, 0, 1, NULL, NULL, NULL},
    {"rpush", -3, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"blpop", -3, 1, -2, 1, 0, 0, NULL, commandWithPrivateConnection, NULL},
    {"zadd", -4, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"hexists", 3, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"save", 1, 0, 0, 0, 0, 0, NULL, NULL, getFirstMultipleReply},
    {"type", 2, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"restore-asking", -4, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"zrevrange", -4, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"zrangebyscore", -4, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"incrby", 3, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"mset", -3, 1, -1, 2, 0, 0, NULL, NULL, getFirstMultipleReply},
    {"discard", 1, 0, 0, 0, 0, 0, NULL, execOrDiscardCommand, NULL},
    {"xclaim", -6, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"decr", 2, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"getrange", 4, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"xsetid", 3, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"lindex", 3, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"readonly", 1, 0, 0, 0, 0, 1, NULL, NULL, NULL},
    {"bgsave", -1, 0, 0, 0, 0, 0, NULL, NULL, getFirstMultipleReply},
    {"sort", -2, 1, 1, 1, 0, 0, sortGetKeys, NULL, NULL},
    {"srandmember", -2, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"zrangebylex", -4, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"linsert", 5, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"lpushx", -3, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    {"client", -2, 0, 0, 0, 0, 1, NULL, NULL, NULL},
    {"memory", -2, 0, 0, 0, 0, 1, NULL, NULL, NULL},
    {"exists", -2, 1, -1, 1, 0, 0, NULL, NULL, sumReplies},
    {"pexpire", 3, 1, 1, 1, 0, 0, NULL, NULL, NULL},
    /* Custom Commands */
    {"proxy", -2, 0, 0, 0, 0, 0, NULL, proxyCommand, NULL}
};

/* Create a new custom proxy command, that is used for module-commands,
 * rename-commands and new commands of redis.
 *
 * Cross-slot queries are unsupported currently becasue they are different
 * for various commands to hanle their replies. */
redisCommandDef *createProxyCustomCommand(char *name, int arity,
                      int first_key, int last_key, int key_step)
{
    redisCommandDef *cmd = zcalloc(sizeof(redisCommandDef));
    cmd->name = zstrdup(name);
    cmd->arity = arity;
    cmd->first_key = first_key;
    cmd->last_key = last_key;
    cmd->key_step = key_step;
    cmd->proxy_flags = CMDFLAG_MULTISLOT_UNSUPPORTED;
    cmd->unsupported = 0;
    cmd->get_keys = NULL;
    cmd->handle = NULL;
    cmd->handleReply = NULL;

    return cmd;
}
