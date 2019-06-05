# Redis Cluster Proxy

Redis Cluster Proxy is a proxy for [Redis](https://redis.io/) Clusters.
Redis has the ability to run in Cluster mode, where a set of Redis instances will take care of failover and partitioning. This special mode requires to use special clients understanding the Cluster protocol: by using this Proxy instead the Cluster is abstracted away, and you can talk with a set of instances composing a Redis Cluster like if they were a single instance.
Redis Cluster Proxy is multi-threaded and it currently uses a multiplexing communication model because the project is currently an alpha, so Redis features that require specific connections per client are yet not supported.
In the future, multiplexing will be automatically disabled for specific cases: connections will start multiplexed. Once a given client calls certain redis commands like MULTI, or blocking commands, the Proxy will create a set of private connections for such specific client. In this way clients just sending trivial commands like GETs and SETs will
not require a private set of connections to the Redis Cluster.

# Build

Redis Cluster Proxy should run without issues on most POSIX systems (Linux, macOS/OSX, NetBSD, FreeBSD) and on the same platforms supported by Redis.

**Anyway**, it requires C11 and its **atomic variables**, so please ensure that your compiler is supporting both C11 and atomic variables (`_Atomic`).
As for **GCC**, those features are supported by version 4.9 or later.

In order to build it, just type:

`% make`

If you need a 32 bit binary, use:

`% make 32bit`

If you need a verbose build, use the `V` option:

`% make V=1`

If you need to rebuild dependencies, use:

`% make distclean`

And, finally, if you want to launch tests, just type:

`% make test`

As you can see, the make syntax (but also the output style) is the same used in Redis, so it will be familiar to Redis users.

# Usage

Redis Cluster Proxy attaches itself to an already running Redis cluster.
Binary will be compiled inside the `src` directory.
The basic usage is:

`./redis-cluster-proxy CLUSTER_ADDRESS`

where `CLUSTER_ADDRESS` is the host address of any cluster's instance (we call it the *entry point*), and it can be expressed in the form of an `IP:PORT` for TCP connections, or as UNIX socket by specifying the file name.

For example:

`./redis-cluster-proxy 127.0.0.1:7000`

If you need a basic help, just run it with the canonical `-h` or `--help` option.

`./redis-cluster-proxy -h`

By default, Redis Cluster Port will listen on port 7777, but you can change it with the `-p` or `--port` option.

You can change the number of threads using the `--threads` option.

After launching it, you can connect to the proxy as if it were a normal Redis server (however make sure to understand the current limitations).

# Install

In order to install Redis Cluster Proxy into /usr/local/bin just use:

`% make install`

You can use make PREFIX=/some/other/directory install if you wish to use a different destination.

# Supported features and commands

Redis Cluster Proxy currently supports only single-key commands, so you're free to use commands like GET, SET, LPUSH, RPUSH, LRANGE, SADD, ZADD, ZRANGE, HSET, HMSET, HGET, HGETALL and so on. You can obviously use commands like DEL as long as they're used with a single key. Multi-key/slot commands will be supported soon.

Furthermore, you cannot use commands with no keys that require interaction with a single cluster's instance, such as DBSIZE, PING, CONFIG, and so on.
More complex commands such as MULTI/EXEC/DISCARD or blocking commands are not supported and will be supported in the future.

Pipelined queries are fully supported.

# Features that are still to be implemented in the next versions

- Multi key and multi slot/node commands
- Blocking commands and transactions (MULTI/EXEC)
- Automatic redirection and reconfiguration in case of cluster configuration change (ie after a resharding).

# Current status

This project is currently alpha code that is indented to be evaluated by the community in order to get suggestions and contributions. We discourage its usage in any production environment.
