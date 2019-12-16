# Redis Cluster Proxy

Redis Cluster Proxy is a proxy for [Redis](https://redis.io/) Clusters.
Redis has the ability to run in Cluster mode, where a set of Redis instances will take care of failover and partitioning. This special mode requires to use special clients understanding the Cluster protocol: by using this Proxy instead the Cluster is abstracted away, and you can talk with a set of instances composing a Redis Cluster like if they were a single instance.
Redis Cluster Proxy is multi-threaded and it currently uses, by default, a multiplexing communication model so that every thread has its own connection to the cluster that is shared to all clients belonging to the thread itself.
Anyway, in some special cases (ie. `MULTI` transactions or blocking commands), the multiplexing gets disabled and the client will have its own cluster connection.
In this way clients just sending trivial commands like GETs and SETs will
not require a private set of connections to the Redis Cluster.

So, these are the main features of Redis Cluster Proxy:

- Routing: every query is automatically routed to the correct node of the cluster
- Multithreaded
- Both multiplexing and private connection models supported
- Query execution and reply order are guaranteed even in multiplexing contexts
- Automatic cluster configuration update after `ASK|MOVED` errors: when those kinds of error replies occur, the proxy automatically updates its internal representation of the cluster by fetching an updated configuration of it and remapping  all the slots. All queries are re-executed after the update is completed, so that, from the client's point-of-view, everything flows as normal (the clients don't receive the ASK|MOVED error: they will directly receive the replies after the cluster configuration has been updated).
- Cross-slot/Cross-node queries: many commands involving more keys belonging to different slots (or even different cluster nodes) are supported. Those commands will split the query into multiple queries that will be routed to different slots/nodes. Reply handling for those commands is specific.  Some commands, such as `MGET`, will merge all the replies as if they were a single reply. Other commands such as `MSET` or `DEL` will sum the result of all the replies. Since those commands actually break the atomicity of the query, they'll be made optional (disabled by default). See below for more info.
- Some commands with no specific node/slot such as `DBSIZE` are delivered to all the nodes and the replies will be map-reduced in order to give a sum of all the values replied.
- Additional `PROXY` command that can be used to perform some proxy-specific actions.

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

# Install

In order to install Redis Cluster Proxy into /usr/local/bin just use:

`% make install`

You can use make PREFIX=/some/other/directory install if you wish to use a different destination.

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

You can also use a configuration file instead of passing arguments by using the `-c` options, ie:

`redis-cluster-proxy -c /path/to/my/proxy.conf 127.0.0.1:7000`

After launching it, you can connect to the proxy as if it were a normal Redis server (however make sure to understand the current limitations).

You can then connect to Redis Cluster Proxy using the client you prefer, ie:

`redis-cli -p 7777`

# Enabling Cross-slots queries

Cross-slots queries are queries using keys belonging to different slots or even different nodes.
Since their execution is not guaranteed to be atomic (so, they can actually break the atomic design of many Redis commands), they are disabled by default.
Anyway, if you don't mind about atomicity and you want this feature, you can turn them on when you launch the proxy by using the `--enable-cross-slot`, or by setting `enable-cross-slot yes` into your config file. You can also activate this feature while the proxy is running by using the special `PROXY` command (see below).

**Note**: cross-slots queries are not supported by all the commands, even if the feature is turned on (ie. you cannot use it with `EVAL` or `ZUNIONSTORE` and many other commands). In that case, you'll receive a specific error reply. You can fetch a list of commands that cannot be used in cross-slots queries by using the `PROXY` command (see below).

# The PROXY command

The `PROXY` command will allow to get specific info or perform actions that are specific to the proxy. The command has various subcommands, here's a little list:

- PROXY CONFIG GET|SET option [value]

  It can be used to get or set a specific option of the proxy, where the options
  are the same used in the command line arguments (without the `--` prefix) or specified in the config file.
  Not all the options can be changed (some of them, ie. `threads`, are read-only).
  
  Examples:

  ```
  PROXY CONFIG GET threads
  PROXY CONFIG SET log-level debug
  PROXT CONFIG SET enable-cross-slot 1
  ```
- PROXY MULTIPLEXING STATUS|OFF

  Get the status of multiplexing connection model for the calling client,
  or disable multiplexing by activating a private connection for the client.
  Examples:

  ```
  -> PROXY MULTIPLEXING STATUS
  -> Reply: "on"
  -> PROXY MULTIPLEXING off
  ```

- PROXY INFO

  Returns info specific to the cluster, in a similar fashion of the `INFO` command in Redis.

- PROXY COMMAND [UNSUPPORTED|CROSSSLOTS-UNSUPPORTED]

  Returns a list of all the Redis commands handled (known) 
  by Redis Cluster Proxy, in a similar fashion to Redis `COMMAND` function.
  The returned reply is a nested Array: every command will be an item of the 
  top-level array and it will be an array itself, containing the following 
  items: command name, arity, first key, last key, key step, supported.
  The last item ("supported") indicates whether the command is currently 
  supported by the proxy.

  The optional third argument can be used to specify a filter, in this case:
  - `UNSUPPORTED`: only lists unsupported commands
  - `CROSSSLOTS-UNSUPPORTED`: only lists commands that cannot be used with 
  cross-slots queries, even if they've been enabled in the proxy's configuration.

- PROXY CLIENT <subcmd>

  Perform client specific actions, ie:

    - `PROXY CLIENT ID`
    
    Get current client's internal ID

    - `PROXY CLIENT THREAD`
    
    Get current client's thread

- PROXY LOG <level> MESSAGE

    Log `MESSAGE` to Proxy's log, for debugging purpose.

    The optional `level` can be used to define the log level:

    debug, info, success, warning, error (default is `debug`)

- PROXY HELP

    Get help for the PROXY command


# Current status

This project is currently alpha code that is indented to be evaluated by the community in order to get suggestions and contributions. We discourage its usage in any production environment.
