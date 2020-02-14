# Redis Cluster Proxy

Redis Cluster Proxy is a proxy for [Redis](https://redis.io/) Clusters.
Redis has the ability to run in Cluster mode, where a set of Redis instances will take care of failover and partitioning. This special mode requires the use of special clients understanding the Cluster protocol: by using this Proxy instead the Cluster is abstracted away, and you can talk with a set of instances composing a Redis Cluster like if they were a single instance.
Redis Cluster Proxy is multi-threaded and it currently uses, by default, a multiplexing communication model so that every thread has its own connection to the cluster that is shared to all clients belonging to the thread itself.
Anyway, in some special cases (ie. `MULTI` transactions or blocking commands), the multiplexing gets disabled and the client will have its own cluster connection.
In this way clients just sending trivial commands like GETs and SETs will
not require a private set of connections to the Redis Cluster.

So, these are the main features of Redis Cluster Proxy:

- Routing: every query is automatically routed to the correct node of the cluster
- Multithreaded
- Both multiplexing and private connection models supported
- Query execution and reply order are guaranteed even in multiplexing contexts
- Automatic update of the cluster's configuration after `ASK|MOVED` errors: when those kinds of errors occur in replies, the proxy automatically updates its internal representation of the cluster by fetching an updated configuration of it and by remapping all the slots. All queries will be re-executed after the update is completed, so that, from the client's point-of-view, everything flows as normal (the clients won't receive the ASK|MOVED error: they will directly receive the expected replies after the cluster configuration has been updated).
- Cross-slot/Cross-node queries: many commands involving multiple keys belonging to different slots (or even to different cluster nodes) are supported. Those commands will split the query into multiple queries that will be routed to different slots/nodes. Reply handling for those commands is command-specific. Some commands, such as `MGET`, will merge all the replies as if they were a single reply. Other commands such as `MSET` or `DEL` will sum the results of all the replies. Since those queries actually break the atomicity of the command, their usage is optional (disabled by default). See below for more info.
- Some commands with no specific node/slot such as `DBSIZE` are delivered to all the nodes and the replies will be map-reduced in order to give a sum of all the values contained in all the replies.
- The additional `PROXY` command that can be used to perform some proxy-specific actions.

# Build

Redis Cluster Proxy should run without issues on most POSIX systems (Linux, macOS/OSX, NetBSD, FreeBSD) and on the same platforms supported by Redis.

**Anyway**, it requires C11 and its **atomic variables**, so please ensure that your compiler is supporting both C11 and atomic variables (`_Atomic`).
As for **GCC**, those features are supported by version 4.9 or later.

In order to build it, just type:

`% make`

If you need a 32-bit binary, use:

`% make 32bit`

If you need a verbose build, use the `V` option:

`% make V=1`

If you need to rebuild dependencies, use:

`% make distclean`

And, finally, if you want to launch tests, just type:

`% make test`

**Note:** by default, tests use the `redis-server` that is installed on your system (the one that is found in your `$PATH`). If you need to use another `redis-server`, use the environment variable `REDIS_HOME`, ie:

`% REDIS_HOME=/path/to/my/redis/src make test`

As you can see, the make syntax (but also the output style) is the same used in Redis, so it will be familiar to Redis users.

# Install

In order to install Redis Cluster Proxy into /usr/local/bin just use:

`% make install`

You can use make PREFIX=/some/other/directory install if you wish to use a different destination.

# Usage

Redis Cluster Proxy attaches itself to an already running Redis cluster.
The binary will be compiled inside the `src` directory.
The basic usage is:

`./redis-cluster-proxy CLUSTER_ADDRESS`

where `CLUSTER_ADDRESS` is the host address of any cluster's instance (we call it the *entry point*), and it can be expressed in the form of an `IP:PORT` for TCP connections, or as UNIX socket by specifying the file name.

For example:

`./redis-cluster-proxy 127.0.0.1:7000`

`./redis-cluster-proxy /path/to/entry-point.socket`

It is also possible to specify more entry-points as multiple addresses. The proxy will use the first reachable entry-point in order to connect to the cluster and fetch the configuration of the cluster itself.
This can be useful since a single entry-point could be down, so you can use multiple addresses to make the proxy more reliable.

Example:

`./redis-cluster-proxy 127.0.0.1:7000 127.0.0.1:7001 127.0.0.1:7002`

If you need a basic help, just run it with the canonical `-h` or `--help` option.

`./redis-cluster-proxy -h`

By default, Redis Cluster Port will listen on port 7777, but you can change it with the `-p` or `--port` option.
Furthermore, by default, Redis Cluster Port will bind all available network interfaces to listen to incoming connections.
You can bind to specific interfaces by using the `--bind` options. You can bind  a single interface or you can bind multiple interfaces by using the `--bind` option more the once.

You can also tell Redis Cluster Port to listen upon a UNIX socket, by using the `--unixsocket` option to specify the socket filename and, optionally, the `--unixsocketperm` to set socket file permissions.

If you want to only listen on the UNIX socket, set `--port` to 0, so that the proxy won't listen on TCP sockets at all.

Examples:

Listen on port 7888

`./redis-cluster-proxy --port 7888 127.0.0.1:7000`

Listen on default port and bind only 127.0.0.1:

`./redis-cluster-proxy --bind 127.0.0.1 127.0.0.1:7000`

Listen on port 7888 and bind multiple interfaces:

`./redis-cluster-proxy --port 7888 --bind 192.168.0.10 --bind 10.0.0.10 127.0.0.1:7000`

Listen on UNIX socket and disable TCP connections

`./redis-cluster-proxy --unixsocket /path/to/proxy.socket --port 0 127.0.0.1:7000`

You can change the number of threads using the `--threads` option.

You can also use a configuration file instead of passing arguments by using the `-c` options, ie:

`redis-cluster-proxy -c /path/to/my/proxy.conf 127.0.0.1:7000`

You can find an example `proxy.conf` file inside the main Redis Cluster Proxy's directory.

After launching it, you can connect to the proxy as if it were a normal Redis server (however make sure to understand the current limitations).

You can then connect to Redis Cluster Proxy using the client you prefer, ie:

`redis-cli -p 7777`

# Private connections pool

Every thread has its own connections pool that contains *ready-to-use* private connections to the cluster, whose sockets are pre-connected in the same moment they are created.
This allows clients requiring private connections (ie. after commands such as `MULTI` or blocking commands) to immediately use a connection that is probably already connected to the cluster, instead of reconnecting to the cluster from scratch (a situation that could slow-down the sequence execution of the queries from the point-of-view of the client itself).
Every connection pool has a predefined size, and it's not allowed to create more connections than those allowed by its size.
The size of the connection pool can be configured via the `--connections-pool-size` option (by default it's 10).
When the pool runs out of connections, every new client requiring a private connection will create a new private connection from scratch and it will have to connect to the cluster and wait for the connection to be established. In this case, the connection model will be "lazy", meaning that the sockets of the new connection will connect to a particular node of the cluster only when the query will require a connection to that node.
Every thread will re-populate its own pool after the number of connections will drop below the specified minimum, that by default is the same of the size of the pool itself, and that can be configured via the `--connections-pool-min-size` option. The population rate and interval can be defined by the `--connections-pool-spawn-every` (interval in milliseconds) and `--connections-pool-spawn-rate` (number of new connection at every interval).

So:

```
redis-cluster-proxy --connections-pool-size 20 connections-pool-min-size 15 --connections-pool-spawn-rate 2 --connections-pool-spawn-every 500 127.0.0.1:7000
```

Means: *"create a connection pool containing 20 connections (maximum), and re-populate it when the number of connections drops below 15, by creating 2 new connections every 500 milliseconds"*.

Remember that every pool will be completely populated when the proxy starts.
It's also important to remark that when clients owning a private connection will disconnect, their thread will try to recycle their private connection in order to add it again to the pool if the pool itself is not already full.

# Password-protected clusters and Redis ACL

If your cluster nodes are protected with a password, you can use the `-a`, `--auth` command-line options or the `auth` option in a configuration file in order to specify an authentication password.
Furthermore, if your cluster is using the new [ACL](https://redis.io/topics/acl) implemented in Redis 6.0 and it has multiple users, you can even authenticate with a specific user by using the `--auth-user` command-line option (or `auth-user` in a config file) followed by the username.
Examples:

`redis-cluster-proxy -a MYPASSWORD 127.0.0.1:7000`

`redis-cluster-proxy --auth MYPASSWORD 127.0.0.1:7000`

`redis-cluster-proxy --auth-user MYUSER --auth MYPASSWORD 127.0.0.1:7000`

The proxy will use these credentials to authenticate to the cluster and fetch the cluster's internal configuration, but it will also automatically authenticate all clients with the provided credentials.
So, **all clients** that will connect to the proxy will be **automatically authenticated** with the user that is specified by `--auth-user` or with the `default` user if no user has been specified, **without the need** to call the `AUTH` command by themselves.
Anyway, if any client wants to be authenticated with a different user, it always can call the Redis `AUTH` command (documented [here](https://redis.io/commands/auth)): in this case, the client will use a private connection instead of the shared, multiplexed connection, and it will authenticate with another user.

# Enabling cross-slots queries

Cross-slots queries use multiple keys belonging to different slots or even different nodes.
Since their execution is not guaranteed to be atomic (so, they can actually break the atomic design of many Redis commands), they are disabled by default.
Anyway, if you don't mind about atomicity and you want this feature, you can enable it when you launch the proxy by using the `--enable-cross-slot`, or by setting `enable-cross-slot yes` into your config file. You can also activate this feature while the proxy is running by using the special `PROXY` command (see below).

**Note**: cross-slots queries are not supported by all the commands, even if the feature is enabled (ie. you cannot use it with `EVAL` or `ZUNIONSTORE` and many other commands). In that case, you'll receive a specific error reply. You can fetch a list of commands that cannot be used in cross-slots queries by using the `PROXY` command (see below).

# The PROXY command

The `PROXY` command will allow you to get specific info or perform actions that are specific to the proxy. The command has various subcommands, here's a little list:

- PROXY CONFIG GET|SET option [value]

  It can be used to get or set a specific option of the proxy, where the options
  are the same used in the command line arguments (without the `--` prefix) or specified in the config file.
  Not all the options can be changed (some of them, ie. `threads`, are read-only).
  
  Examples:

  ```
  PROXY CONFIG GET threads
  PROXY CONFIG SET log-level debug
  PROXY CONFIG SET enable-cross-slot 1
  ```
- PROXY MULTIPLEXING STATUS|OFF

  Get the status of multiplexing connection model for the calling client,
  or disable multiplexing by activating a private connection for the calling client.
  Examples:

  ```
  -> PROXY MULTIPLEXING STATUS
  -> Reply: "on"
  -> PROXY MULTIPLEXING off
  ```

- PROXY INFO

  Returns info specific to the cluster, similarly to the `INFO` command in Redis.

- PROXY COMMAND [UNSUPPORTED|CROSSSLOTS-UNSUPPORTED]

  Returns a list of all the Redis commands handled (known) 
  by Redis Cluster Proxy, in a similar fashion to Redis `COMMAND` function.
  The returned reply is a nested Array: every command will be an item of the 
  top-level array and it will be an array itself, containing the following 
  items: command name, arity, first key, last key, key step, supported.
  The last item ("supported") indicates whether the command is currently 
  supported by the proxy.

  The optional third argument can be used as a filter, with the following options:
  - `UNSUPPORTED`: only lists unsupported commands
  - `CROSSSLOTS-UNSUPPORTED`: only lists commands that cannot be used with 
  cross-slots queries, even if cross-slots queries have been enabled in the proxy's configuration.

- PROXY CLIENT <subcmd>

  Perform client-specific actions, ie:

    - `PROXY CLIENT ID`: get the current client's internal ID

    - `PROXY CLIENT THREAD`: get the current client's thread

- PROXY CLUSTER [subcmd]

  Perform actions related to the cluster associated with the calling client, ie:

    - `PROXY CLUSTER` or `PROXY CLUSTER INFO` Get info about the cluster. Info is an array whose elements are in the form of name/value pairs, where the names are specific features such as `status`, `connection`, and so on. You can also retrieve info for a single specific feature, ie. by calling `PROXY CLUSTER STATUS`.
    Below there's a list of common info that can be retrieved:

        - `status`: Current status of the cluster, can be `updating`, `updated` or `broken`
        - `connection`: Connection type, that can be `shared` if the client is working inside a multiplexing context (so the connection is shared with all the clients of the thread), or `private` if the client is using its own private connection.
        - `nodes`: A nested array containing the list of all the master nodes
                   of the cluster. Every node is another nested array, containing name/value pairs.

    - `PROXY CLUSTER UPDATE`: request an update of the current cluster's configuration.


  Examples:

  ```
  -> PROXY CLUSTER

  1) status
  2) updated
  3) connection
  4) shared
  5) nodes
  6) 1)  1) name
         2) 8d829c8b66f67dd9c4adad16e5c0a4c82aadd810
         3) ip
         4) 127.0.0.1
         5) port
         6) (integer) 7002
         7) slots
         8) (integer) 5462
         9) replicas
        10) (integer) 1
        11) connected
        12) (integer) 1
    ...

  ```

- PROXY LOG [level] MESSAGE

    Log `MESSAGE` to Proxy's log, for debugging purposes.

    The optional `level` can be used to define the log level:

    `debug`, `info`, `success`, `warning`, `error` (default is `debug`)
    
- PROXY DEBUG <subcommand>

    Perform different actions for debugging purpose, where `subcommand` can be:

    - `SEGFAULT`: crash the proxy with sigsegv

    - `ASSERT`: crash the proxy with an assertion failure

- PROXY SHUTDOWN [ASAP]

    Shutdown the proxy. The optional `ASAP` option makes the proxy exit immeditely (dirty exit).

- PROXY HELP

    Get help for the PROXY command

# Commands that act differently from standard Redis commands or that have special behavior

- PING: `PONG` is replied directly by the proxy
- MULTI: disables multiplexing for the calling client by creating a private
         connection in the client itself. **Note**: since it's required to be
         atomic, cross-slots queries cannot work inside a multi transaction.
- DBSIZE: sends the query to all nodes in the cluster and sums their replies,
          so that the result will be the total number of keys in the whole
          cluster.
- SCAN: performs the scan on all the master nodes of the cluster. The **cursor** contained in the reply will have a special four-digits suffix indicating the index of the node that has to be scanned. **Note**: sometimes the cursor could be something like "00001", so you mustn't convert it to an integer when your client has to use it to perform the next scan.

For a list of all known commands (both supported and unsupported) and their 
features, see [COMMANDS.md](COMMANDS.md)

# Current status

This project is currently beta code that is indented to be evaluated by the community in order to get suggestions and contributions. We discourage its usage in any production environment.
