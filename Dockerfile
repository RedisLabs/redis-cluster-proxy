FROM ubuntu:18.04 as build-env

# Install dependencies
RUN apt-get update && apt-get install -y git gcc make libhiredis0.13

RUN git clone https://github.com/RedisLabs/redis-cluster-proxy.git

WORKDIR /redis-cluster-proxy/

RUN git checkout 1.0

RUN make


FROM ubuntu:18.04

WORKDIR /bin/

COPY --from=build-env redis-cluster-proxy/src/redis-cluster-proxy .