FROM alpine:3.11 as build

RUN apk add --no-cache gcc musl-dev linux-headers openssl-dev make

RUN addgroup -S app && adduser -S -G app app 
RUN chown -R app:app /opt
RUN chown -R app:app /usr/local

# There is a bug in CMake where we cannot build from the root top folder
# So we build from /opt
COPY --chown=app:app . /opt
WORKDIR /opt

USER app
RUN [ "make", "install" ]

FROM alpine:3.11 as runtime

# It is quite helpful to have the redis-cli available 
# on the proxy to do basic sanity testing
# curl can be used to fetch redis-cluster endpoints
# configuration with the kubernete HTTP apis
RUN apk add --no-cache redis curl ca-certificates

RUN addgroup -S app && adduser -S -G app app 
COPY --chown=app:app --from=build /usr/local/bin/redis-cluster-proxy /usr/local/bin/redis-cluster-proxy
RUN chmod +x /usr/local/bin/redis-cluster-proxy
RUN ldd /usr/local/bin/redis-cluster-proxy

# Copy source code for gcc
COPY --chown=app:app --from=build /opt /opt

# Now run in usermode
USER app
WORKDIR /home/app

ENTRYPOINT ["/usr/local/bin/redis-cluster-proxy"]
EXPOSE 7777
CMD ["redis-cluster-proxy"]
