FROM ubuntu:latest

RUN apt-get update && apt-get -y install build-essential

COPY ./ /src
WORKDIR /src
RUN make && make install
ENTRYPOINT ["redis-cluster-proxy"]
CMD ["--help"]
