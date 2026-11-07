# LazyFS (https://github.com/dsrhaslab/lazyfs): a FUSE file system that keeps what is not synced in memory and drops it on
# request, as a power cut does. The binary of vecturadb under test is mounted at /vecturadb.
FROM debian:stable-slim AS lazyfs
RUN apt-get update && apt-get install -y --no-install-recommends git ca-certificates g++ cmake make pkg-config libfuse3-dev fuse3 && rm -rf /var/lib/apt/lists/*
RUN git clone https://github.com/dsrhaslab/lazyfs.git /lazyfs && cd /lazyfs && git checkout d93e54a7944b28e86506d2af3a97342f5c3eb21f \
    && cd libs/libpcache && ./build.sh && cd /lazyfs/lazyfs && ./build.sh
FROM debian:stable-slim
RUN apt-get update && apt-get install -y --no-install-recommends fuse3 python3 curl ca-certificates procps && rm -rf /var/lib/apt/lists/*
COPY --from=lazyfs /lazyfs /lazyfs
COPY restore.sh client.py save.sh snapshot.sh client_save.py /
ENTRYPOINT ["bash", "/restore.sh"]
