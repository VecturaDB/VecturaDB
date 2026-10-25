# Copyright VecturaDB contributors. Licensed under the Apache License, Version 2.0.
# Single-node image of VecturaDB: the same runner as the "cli" stage of rust/Dockerfile, with a vecturadb binary
# built outside Docker (cargo zigbuild, glibc 2.17) and copied in, also as chroma. Build context: vecturadb-amd64,
# vecturadb-arm64, config.yaml.
FROM debian:stable-slim
ARG TARGETARCH
ARG REVISION=unknown
LABEL org.opencontainers.image.title="VecturaDB" \
      org.opencontainers.image.description="Vector database compatible with the Chroma API. Independent project. Not affiliated with, endorsed by or sponsored by Chroma Inc. CHROMA is a registered trademark of Chroma Inc., used here only to describe compatibility." \
      org.opencontainers.image.source="https://github.com/VecturaDB/VecturaDB" \
      org.opencontainers.image.url="https://vecturadb.org" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.revision="${REVISION}"
RUN apt-get update && apt-get install -y dumb-init libssl-dev ca-certificates && rm -rf /var/lib/apt/lists/*
COPY config.yaml /config.yaml
COPY vecturadb-${TARGETARCH} /usr/local/bin/vecturadb
RUN ln -s vecturadb /usr/local/bin/chroma
EXPOSE 8000
ENTRYPOINT [ "dumb-init", "--", "vecturadb" ]
CMD [ "run", "/config.yaml" ]
