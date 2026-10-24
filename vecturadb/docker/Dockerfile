# Copyright VecturaDB contributors. Licensed under the Apache License, Version 2.0.
# Single-node image of VecturaDB: the same runner as the "cli" stage of rust/Dockerfile, with a chroma binary built
# outside Docker (cargo zigbuild, glibc 2.17) and copied in. Build context: chroma-amd64, chroma-arm64, config.yaml.
FROM debian:stable-slim
ARG TARGETARCH
ARG REVISION=unknown
LABEL org.opencontainers.image.title="VecturaDB" \
      org.opencontainers.image.description="VecturaDB, compatible with the Chroma API" \
      org.opencontainers.image.source="https://github.com/VecturaDB/VecturaDB" \
      org.opencontainers.image.url="https://vecturadb.org" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.revision="${REVISION}"
RUN apt-get update && apt-get install -y dumb-init libssl-dev ca-certificates && rm -rf /var/lib/apt/lists/*
COPY config.yaml /config.yaml
COPY chroma-${TARGETARCH} /usr/local/bin/chroma
EXPOSE 8000
ENTRYPOINT [ "dumb-init", "--", "chroma" ]
CMD [ "run", "/config.yaml" ]
