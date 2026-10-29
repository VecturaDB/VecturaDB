# syntax=docker/dockerfile:1
# Copyright VecturaDB contributors. Licensed under the Apache License, Version 2.0.
# Single-node image of VecturaDB, built from the source as Chroma builds its official image (the "cli" stage of
# rust/Dockerfile): rust:1.92.0, whose C and C++ compilers are the gcc and the libstdc++ of Debian, a release build, and the
# same runner. The differences: only the binary vecturadb is built (chroma-cli), as the CLI of a release is, not the whole
# workspace, which does not fit in the standard runners of GitHub, so the features of the dependencies are those of the
# CLI; the profile is the one of the release of the CLI, without debug information and with the symbols stripped; there
# are no stages of cargo-chef, whose layers the runners of GitHub do not keep from one build to the next; and none of the
# arguments that the release of Chroma leaves empty (ADDRESS_SANITIZER, ENABLE_AVX512, LOG_SERVICE_CARGO_FEATURES). Build
# context: the root of the repository.
FROM rust:1.92.0 AS builder

ARG PROTOC_VERSION=31.1
RUN ARCH=$(uname -m) && \
  if [ "$ARCH" = "x86_64" ]; then \
  PROTOC_ZIP=protoc-${PROTOC_VERSION}-linux-x86_64.zip; \
  elif [ "$ARCH" = "aarch64" ]; then \
  PROTOC_ZIP=protoc-${PROTOC_VERSION}-linux-aarch_64.zip; \
  else \
  echo "Unsupported architecture: $ARCH" && exit 1; \
  fi && \
  curl -OL https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/$PROTOC_ZIP && \
  unzip -o $PROTOC_ZIP -d /usr/local bin/protoc && \
  unzip -o $PROTOC_ZIP -d /usr/local 'include/*' && \
  rm -f $PROTOC_ZIP && \
  chmod +x /usr/local/bin/protoc && \
  protoc --version

WORKDIR /chroma
ENV CARGO_INCREMENTAL=0 \
    CARGO_PROFILE_RELEASE_DEBUG=0 \
    CARGO_PROFILE_RELEASE_STRIP=symbols

# The settings of the builds of the repository, SQLite among them.
COPY .cargo/ .cargo/
COPY idl/ idl/
COPY Cargo.toml Cargo.toml
COPY Cargo.lock Cargo.lock
COPY rust/ rust/

RUN --mount=type=cache,sharing=locked,target=/usr/local/cargo/registry/ \
  --mount=type=cache,sharing=locked,target=/usr/local/cargo/git/ \
  cargo build --release -p chroma-cli --bin vecturadb && \
  cp target/release/vecturadb /vecturadb

FROM debian:stable-slim AS runner
RUN apt-get update && apt-get install -y dumb-init libssl-dev ca-certificates && rm -rf /var/lib/apt/lists/*

FROM runner AS cli
ARG REVISION=unknown
LABEL org.opencontainers.image.title="VecturaDB" \
      org.opencontainers.image.description="Vector database compatible with the Chroma API. Independent project. Not affiliated with, endorsed by or sponsored by Chroma Inc. CHROMA is a registered trademark of Chroma Inc., used here only to describe compatibility." \
      org.opencontainers.image.source="https://github.com/VecturaDB/VecturaDB" \
      org.opencontainers.image.url="https://vecturadb.org" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.revision="${REVISION}"
COPY vecturadb/docker/config.yaml /config.yaml
COPY --from=builder /vecturadb /usr/local/bin/vecturadb
RUN ln -s vecturadb /usr/local/bin/chroma
EXPOSE 8000
ENTRYPOINT [ "dumb-init", "--", "vecturadb" ]
CMD [ "run", "/config.yaml" ]
