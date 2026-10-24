# VecturaDB

VecturaDB was forked from the open-source [Chroma](https://github.com/chroma-core/chroma) project. It is not affiliated with Chroma.

VecturaDB is an open-source vector database, compatible with the Chroma API. This repository keeps the full git history of Chroma and its Apache 2.0 license.

## Why VecturaDB

Chroma Cloud is winding down: it stops accepting writes on 22 November 2026 and stops serving reads on 22 December 2026. Some features of Chroma exist only in Chroma Cloud and in its distributed version: sparse vectors, the BM25 index and hybrid search. VecturaDB brings them to the single-node database, the one that runs with Docker, as a server, or inside a Python application, so that they stay available to everyone, as open source.

Sparse vectors and BM25 are in review in this repository; hybrid search is in progress.

No package is published yet. The CI of this repository builds the CLI, the Python wheels and the Docker image.

## Python

When it is published, the `VecturaDB` package will replace `chromadb`: the import name stays `chromadb`, so existing code runs as it is. The two packages install the same modules, so they cannot be installed side by side.

## Telemetry

Telemetry is off by default. Setting `ANONYMIZED_TELEMETRY=TRUE` turns on the anonymized telemetry inherited from Chroma (PostHog, Python package only). The server binary sends none.

## License

Apache License 2.0: see [LICENSE](LICENSE) and [NOTICE](NOTICE).
