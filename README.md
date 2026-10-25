<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/assets/vecturadb-mark-dark.svg">
    <img src="https://raw.githubusercontent.com/VecturaDB/VecturaDB/main/docs/assets/vecturadb-mark-256.png" alt="VecturaDB" width="96">
  </picture>
</p>

# VecturaDB

[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/VecturaDB/VecturaDB/badge)](https://scorecard.dev/viewer/?uri=github.com/VecturaDB/VecturaDB)

VecturaDB was forked from the open-source [Chroma](https://github.com/chroma-core/chroma) project. It is not affiliated with Chroma.

VecturaDB is an open-source vector database, compatible with the Chroma API. This repository keeps the full git history of Chroma and its Apache 2.0 license.

## Why VecturaDB

Chroma Cloud is winding down: it stops accepting writes on 22 November 2026 and stops serving reads on 22 December 2026. Some features of Chroma exist only in Chroma Cloud and in its distributed version: sparse vectors, the BM25 index and hybrid search. VecturaDB brings them to the single-node database, the one that runs with Docker, as a server, or inside a Python application, so that they stay available to everyone, as open source.

Sparse vectors, BM25, the Search API, hybrid search included, and collection fork work on the single node.

The releases are previews, `1.0.0-preview.N`, until VecturaDB has been proven in use; then comes `1.0.0`.

## Python

```bash
pip uninstall -y chromadb
pip install VecturaDB
```

The `VecturaDB` package keeps the import name `chromadb`, so existing code runs as it is.

```python
import chromadb

client = chromadb.HttpClient(host="localhost", port=8000)
```

While only previews exist, `pip install VecturaDB` installs the newest preview. Once `1.0.0` is out, a preview needs `pip install --pre VecturaDB`.

### Packages that depend on chromadb

`VecturaDB` and `chromadb` install the same modules, so they cannot be installed side by side. Packages that declare a dependency on `chromadb`, such as `langchain-chroma`, make pip install `chromadb` too, and its files replace those of VecturaDB. Install those packages first and VecturaDB last:

```bash
pip install langchain-chroma
pip install --force-reinstall --no-deps VecturaDB
```

- Run the last line again after any command that installs or upgrades `chromadb`.
- Do not uninstall `chromadb` afterwards: it removes files that VecturaDB uses.
- A constraints file does not help: pip cannot exclude `chromadb` and still install a package that requires it.

## Docker

```bash
docker run -p 8000:8000 -v ./data:/data ghcr.io/vecturadb/vecturadb:preview
```

Images for amd64 and arm64. `preview` is the newest preview; every release also has a tag with its version. The data lives in `/data`.

## CLI

Linux (x86_64 and arm64) and macOS (x64 and arm64):

```bash
curl -sSL https://raw.githubusercontent.com/VecturaDB/VecturaDB/main/rust/cli/install/install.sh | bash
chroma run --path ./data
```

Windows (x64), in PowerShell:

```powershell
iex ((New-Object System.Net.WebClient).DownloadString('https://raw.githubusercontent.com/VecturaDB/VecturaDB/main/rust/cli/install/install.ps1'))
```

The scripts install the newest release, previews included. Every binary is also in the [releases](https://github.com/VecturaDB/VecturaDB/releases). The binary keeps the name `chroma`.

## Moving data out of Chroma Cloud

The CLI copies collections from Chroma Cloud into a running VecturaDB:

```bash
chroma login --api-key <your Chroma Cloud API key>
chroma copy --from-cloud --to-local --host http://localhost:8000 --db <database> --all
```

To copy only some collections, use `--collections <name>` once per collection instead of `--all`. The collections must not exist yet in VecturaDB.

The copy stops at the first collection it cannot create. The collections copied before it stay in VecturaDB: copy the rest with `--collections`. Collections whose metadata holds the legacy `hnsw:*` settings fail with `Cannot set both collection config and schema simultaneously`; Chroma gives the same error.

What arrives: every record with its document, metadata, embedding and URI, the metadata of the collection, and its schema, sparse and BM25 indexes included. Sparse and BM25 searches give the same scores as on Chroma Cloud. The settings of the dense index do not arrive: Chroma Cloud uses SPANN, VecturaDB uses HNSW with its default settings.

## Telemetry

Telemetry is off by default. Setting `ANONYMIZED_TELEMETRY=TRUE` turns on the anonymized telemetry inherited from Chroma (PostHog, Python package only). The server binary sends none.

## License

Apache License 2.0: see [LICENSE](https://github.com/VecturaDB/VecturaDB/blob/main/LICENSE) and [NOTICE](https://github.com/VecturaDB/VecturaDB/blob/main/NOTICE).
