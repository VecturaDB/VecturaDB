<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/assets/vecturadb-mark-dark.svg">
    <img src="https://raw.githubusercontent.com/VecturaDB/VecturaDB/main/docs/assets/vecturadb-mark-256.png" alt="VecturaDB" width="96">
  </picture>
</p>

# VecturaDB

[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/VecturaDB/VecturaDB/badge)](https://scorecard.dev/viewer/?uri=github.com/VecturaDB/VecturaDB)

VecturaDB is an independent fork of [Chroma](https://github.com/chroma-core/chroma), under the Apache License 2.0. The original code is copyright Chroma Inc. and its contributors. Independent project. Not affiliated with, endorsed by or sponsored by Chroma Inc. CHROMA is a registered trademark of Chroma Inc., used here only to describe compatibility. We use the name "Chroma" only to say which software our projects work with. It does not mean that Chroma Inc. made, approved or supports them.

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

While only previews exist, `pip install VecturaDB` installs the newest preview. Once `1.0.0` is out, install a preview with its exact version, for example `pip install VecturaDB==1.0.0rc4`: `--pre` takes the previews of the dependencies too, and a preview of httpx breaks the client.

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
docker run -p 127.0.0.1:8000:8000 -v ./data:/data ghcr.io/vecturadb/vecturadb:preview
```

Images for amd64 and arm64. `preview` is the newest preview; every release also has a tag with its version. The data lives in `/data`. `-p 127.0.0.1:8000:8000` publishes the port on this machine only.

On Windows with Docker Desktop, keep the data in a Docker volume rather than in a folder of Windows mounted into the container: in our tests the index of a collection in a mounted folder broke after a few abrupt stops of the server, in Chroma 1.5.9 too; in a volume it held far longer.

```bash
docker run -p 127.0.0.1:8000:8000 -v vecturadb-data:/data ghcr.io/vecturadb/vecturadb:preview
```

The files of a volume are not in a folder of Windows, and removing the volume removes the data. To back it up, with the server stopped, into vecturadb-data.tar.gz in the current folder, and to restore it, in PowerShell:

```powershell
docker run --rm -v vecturadb-data:/data -v "${PWD}:/backup" alpine tar czf /backup/vecturadb-data.tar.gz -C /data .
docker run --rm -v vecturadb-data:/data -v "${PWD}:/backup" alpine tar xzf /backup/vecturadb-data.tar.gz -C /data
```

In cmd, `"%cd%":/backup` in place of `"${PWD}:/backup"`.

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

## SPANN index

Chroma Cloud indexes the dense vectors of a collection with SPANN, single-node Chroma with HNSW. VecturaDB can give a collection a SPANN index on the single node too, behind the server option `spann`, off by default.

With a configuration file:

```yaml
persist_path: "./data"
executor:
  local:
    spann: true
```

```bash
vecturadb run config.yaml
```

With Docker, the image reads its configuration file, and an environment variable sets the option:

```bash
docker run -p 127.0.0.1:8000:8000 -v ./data:/data -e CHROMA_EXECUTOR__LOCAL__SPANN=true ghcr.io/vecturadb/vecturadb:preview
```

`run --path` reads no option, and the Python package has no SPANN in process.

- With the option on, a collection created with a SPANN configuration or schema gets a SPANN index; any other collection gets HNSW, as before.
- With the option off, a SPANN request gets HNSW, as on single-node Chroma. A SPANN collection created before still answers reads; a write to it fails with `SPANN collections need the executor option spann`.
- A read of a SPANN collection that fails with an error that distributed Chroma retries reads again: at once after a NotFound, otherwise after the backoff of `executor: local: retry` (`factor`, `min_delay_ms`, `max_delay_ms`, `max_attempts`, `jitter`), by default that of distributed Chroma: 2.0, 100 ms, 5000 ms, 5 times, with jitter.
- A write to a SPANN collection answers once its records are in the log and in SQLite, and reads see them at once. The server writes them to the files of the index later, as the compactor of distributed Chroma: every `compaction_interval_sec`, for the collections with at least `min_compaction_size` records after their last flush, or with one waiting at least `timeout_us`, at most `max_compaction_size` records per flush. The options are under `local_compaction_manager:`, by default those of distributed Chroma: 10 s, 10 records, 10,000 records and 86,400,000,000 µs (24 hours). With Docker, one variable per option, for example `-e CHROMA_LOCAL_COMPACTION_MANAGER__COMPACTION_INTERVAL_SEC=30`.
- During a bulk load, the reads of a SPANN collection slow down until its records reach the files of the index: each read also goes through the records that wait for a flush, about 10 ms more for every 1,000 of them with 64 dimensions in our tests, more with larger vectors and documents. A lower `compaction_interval_sec` keeps them fewer.
- Chroma 1.5.9 and VecturaDB 1.0.0-preview.3.post1 open a data path with SPANN collections and serve the other collections as before. A SPANN collection answers there with an error, 500: do not write to it with them. Deleting it there works; VecturaDB removes its files at the next startup.

## Clients

A client that needs to know which server it talks to reads `GET /api/v2/pre-flight-checks`. VecturaDB answers with the fields of Chroma and adds a `server` block, for example:

```json
{"max_batch_size": 5461, "supports_base64_encoding": true, "server": {"name": "VecturaDB", "version": "1.0.0-preview.N", "features": ["sparse_vector_index", "bm25", "search", "fork", "api_keys", "indexing_status"]}}
```

`features` lists what VecturaDB has beyond single-node Chroma:

- `sparse_vector_index`: sparse vector indexes in the schema of a collection;
- `bm25`: sparse indexes with BM25 scoring;
- `search`: the Search API, hybrid search included;
- `fork`: collection fork and fork count;
- `api_keys`: API keys, as on Chroma Cloud;
- `indexing_status`: the indexing status of a collection, as on Chroma Cloud.

With [API keys](docs/mintlify/guides/deploy/client-server-mode.mdx), only a request with a valid key gets the block. Chroma does not send the block, and the clients of Chroma ignore it. `GET /api/v2/version` answers `1.0.0`, as Chroma does.

## Telemetry

Telemetry is off by default. Setting `ANONYMIZED_TELEMETRY=TRUE` turns on the anonymized telemetry inherited from Chroma (PostHog, Python package only). The server binary sends none.

## License

Apache License 2.0: see [LICENSE](https://github.com/VecturaDB/VecturaDB/blob/main/LICENSE) and [NOTICE](https://github.com/VecturaDB/VecturaDB/blob/main/NOTICE).
