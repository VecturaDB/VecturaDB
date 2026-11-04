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

Chroma Cloud is winding down: it stops accepting writes on 7 December 2026 and stops serving reads on 22 December 2026. Some features of Chroma exist only in Chroma Cloud and in its distributed version: sparse vectors, the BM25 index and hybrid search. VecturaDB brings them to the single-node database, the one that runs with Docker, as a server, or inside a Python application, so that they stay available to everyone, as open source.

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
vecturadb run --path ./data
```

Windows (x64), in PowerShell:

```powershell
iex ((New-Object System.Net.WebClient).DownloadString('https://raw.githubusercontent.com/VecturaDB/VecturaDB/main/rust/cli/install/install.ps1'))
```

The scripts install the newest release, previews included. Every binary is also in the [releases](https://github.com/VecturaDB/VecturaDB/releases). The command is `vecturadb`. To have it also as `chroma`, set `VECTURADB_CHROMA_ALIAS=1` for the script.

## The SQLite database of the data path

The database is in WAL mode: a read does not wait for a write. A normal stop writes the log of the writes back into `chroma.sqlite3`, which is then the only file of SQLite. After a stop that is not normal, such as a kill, the log stays in `chroma.sqlite3-wal`: a copy of the data path needs it too, or start and stop the server once before the copy. Chroma 1.5.9 and VecturaDB 1.0.0-preview.4 open the data path as it is.

SQLite keeps WAL to the processes of one machine. For a data path on a network filesystem (SMB, NFS), set `sqlitedb: journal_mode: delete`, the mode of Chroma; with Docker, `-e CHROMA_SQLITEDB__JOURNAL_MODE=delete`. In our tests, on an SMB share mounted by Docker with `vers=3.0`, Chroma 1.5.9, VecturaDB 1.0.0-preview.4 and VecturaDB in WAL and in delete did not start without the mount option `nobrl`, and all of them worked with it.

## Moving data out of Chroma Cloud

The CLI copies collections from Chroma Cloud into a running VecturaDB:

```bash
vecturadb login --api-key <your Chroma Cloud API key>
vecturadb copy --from-cloud --to-local --host http://127.0.0.1:8000 --db <database> --all
```

To copy only some collections, use `--collections <name>` once per collection instead of `--all`.

What arrives: every record with its document, metadata, embedding and URI, the metadata of the collection, and its schema, sparse and BM25 indexes included. Sparse and BM25 searches give the same scores as on Chroma Cloud. The dense index arrives as on Chroma Cloud: SPANN, with the same settings. Collections whose metadata holds the legacy `hnsw:*` settings arrive with them.

The copy waits when VecturaDB asks it to, when a server fails and when the network falls, and says why. A collection that cannot be copied does not stop the others: at the end the CLI says what happened to it, and exits with code 1.

Run again, the same command compares every collection already in VecturaDB with Chroma Cloud, record by record, before it writes anything, and reports how many records are the same, missing, different and only in VecturaDB. A collection that is the same is left alone. For the others the CLI asks what to do. Where nobody can answer, add `--resume --yes` to add the records they do not have, or `--force --yes` to make them exact copies of Chroma Cloud: `--force` also removes the records that are not on Chroma Cloud.

The steps, with Docker and without, are in [Leaving Chroma Cloud: to VecturaDB](https://vecturadb.org/leaving-chroma-cloud/to-vecturadb). The same command copies into Chroma 1.5.9: see [to Chroma 1.5.9](https://vecturadb.org/leaving-chroma-cloud/to-chroma-single-node).

## SPANN index

Chroma Cloud indexes the dense vectors of a collection with SPANN, single-node Chroma with HNSW. VecturaDB gives a collection that asks for SPANN a SPANN index on the single node too, with the server option `spann`, on by default; every other collection gets HNSW, as before.

To turn it off, with a configuration file:

```yaml
persist_path: "./data"
executor:
  local:
    spann: false
```

```bash
vecturadb run config.yaml
```

With Docker, the image reads its configuration file, and an environment variable sets the option:

```bash
docker run -p 127.0.0.1:8000:8000 -v ./data:/data -e CHROMA_EXECUTOR__LOCAL__SPANN=false ghcr.io/vecturadb/vecturadb:preview
```

`run --path` and the Python package in process (`PersistentClient`) have the option on and read no other option. A server or a client without a persistent data path gives every collection HNSW.

- With the option on, a collection created with a SPANN configuration or schema gets a SPANN index; any other collection gets HNSW, as before.
- With the option off, a SPANN request gets HNSW, as on single-node Chroma. A SPANN collection created before still answers reads; a write to it fails with `SPANN collections need the executor option spann`.
- A read of a SPANN collection that fails with an error that distributed Chroma retries reads again: at once after a NotFound, otherwise after the backoff of `executor: local: retry` (`factor`, `min_delay_ms`, `max_delay_ms`, `max_attempts`, `jitter`), by default that of distributed Chroma: 2.0, 100 ms, 5000 ms, 5 times, with jitter.
- A write to a SPANN collection answers once its records are in the log and in SQLite, and reads see them at once. The server writes them to the files of the index later, as the compactor of distributed Chroma: every `compaction_interval_sec`, for the collections with at least `min_compaction_size` records after their last flush, or with one waiting at least `timeout_us`, at most `max_compaction_size` records per flush, and a flush that takes them all is followed by the next one at once. The options are under `local_compaction_manager:`, by default those of distributed Chroma except `max_compaction_size`: 10 s, 10 records, 50,000 records (distributed Chroma takes 10,000) and 86,400,000,000 µs (24 hours). With Docker, one variable per option, for example `-e CHROMA_LOCAL_COMPACTION_MANAGER__COMPACTION_INTERVAL_SEC=30`.
- During a bulk load, the reads of a SPANN collection slow down until its records reach the files of the index: each read also goes through the records that wait for a flush, about 10 ms more for every 1,000 of them with 64 dimensions in our tests, more with larger vectors and documents. A lower `compaction_interval_sec` keeps them fewer.
- A search of a SPANN collection takes the `read_level` of distributed Chroma. `index_only` reads no log: it answers what the last flush holds, so it does not see the records written after it, answers the old version of a record updated after it, and can still find a record deleted after it. `index_and_bounded_wal` also reads the first `executor: local: bounded_wal_limit` records after the flush. Unlike distributed Chroma, filters and sparse ranks read the current data of the records: a filter judges an updated record by its new metadata and leaves out a deleted one. `get` and `query` have no read level, as in distributed Chroma.
- A write to a SPANN collection whose log holds `log: sqlite: num_records_before_backpressure` records after its last flush, 100,000 by default as in distributed Chroma, is refused until a flush takes them, with the answer of distributed Chroma: 429, `log needs compaction before accepting more writes; please backoff exponentially and retry`. Unlike distributed Chroma, the answer also has the header `Retry-After`: the seconds, at least 1, until the end of the flushes that should take the log under the limit, estimated from the rounds of the compactor.
- Chroma 1.5.9 and VecturaDB 1.0.0-preview.3.post1 open a data path with SPANN collections and serve the other collections as before. A SPANN collection answers there with an error, 500: do not write to it with them. Deleting it there works; VecturaDB removes its files at the next startup.

## Conditional transactions

The conditional transactions of distributed Chroma work on the single node, on by default as on Chroma Cloud, with the same API and, except for the differences below, the same answers as Chroma Cloud: `collection.conditional()` reads records, buffers writes, and commits them only if no record it read or writes changed after its reads; otherwise the commit fails with `ConditionalWriteConflictError` (409), and the client retries with a new transaction. They work over HTTP and in process, with the Rust bindings.

Reads and writes outside a transaction do not look at the log, and a collection that never uses them works as on Chroma 1.5.9. To turn them off:

```yaml
enable_transactions: false
enable_log_scouting: false
```

With Docker, `-e CHROMA_ENABLE_TRANSACTIONS=false -e CHROMA_ENABLE_LOG_SCOUTING=false`.

A commit checks the records written after its reads in the log of the collection. From its first conditional read or write, a collection keeps them for `log: sqlite: purge_cutoff_time_seconds` after they are written, by default 43,200 (12 hours), as distributed Chroma keeps its log, and purges them every `local_compaction_manager: purge_interval_sec`, by default 7,200 (2 hours). A collection that has used no transaction for that long, or since the server started, purges what it applied at once, as Chroma 1.5.9. A lower cutoff keeps the database file smaller.

Differences from Chroma Cloud:

- A later read of a transaction answers its records as they were at its first read, or fails with `StaleReadError` (412) when one changed after it, and the client starts again, as `collection.conditional().run()` does. Chroma Cloud answers them as they were.
- A record that a filter found at the first read and no longer finds is not in the later read, where Chroma Cloud answers it, so its commit does not check that record.
- A transaction whose reads are older than a restart of the server may fail at its commit with `StaleReadError` (412), as one older than the cutoff, and the client starts it again.
- A stale read names its error `StaleReadError`, the name the clients go by, where Chroma Cloud answers 412 with the name `ChromaError`.
- The positions in the log, the read token and `first_inserted_record_offset`, count the records of the whole server, not those of the collection.

## Attached functions

The attached functions of distributed Chroma work on the single node, with the same API: `collection.attach_function()` writes what a function computes from the records of a collection to its output collection. The functions are those of distributed Chroma: `record_counter`, `statistics` and `revision_history` run after each apply of the records of their input; `count_to_file_async` runs later, from its queue, and after 5 failures in a row it stops, as in distributed Chroma. `http_generate` and `http_currents` do not run on the single node, as they are part of Foundations, a service of Chroma Cloud: each of their runs fails, and they stop after 5.

`count_to_file_async` writes a file, and as in distributed Chroma it is only enabled with `allow_reset: true`. By default it writes in the data path. To write to S3 instead, give the storage under `local_compaction_manager:`, in the format of distributed Chroma, for example:

```yaml
local_compaction_manager:
  storage:
    s3:
      bucket: "chroma-storage"
      credentials: "AWS"
```

The storage holds only the files of the functions: the indexes stay in the data path.

Chroma 1.5.9 opens a data path with attached functions, and serves its collections as before; it does not run the functions. A synchronous function catches up with the writes of Chroma 1.5.9 once VecturaDB opens the data path again. An asynchronous function takes the records that already existed and that Chroma 1.5.9 deleted or updated as if they had not existed: `count_to_file_async` does not count those deletes, and counts those upserts as new records. To count again, detach it and attach it again with a new file.

## Clients

A client that needs to know which server it talks to reads `GET /api/v2/pre-flight-checks`. VecturaDB answers with the fields of Chroma and adds a `server` block, for example:

```json
{"max_batch_size": 5461, "supports_base64_encoding": true, "server": {"name": "VecturaDB", "version": "1.0.0-preview.N", "features": ["sparse_vector_index", "bm25", "search", "fork", "api_keys", "indexing_status", "attached_functions", "spann", "conditional_transactions"]}}
```

`features` lists what VecturaDB has beyond single-node Chroma:

- `sparse_vector_index`: sparse vector indexes in the schema of a collection;
- `bm25`: sparse indexes with BM25 scoring;
- `search`: the Search API, hybrid search included;
- `fork`: collection fork and fork count;
- `api_keys`: API keys, as on Chroma Cloud;
- `indexing_status`: the indexing status of a collection. Distributed Chroma counts the operations of its log; the single node counts the records, so 3 adds and then 2 deletes count 1 and not 5. An HNSW collection has nothing waiting; a SPANN collection reports the records of its last flush as indexed and the operations of its log after it as not indexed;
- `attached_functions`: the attached functions of distributed Chroma;
- `spann`: SPANN indexes for the collections that ask for them, as on distributed Chroma; absent with `executor: local: spann: false`;
- `conditional_transactions`: the conditional transactions of distributed Chroma; absent with `enable_transactions: false`.

With [API keys](docs/mintlify/guides/deploy/client-server-mode.mdx), only a request with a valid key gets the block. Chroma does not send the block, and the clients of Chroma ignore it. `GET /api/v2/version` answers `1.0.0`, as Chroma does.

## Telemetry

Telemetry is off by default. Setting `ANONYMIZED_TELEMETRY=TRUE` turns on the anonymized telemetry inherited from Chroma (PostHog, Python package only). The server binary sends none.

## License

Apache License 2.0: see [LICENSE](https://github.com/VecturaDB/VecturaDB/blob/main/LICENSE) and [NOTICE](https://github.com/VecturaDB/VecturaDB/blob/main/NOTICE).
