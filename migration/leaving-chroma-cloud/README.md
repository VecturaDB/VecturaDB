# Leaving Chroma Cloud: scripts

Three scripts for moving collections out of Chroma Cloud, to Chroma 1.5.9, Chroma distributed or VecturaDB. The guides on [vecturadb.org](https://vecturadb.org) explain when to use them.

| Script | What it does |
|---|---|
| `list_for_copy.py` | Prints `--collections <name>` for every collection of a Chroma Cloud database, for `chroma copy` |
| `copy_without_sparse.py` | Copies the collections that `chroma copy` cannot create: on Chroma 1.5.9 those with a sparse vector or BM25 index, and anywhere those with the legacy `hnsw:*` settings in their metadata. Sparse vectors go to `<name>.sparse.jsonl` |
| `verify.py` | Compares each collection on Chroma Cloud and on the destination: count, ids, documents, metadata, URIs, embeddings, and the distances of three dense queries |

Requirements: Python with `chromadb` 1.5.9 or `VecturaDB`. The Chroma Cloud credentials come from the environment:

```bash
export CHROMA_API_KEY=<key> CHROMA_TENANT=<tenant> CHROMA_DATABASE=<database>
python list_for_copy.py
python copy_without_sparse.py 127.0.0.1 8000 <name> <name>
python verify.py 127.0.0.1 8000 <name> <name>
```

We tested them on 25 October 2026 against Chroma Cloud, Chroma 1.5.9, Chroma distributed and VecturaDB 1.0.0-preview.1. VecturaDB is not affiliated with Chroma.
