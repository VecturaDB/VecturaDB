# Copyright VecturaDB contributors. Licensed under the Apache License, Version 2.0.
# Copies Chroma Cloud collections that chroma copy cannot create: on a single-node Chroma 1.5.9 those with a sparse
# vector or BM25 index, and on any destination those with the legacy hnsw:* settings in their metadata. Records,
# documents, metadata, embeddings and URIs go to a new collection without the sparse index, with the same name,
# metadata and distance space.
# Chroma 1.5.9 rejects sparse vectors even as plain metadata, so they are saved to <collection>.sparse.jsonl
# (one line per record: id and its sparse vectors) instead of being lost.
# Usage: python copy_without_sparse.py <host> <port> <collection> [<collection> ...]
# Chroma Cloud credentials come from the environment: CHROMA_API_KEY, CHROMA_TENANT, CHROMA_DATABASE.
import json
import os
import sys

import chromadb
from chromadb.base_types import SparseVector

PAGE = 300  # Chroma Cloud answers get() with at most 300 records per call

cloud = chromadb.CloudClient(tenant=os.environ["CHROMA_TENANT"], database=os.environ["CHROMA_DATABASE"],
                             api_key=os.environ["CHROMA_API_KEY"])
local = chromadb.HttpClient(host=sys.argv[1], port=int(sys.argv[2]))

for name in sys.argv[3:]:
    source = cloud.get_collection(name, embedding_function=None)
    config = source.configuration_json or {}
    space = ((config.get("spann") or config.get("hnsw") or {}).get("space")) or "l2"
    target = local.create_collection(name, metadata=source.metadata, embedding_function=None,
                                     configuration={"hnsw": {"space": space}})
    copied = saved = 0
    offset = 0
    with open(f"{name}.sparse.jsonl", "w", encoding="utf-8") as sparse_file:
        while True:
            page = source.get(include=["documents", "metadatas", "embeddings", "uris"], limit=PAGE, offset=offset)
            if not page["ids"]:
                break
            metadatas = []
            for record_id, metadata in zip(page["ids"], page["metadatas"]):
                if metadata is None:
                    metadatas.append(None)
                    continue
                sparse = {k: {"indices": list(v.indices), "values": list(v.values)}
                          for k, v in metadata.items() if isinstance(v, SparseVector)}
                if sparse:
                    sparse_file.write(json.dumps({"id": record_id, "sparse": sparse}) + "\n")
                    saved += len(sparse)
                kept = {k: v for k, v in metadata.items() if not isinstance(v, SparseVector)}
                metadatas.append(kept or None)
            target.add(ids=page["ids"], embeddings=page["embeddings"], documents=page["documents"],
                       metadatas=metadatas, uris=page["uris"])
            copied += len(page["ids"])
            offset += PAGE
    print(f"{name}: {copied} records copied, {saved} sparse vectors saved to {name}.sparse.jsonl, space {space}")
