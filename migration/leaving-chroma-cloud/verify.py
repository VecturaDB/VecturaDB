# Copyright VecturaDB contributors. Licensed under the Apache License, Version 2.0.
# Final check after a move out of Chroma Cloud: the same records on both sides, and the same answers to queries.
# Usage: python verify.py <host> <port> <collection> [<collection> ...]
# Chroma Cloud credentials come from the environment: CHROMA_API_KEY, CHROMA_TENANT, CHROMA_DATABASE.
# Sparse vectors in metadata are compared only when the destination has them (Chroma 1.5.9 cannot hold them).
import os
import sys

import chromadb
import numpy as np
from chromadb.base_types import SparseVector

PAGE = 300

cloud = chromadb.CloudClient(tenant=os.environ["CHROMA_TENANT"], database=os.environ["CHROMA_DATABASE"],
                             api_key=os.environ["CHROMA_API_KEY"])
local = chromadb.HttpClient(host=sys.argv[1], port=int(sys.argv[2]))


def read_all(collection):
    records = {}
    offset = 0
    while True:
        page = collection.get(include=["documents", "metadatas", "embeddings", "uris"], limit=PAGE, offset=offset)
        if not page["ids"]:
            return records
        for i, record_id in enumerate(page["ids"]):
            records[record_id] = (page["documents"][i], page["metadatas"][i], np.asarray(page["embeddings"][i]),
                                  page["uris"][i] if page["uris"] else None)
        offset += PAGE


def without_sparse(metadata):
    return {k: v for k, v in (metadata or {}).items() if not isinstance(v, SparseVector)} or None


all_ok = True
for name in sys.argv[3:]:
    source, target = cloud.get_collection(name, embedding_function=None), local.get_collection(name, embedding_function=None)
    a, b = read_all(source), read_all(target)
    sparse_on_target = any(isinstance(v, SparseVector) for r in b.values() for v in (r[1] or {}).values())
    same_meta = all((a[i][1] if sparse_on_target else without_sparse(a[i][1])) == b[i][1] for i in a if i in b)
    # Relative difference: the stored float32 values can come back rounded (100.0 as 100.00001 in a cosine collection)
    max_diff = max((float(np.max(np.abs(a[i][2] - b[i][2]) / np.maximum(1.0, np.abs(a[i][2])))) for i in a if i in b), default=0.0)
    checks = {
        "count": (source.count(), target.count()),
        "ids": set(a) == set(b),
        "documents": all(a[i][0] == b[i][0] for i in a if i in b),
        "metadata" + ("" if sparse_on_target else " (sparse left out)"): same_meta,
        "uris": all(a[i][3] == b[i][3] for i in a if i in b),
        "embeddings max relative diff": max_diff,
    }
    # Three queries with the embeddings of the first three records. Records at the same distance can come back in
    # any order, so the check compares the distances of the top 3; the ids are printed when they differ.
    queries = [a[i][2].tolist() for i in sorted(a)[:3]]
    qa = source.query(query_embeddings=queries, n_results=3, include=["distances"])
    qb = target.query(query_embeddings=queries, n_results=3, include=["distances"])
    same = np.allclose(np.asarray(qa["distances"]), np.asarray(qb["distances"]), atol=1e-5)
    checks["dense queries, same top 3 distances"] = same
    ok = (checks["count"][0] == checks["count"][1] and checks["ids"] and checks["documents"] and same_meta
          and checks["uris"] and max_diff < 1e-6 and same)
    all_ok &= ok
    print(f"{'OK  ' if ok else 'DIFF'} {name}: {checks}")
    if not same:
        print(f"     Chroma Cloud: {qa['ids']} {np.round(qa['distances'], 6).tolist()}")
        print(f"     destination:  {qb['ids']} {np.round(qb['distances'], 6).tolist()}")
print("ALL OK" if all_ok else "SOME DIFFERENCES")
