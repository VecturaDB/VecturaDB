# Copyright VecturaDB contributors. Licensed under the Apache License, Version 2.0.
# Usage: python smoke.py <port>. Heartbeat, add, query against a single-node server, with the Python HttpClient.
import sys, uuid
import chromadb

port = int(sys.argv[1])
c = chromadb.HttpClient(host=sys.argv[2] if len(sys.argv) > 2 else "127.0.0.1", port=port)
print("heartbeat", c.heartbeat(), "version", c.get_version())
name = "smoke-" + uuid.uuid4().hex[:8]
col = c.create_collection(name, embedding_function=None)
col.add(ids=["a", "b", "c"], embeddings=[[1, 0, 0], [0, 1, 0], [0, 0, 1]], documents=["one", "two", "three"], metadatas=[{"n": 1}, {"n": 2}, {"n": 3}])
q = col.query(query_embeddings=[[0.9, 0.1, 0]], n_results=2, include=["documents", "distances"])
ok = q["ids"][0][0] == "a" and col.count() == 3
print("count", col.count(), "query", q["ids"][0], q["documents"][0])
c.delete_collection(name)
print("PYTHON OK" if ok else "PYTHON FAIL")
sys.exit(0 if ok else 1)
