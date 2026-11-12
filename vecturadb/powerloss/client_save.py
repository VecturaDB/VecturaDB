# write <batches> [<pause after>]: one collection "col", batches of 100 records of 3 dimensions, printing each batch
# acknowledged; after the batch <pause after>, a pause of 31 seconds, for a server with hnsw_min_save_interval_seconds
# of 30, so that the batch after it saves the index again.
# read <batches>: every acknowledged record read back with its embedding, and a query on each 100th one.
import json, sys, time, urllib.request, urllib.error
B = "http://127.0.0.1:8000/api/v2/tenants/default_tenant/databases/default_database"
def call(m, p, body=None):
    r = urllib.request.Request(B + p, method=m, data=None if body is None else json.dumps(body).encode(), headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(r, timeout=60) as f: return json.loads(f.read() or b"null")
    except urllib.error.HTTPError as e: raise RuntimeError(f"{e.code} {e.read()[:300]!r}")
emb = lambda i: [i / 3000, (i % 7) / 7, (i % 11) / 11]
if sys.argv[1] == "write":
    c = call("POST", "/collections", {"name": "col", "get_or_create": True})
    pause_after = int(sys.argv[3]) if len(sys.argv) > 3 else 0
    for b in range(int(sys.argv[2])):
        ids = range(b * 100, b * 100 + 100)
        call("POST", f"/collections/{c['id']}/add", {"ids": [f"r{i}" for i in ids], "embeddings": [emb(i) for i in ids]})
        print(b + 1, flush=True)
        if b + 1 == pause_after:
            time.sleep(31)
else:
    n = int(sys.argv[2]) * 100
    c = call("GET", "/collections/col")
    errors = 0
    for s in range(0, n, 500):
        ids = [f"r{i}" for i in range(s, min(n, s + 500))]
        try:
            g = call("POST", f"/collections/{c['id']}/get", {"ids": ids, "include": ["embeddings"]})
            if len(g["ids"]) != len(ids): print("missing", len(ids) - len(g["ids"]), "from", s); errors += 1
        except Exception as e: print("get", s, e); errors += 1
    for i in range(0, n, 100):
        try:
            q = call("POST", f"/collections/{c['id']}/query", {"query_embeddings": [emb(i)], "n_results": 1, "include": ["distances"]})
            if q["ids"][0][0] != f"r{i}": print("query", i, q["ids"]); errors += 1
        except Exception as e: print("query", i, e); errors += 1
    print("acknowledged", n, "errors", errors)
    sys.exit(1 if errors else 0)
