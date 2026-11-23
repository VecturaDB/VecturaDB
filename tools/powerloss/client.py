# write <name>: a collection of 250 records of 3 dimensions. read <n>: every collection, each with 250 records, n in all.
import json, sys, urllib.request
B = "http://127.0.0.1:8000/api/v2/tenants/default_tenant/databases/default_database"
def call(m, p, body=None):
    r = urllib.request.Request(B + p, method=m, data=None if body is None else json.dumps(body).encode(), headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(r, timeout=60) as f: return json.loads(f.read() or b"null")
if sys.argv[1] == "write":
    c = call("POST", "/collections", {"name": sys.argv[2]})
    call("POST", f"/collections/{c['id']}/add", {"ids": [f"r{i}" for i in range(250)], "embeddings": [[i / 250, (i % 7) / 7, (i % 11) / 11] for i in range(250)]})
else:
    found = {c["name"]: call("GET", f"/collections/{c['id']}/count") for c in call("GET", "/collections")}
    print("round", sys.argv[2], found)
    sys.exit(0 if found == {f"rep-{i}": 250 for i in range(1, int(sys.argv[2]) + 1)} else 1)
