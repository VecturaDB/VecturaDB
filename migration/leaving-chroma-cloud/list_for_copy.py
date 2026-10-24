# Copyright VecturaDB contributors. Licensed under the Apache License, Version 2.0.
# Prints the --collections options for chroma copy, one per collection of a Chroma Cloud database.
# Usage: python list_for_copy.py   (CHROMA_API_KEY, CHROMA_TENANT and CHROMA_DATABASE in the environment)
import os

import chromadb

client = chromadb.CloudClient(api_key=os.environ["CHROMA_API_KEY"], tenant=os.environ["CHROMA_TENANT"],
                              database=os.environ["CHROMA_DATABASE"])
names, offset = [], 0
while True:
    page = client.list_collections(limit=1000, offset=offset)  # Chroma Cloud lists at most 1000 per call
    names += [c.name for c in page]
    if len(page) < 1000:
        break
    offset += 1000
print(" ".join(f"--collections {name}" for name in names))
