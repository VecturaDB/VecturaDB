# vecturadb

VecturaDB is a vector database compatible with the Chroma API: the features of Chroma Cloud, that is, of distributed Chroma, on a single node.

VecturaDB is an independent fork of [Chroma](https://github.com/chroma-core/chroma), under the Apache License 2.0. The original code is copyright Chroma Inc. and its contributors. Independent project. Not affiliated with, endorsed by or sponsored by Chroma Inc. CHROMA is a registered trademark of Chroma Inc., used here only to describe compatibility.

This package runs the VecturaDB server from npm, without Docker:

```bash
npm install vecturadb
npx vecturadb run --path ./data
```

The client is the `chromadb` package on npm, the same as for Chroma:

```bash
npm install chromadb
```

```js
import { ChromaClient } from "chromadb";

const client = new ChromaClient({ host: "localhost", port: 8000 });
```

- With `vecturadb`, npm installs the CLI package for your platform: macOS arm64 and x64, Linux arm64 and x64 (glibc 2.17 or later), Windows x64. There is none for Alpine (musl): use the Docker image there.
- The CLI is the same binary as the [GitHub Release](https://github.com/VecturaDB/VecturaDB/releases) of the same version, so every CLI command works, such as `npx vecturadb copy`.
- `npx vecturadb update` says whether npm has a newer `vecturadb`.
- With `--omit=optional`, npm leaves out the CLI package, and the command says so.

While only previews exist, `npm install vecturadb` installs the newest preview.

More in the [README of VecturaDB](https://github.com/VecturaDB/VecturaDB#readme).
