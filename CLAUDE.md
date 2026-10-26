# Chroma Codebase Guidelines

## Commit Message Style

VecturaDB commits use `Area: what changes`, in plain words, as the history since the fork:

```
Single node: a collection by CRN answers the collection, or 404
Local storage: write each file whole, and on disk before its key is recorded
Release: a tag vX.Y.Z-preview.N.postM, for a post-release of a preview
```

- Subject: one line. The area is the component or path (`Single node`, `Local HNSW`, `Release`), and after the colon
  comes what the change does, as its user sees it. No `[TYPE](scope)` prefix.
- Blank line, then a body wrapped at 120 characters: what was wrong, what the change does, and the measured numbers
  where there are any.
- A commit that only adds the header of a changed Chroma file says so: `Mark the Chroma file changed by ...`.
- The commits of Chroma in the history keep their own format, `[TYPE](scope): Description`.

## Tilt-Backed Tests

Tests named `test_k8s_integration` depend on a local Tilt-managed Kubernetes
environment and should not be run blindly.

Before running these tests, ensure Tilt is already running and the local
services are ready. In this repo, the key readiness signals are:

- frontend healthcheck responds on `http://localhost:8000/api/v2/healthcheck`
- Spanner emulator is reachable on `localhost:9010`
- required Kubernetes pods in the `chroma` namespace are `Ready`

If a task requires running `test_k8s_integration` tests and Tilt is not
clearly ready yet, do one of these before running the tests:

- verify an existing Tilt environment is ready and then run the tests
- start or restart Tilt, wait for readiness, and only then run the tests

Do not interpret early connection failures from these tests as product bugs
until the Tilt dependency has been checked.
