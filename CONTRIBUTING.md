# Contributing

Issues and pull requests are welcome. To report a vulnerability, follow [SECURITY.md](SECURITY.md) instead.

## Build and test

[DEVELOP.md](DEVELOP.md) describes the setup. The CI runs:

- the Rust tests: `cargo nextest run --profile ci`
- the Python tests of the single node: `python -m pytest`

## Pull requests

- A fix comes with a test that fails without it.
- New functionality comes with tests in the automated test suite, which the CI runs on every pull request.
- The CI must pass on every job before a merge.
- The maintainers sign their commits.
- Code that comes from Chroma keeps its license header; a file changed for VecturaDB says so in its header, as the files already changed do.

Only the version tags publish a release: see [RELEASE_PROCESS.md](RELEASE_PROCESS.md).
