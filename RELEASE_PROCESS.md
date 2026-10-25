<!-- Changed from the original Chroma file; see the git history for the changes and their authors. -->

# Release process

A release starts from a version tag on a commit of `main` whose CI passed: `vX.Y.Z`, or `vX.Y.Z-preview.N` for a preview. Any other tag stops the release workflow at its first job, before anything is published.

`.github/workflows/release.yml` then:

1. runs the whole CI with the version of the tag. The CLI binaries carry the version as written (`1.0.0-preview.1`); the wheels carry its PEP 440 form (`1.0.0rc1`).
2. Only if the CI passes, three jobs run in parallel:
   - the image `ghcr.io/vecturadb/vecturadb` for amd64 and arm64, tagged with the version without the `v` (`1.0.0-preview.1`), and with `preview` for a preview, `latest` otherwise;
   - the GitHub release, with the five CLI binaries, the five wheels and notes generated from the merged pull requests; a preview is a prerelease;
   - the wheels to PyPI with trusted publishing, in the environment `pypi`.

The three publishing jobs do not wait for each other. If one fails after another has published, do not reuse the tag: PyPI does not accept the same file twice. Fix the problem and release the next version.

After a release, set `rust/cli/Cargo.toml` and `Cargo.lock` on `main` to the next version, so that builds of `main` carry it.

The workflow and the environment keep their names: PyPI trusted publishing is tied to both.
