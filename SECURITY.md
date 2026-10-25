# Security

## Supported versions

Security fixes go into the latest release of VecturaDB: the `VecturaDB` package on PyPI, the image `ghcr.io/vecturadb/vecturadb` and the CLI binaries of the GitHub releases. Earlier versions do not get fixes: update to the latest one.

## Reporting a vulnerability

Please do not report a vulnerability in a public issue, discussion or pull request.

Report it privately on GitHub instead: [Report a vulnerability](https://github.com/VecturaDB/VecturaDB/security/advisories/new), in the **Security** tab of this repository. Only the maintainers see the report.

Include the version of VecturaDB, how you run it (Python package, Docker image or CLI), and the steps to reproduce the problem.

VecturaDB was forked from Chroma. If a vulnerability also affects Chroma, we fix it here too, and we ask you to report it to the Chroma maintainers as well, through the contact they give for security issues.

## What happens next

- We acknowledge the report within 7 days.
- We confirm or rule out the vulnerability within 14 days, and keep you informed of the progress.
- A confirmed vulnerability is fixed in a new release. A GitHub security advisory follows the release, with credit to you unless you prefer otherwise.
- Please keep the details private until the advisory is published, and for at most 90 days from the report.

## How the releases are published

- Only the CI publishes, from the version tags of this repository: the wheels to PyPI with trusted publishing, so no PyPI token is kept by a person or in the repository; the image to ghcr.io; the CLI binaries and the wheels to the GitHub release.
- The workflows pin their actions to commit hashes, and Dependabot proposes their updates.
