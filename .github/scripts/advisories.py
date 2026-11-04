"""The advisories that VecturaDB has to look at.

- The advisories on Chroma. VecturaDB was forked from Chroma, so an advisory on the chromadb package or on the Chroma
  server is an advisory on our code, and a scan of our dependencies does not find it. Some have no package in the
  databases, and their text says Chroma, not chromadb: NVD finds them by the word chroma, and the script keeps the ones
  whose text or links name chromadb, chroma-core or trychroma.
- The advisories on the dependencies of what the release builds, from osv-scanner:
  - the whole Cargo.lock, from which the CLI, the image and the wheel are built, with the crates that only the
    distributed services and the tests use;
  - the dependencies that pyproject.toml declares for the wheel, at their minimum versions, the oldest that pip may
    install. A dependency without a minimum version and the packages that the dependencies bring are not scanned.
  The packages of the base image of the Docker image are not scanned.

An advisory has one or more ids (CVE, GHSA, PYSEC, RUSTSEC), and the databases do not all give the same ones: ids that
appear together belong to one advisory. The ids already looked at are in .github/advisories-seen.txt, one per line, with
an optional comment after #.
- Every day, an advisory is new if none of its ids is in that file.
- On a pull request, only the dependencies count. An advisory is new if the pull request brings it: the base branch does
  not have it, and the pull request does not add or change the line of one of its ids in the file.
A new advisory, or a database that cannot be read, makes the script exit with 1.

Usage, from the root of the repository: python3 .github/scripts/advisories.py [--pull-request] [--write]
  --pull-request  compares HEAD with HEAD^1, the head of the base branch in the merge commit of a pull request.
  --write         adds the ids found now to the file of the seen ids, after the new ones were looked at.
OSV_SCANNER is the path of osv-scanner, and GH_TOKEN a token for the GitHub API.
"""

import http.client
import json
import os
import pathlib
import re
import subprocess
import sys
import tempfile
import time
import tomllib
import urllib.error
import urllib.request
from typing import Any, Callable, Dict, Iterator, List, Optional, Set, Tuple

SEEN = ".github/advisories-seen.txt"
USAGE = "usage: python3 .github/scripts/advisories.py [--pull-request] [--write]"
Advisory = Tuple[str, List[str]]  # where it was found, and its ids
# What a database that does not answer, or does not answer its usual JSON, raises.
UNREADABLE = (OSError, http.client.HTTPException, ValueError, KeyError, TypeError)
# The words that make an advisory found by the word chroma an advisory on Chroma, in its text or in its links.
CHROMA = ("chromadb", "chroma-core", "trychroma")
REQUIREMENT = re.compile(r"\s*([A-Za-z0-9][A-Za-z0-9._-]*)\s*(?:\[[^\]]*\])?\s*([^;]*)")
MINIMUM = re.compile(r"(?:>=|==|~=)\s*([0-9][^,\s]*)")


def fetch(
    url: str, body: Optional[Dict[str, Any]] = None
) -> Tuple[Any, Dict[str, str]]:
    headers = {"User-Agent": "VecturaDB advisories", "Accept": "application/json"}
    if url.startswith("https://api.github.com/") and os.environ.get("GH_TOKEN"):
        headers["Authorization"] = "Bearer " + os.environ["GH_TOKEN"]
    data = None
    if body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    for attempt in range(3):
        try:
            request = urllib.request.Request(url, data=data, headers=headers)
            with urllib.request.urlopen(request, timeout=60) as response:
                # The names of the headers in lower case, however the server writes them.
                names = {k.lower(): v for k, v in response.headers.items()}
                return json.load(response), names
        except urllib.error.HTTPError as e:
            # A refused request stays refused. A rate limit or an error of the server may pass.
            if attempt == 2 or (e.code < 500 and e.code not in (403, 429)):
                raise
        except (OSError, http.client.HTTPException, ValueError):
            if attempt == 2:
                raise
        # Without a key, NVD answers 403 to more than 5 requests in 30 seconds: the last attempt comes 45 seconds later.
        time.sleep(15 * 2**attempt)
    raise AssertionError("unreachable")


def nvd() -> Iterator[Advisory]:
    start = 0
    while True:
        page, _ = fetch(
            "https://services.nvd.nist.gov/rest/json/cves/2.0"
            f"?keywordSearch=chroma&startIndex={start}"
        )
        for item in page["vulnerabilities"]:
            cve = item["cve"]
            text = " ".join(
                [d["value"] for d in cve["descriptions"]]
                + [r["url"] for r in cve.get("references", [])]
            ).lower()
            if any(word in text for word in CHROMA):
                yield "NVD, text chroma", [cve["id"]]
        start += page["resultsPerPage"]
        if page["resultsPerPage"] == 0 or start >= page["totalResults"]:
            return


def github() -> Iterator[Advisory]:
    url: Optional[
        str
    ] = "https://api.github.com/advisories?ecosystem=pip&affects=chromadb&per_page=100"
    while url:
        page, headers = fetch(url)
        for item in page:
            yield "GitHub, pip chromadb", [
                i for i in (item["ghsa_id"], item.get("cve_id")) if i
            ]
        url = None
        for link in headers.get("link", "").split(","):
            if 'rel="next"' in link:
                url = link[link.index("<") + 1 : link.index(">")]


def osv_chromadb() -> Iterator[Advisory]:
    query: Dict[str, Any] = {"package": {"name": "chromadb", "ecosystem": "PyPI"}}
    while True:
        page, _ = fetch("https://api.osv.dev/v1/query", query)
        for item in page.get("vulns", []):
            yield "OSV, PyPI chromadb", [item["id"], *item.get("aliases", [])]
        if not page.get("next_page_token"):
            return
        query["page_token"] = page["next_page_token"]


def git_show(revision: str, path: str) -> Optional[bytes]:
    result = subprocess.run(["git", "show", f"{revision}:{path}"], capture_output=True)
    return result.stdout if result.returncode == 0 else None


def minimums(dependencies: List[str]) -> List[str]:
    """Each dependency at its minimum version, as name==version; one without a minimum version is left out."""
    pinned = []
    for dependency in dependencies:
        requirement = REQUIREMENT.match(dependency)
        minimum = MINIMUM.search(requirement.group(2)) if requirement else None
        if requirement and minimum:
            pinned.append(f"{requirement.group(1)}=={minimum.group(1)}")
    return pinned


def dependencies(revision: str) -> Iterator[Advisory]:
    cargo_lock = git_show(revision, "Cargo.lock")
    pyproject = git_show(revision, "pyproject.toml")
    if cargo_lock is None or pyproject is None:
        raise FileNotFoundError(f"no Cargo.lock or pyproject.toml at {revision}")
    requirements = minimums(
        tomllib.loads(pyproject.decode())["project"]["dependencies"]
    )
    with tempfile.TemporaryDirectory() as folder:
        pathlib.Path(folder, "Cargo.lock").write_bytes(cargo_lock)
        pathlib.Path(folder, "requirements.txt").write_text("\n".join(requirements))
        output = os.path.join(folder, "osv.json")
        # osv-scanner exits with 1 when it finds vulnerabilities.
        scan = subprocess.run(
            [
                os.environ.get("OSV_SCANNER", "osv-scanner"),
                "scan",
                "source",
                "--no-resolve",
                "-L",
                os.path.join(folder, "Cargo.lock"),
                "-L",
                os.path.join(folder, "requirements.txt"),
                "--format",
                "json",
                "--output-file",
                output,
            ],
            capture_output=True,
            text=True,
        )
        if scan.returncode not in (0, 1):
            raise OSError(f"osv-scanner exited with {scan.returncode}: {scan.stderr}")
        with open(output, encoding="utf-8") as f:
            found = json.load(f)
    names = {"Cargo.lock": "Cargo.lock", "requirements.txt": "pyproject.toml"}
    for result in found.get("results", []):
        source = names[os.path.basename(result["source"]["path"])]
        for package in result.get("packages", []):
            name = f"{source}, {package['package']['name']} {package['package']['version']}"
            for item in package.get("vulnerabilities", []):
                yield name, [item["id"], *item.get("aliases", [])]


def seen_lines(text: str) -> Dict[str, str]:
    """The ids of the file of the seen ids, each with its line."""
    lines = {}
    for line in text.splitlines():
        advisory_id = line.split("#")[0].strip()
        if advisory_id:
            lines[advisory_id] = line
    return lines


def grouped(found: List[Advisory]) -> List[Tuple[Set[str], Set[str]]]:
    """The advisories found, as their ids and the places where they were found."""
    groups: List[Tuple[Set[str], Set[str]]] = []
    for where, item in found:
        ids, places = set(item), {where}
        for group in [g for g in groups if g[0] & ids]:
            ids |= group[0]
            places |= group[1]
            groups.remove(group)
        groups.append((ids, places))
    return groups


def main() -> int:
    flags = set(sys.argv[1:])
    if not flags <= {"--pull-request", "--write"}:
        print(USAGE)
        return 2
    with open(SEEN, encoding="utf-8") as f:
        text = f.read()
    seen = seen_lines(text)
    failed: List[str] = []

    def read(name: str, source: Callable[[], Iterator[Advisory]]) -> List[Advisory]:
        try:
            return list(source())
        except UNREADABLE as e:
            # A check that cannot read a database must not pass as a check that found nothing.
            print(f"::error::{name} could not be read: {e}")
            failed.append(name)
            return []

    found = read("osv-scanner", lambda: dependencies("HEAD"))
    if "--pull-request" in flags:
        base = read("osv-scanner on HEAD^1", lambda: dependencies("HEAD^1"))
        base_seen = seen_lines((git_show("HEAD^1", SEEN) or b"").decode())
        # A pull request accepts an advisory that it brings, even one that came back, by adding or changing its line.
        added = {i for i, line in seen.items() if base_seen.get(i) != line}
        known = {i for _, item in base for i in item} | added
    else:
        for name, source in (("NVD", nvd), ("GitHub", github), ("OSV", osv_chromadb)):
            found += read(name, source)
        known = set(seen)

    if "--write" in flags:
        if failed:
            # The ids of the database that could not be read would come back as new.
            return 1
        ids = {i for _, item in found for i in item} - set(seen)
        entries = {**seen, **{i: i for i in ids}}
        comments = [line for line in text.splitlines() if line.startswith("#")]
        with open(SEEN, "w", encoding="utf-8", newline="\n") as f:
            f.write("\n".join(comments + [entries[i] for i in sorted(entries)]) + "\n")
        print(f"{len(ids)} ids added to {SEEN}")
        return 0

    groups = grouped(found)
    new = [(ids, places) for ids, places in groups if not ids & known]
    lines = sorted(
        f"- {'; '.join(sorted(places))}: {', '.join(sorted(ids))}"
        for ids, places in new
    )
    lines += [f"- {name} could not be read" for name in failed]
    report = "\n".join([f"{len(groups)} advisories read, {len(new)} new", *lines])
    print(report)
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as f:
            f.write(report + "\n")
    return 1 if new or failed else 0


if __name__ == "__main__":
    sys.exit(main())
