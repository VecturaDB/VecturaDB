"""The import name vecturadb: the same package as chromadb, with the same module objects."""

import importlib
import importlib.util
import os
import pickle
import re
import subprocess
import sys
import textwrap
from pathlib import Path
from types import ModuleType

import chromadb
import chromadb.api.fastapi
import chromadb.config

# The folder that holds the chromadb that the tests import, and vecturadb beside it: the repository, or site-packages
# when the tests import the installed package.
PACKAGES = Path(chromadb.__file__).resolve().parent.parent
REPOSITORY = Path(__file__).resolve().parents[2]
REGENERATE = "python tools/vecturadb_reexports.py"

# In a fresh interpreter: every vecturadb.* module in sys.modules is the chromadb.* module, and no file of chromadb is
# loaded by two module objects.
CHECK_NO_DUPLICATES = """
import sys
aliases = [
    name for name, module in list(sys.modules.items())
    if name.startswith("vecturadb.") and module is not sys.modules.get("chromadb." + name[len("vecturadb."):])
]
assert not aliases, aliases
files = {}
for module in {id(m): m for m in list(sys.modules.values()) if m is not None}.values():
    path = getattr(module, "__file__", None)
    if path:
        files.setdefault(path, []).append(module.__name__)
twice = {path: names for path, names in files.items() if len(names) > 1}
assert not twice, twice
"""


def generator() -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        "vecturadb_reexports", REPOSITORY / "tools" / "vecturadb_reexports.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def run_python(*args: str, cwd: Path) -> "subprocess.CompletedProcess[str]":
    env = {**os.environ, "PYTHONPATH": str(PACKAGES)}
    return subprocess.run(
        [sys.executable, *args],
        cwd=cwd,
        env=env,
        capture_output=True,
        text=True,
        timeout=300,
    )


def test_vecturadb_is_chromadb() -> None:
    import vecturadb
    import vecturadb.config

    assert vecturadb is chromadb
    assert vecturadb.config is chromadb.config
    assert sys.modules["vecturadb.config"] is chromadb.config
    assert vecturadb.HttpClient is chromadb.HttpClient
    # Settings is not exported by chromadb for mypy in strict mode, nor by vecturadb.
    assert vecturadb.Settings is chromadb.config.Settings  # type: ignore[attr-defined]
    assert vecturadb.__version__ == chromadb.__version__
    # The modules keep their own name and spec: the vecturadb name does not change them.
    assert chromadb.config.__name__ == "chromadb.config"
    assert chromadb.config.__spec__ is not None
    assert chromadb.config.__spec__.name == "chromadb.config"


def test_submodule_from_vecturadb_first(tmp_path: Path) -> None:
    # A fresh interpreter, which imports the vecturadb names before the chromadb ones.
    code = textwrap.dedent(
        """
        import vecturadb.config
        import vecturadb.api.types as types
        from vecturadb.config import Settings
        import chromadb.config
        import chromadb.api.types
        assert vecturadb.config is chromadb.config
        assert types is chromadb.api.types
        assert Settings is chromadb.config.Settings
        assert chromadb.config.__spec__.name == "chromadb.config"
        assert chromadb.api.types.__spec__.name == "chromadb.api.types"
        """
    )
    result = run_python("-c", code + CHECK_NO_DUPLICATES, cwd=tmp_path)
    assert result.returncode == 0, result.stderr


def test_isinstance_and_pickle_across_the_names() -> None:
    import vecturadb.config

    settings = vecturadb.config.Settings(allow_reset=True)
    assert isinstance(settings, chromadb.config.Settings)
    data = pickle.dumps(settings)
    # The pickle names the module of chromadb, so it loads with either import name.
    assert b"chromadb.config" in data and b"vecturadb" not in data
    loaded = pickle.loads(data)
    assert isinstance(loaded, vecturadb.config.Settings)
    assert loaded.allow_reset is True


def test_class_names_in_the_settings() -> None:
    cls = chromadb.config.get_class("vecturadb.api.fastapi.FastAPI", object)
    assert cls is chromadb.api.fastapi.FastAPI


def test_missing_submodule() -> None:
    import vecturadb  # noqa: F401

    try:
        importlib.import_module("vecturadb.no_such_module")
    except ModuleNotFoundError as error:
        assert error.name == "vecturadb.no_such_module"
    else:
        raise AssertionError("vecturadb.no_such_module was imported")


def test_python_m(tmp_path: Path) -> None:
    alias = run_python("-m", "vecturadb.errors", cwd=tmp_path)
    original = run_python("-m", "chromadb.errors", cwd=tmp_path)
    assert alias.returncode == 0, alias.stderr
    assert (alias.returncode, alias.stdout) == (original.returncode, original.stdout)


def test_threads_import_both_names_at_once(tmp_path: Path) -> None:
    # The finder is already in sys.meta_path; for each module not imported yet, two threads import it at the same
    # time, one with each name, and get one module object.
    code = textwrap.dedent(
        """
        import importlib
        import pathlib
        import sys
        import threading
        import vecturadb

        sys.path.append(sys.argv[1])
        import vecturadb_reexports

        names = vecturadb_reexports.chromadb_modules(pathlib.Path(sys.argv[2]))
        raced = 0
        for name in names:
            if "chromadb." + name in sys.modules:
                continue
            barrier = threading.Barrier(2)
            results = {}

            def load(prefix):
                barrier.wait()
                try:
                    results[prefix] = importlib.import_module(prefix + name)
                except ImportError as error:  # a module that needs an optional dependency
                    results[prefix] = error

            threads = [threading.Thread(target=load, args=(p,)) for p in ("vecturadb.", "chromadb.")]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(60)
                assert not thread.is_alive(), name
            alias, original = results["vecturadb."], results["chromadb."]
            if isinstance(original, ImportError):
                assert isinstance(alias, ImportError), name
                continue
            assert alias is original is sys.modules["chromadb." + name], name
            raced += 1
            if raced == 25:
                break
        assert raced >= 10, raced
        print(raced)
        """
    )
    result = run_python(
        "-c",
        code + CHECK_NO_DUPLICATES,
        str(REPOSITORY / "tools"),
        str(PACKAGES / "chromadb"),
        cwd=tmp_path,
    )
    assert result.returncode == 0, result.stderr


def test_reexports_match_the_modules_of_chromadb() -> None:
    # One re-export for every module of chromadb that is not a test, in the repository and, when the tests import the
    # installed package, in the installed one too.
    reexports = generator()
    trees = {REPOSITORY}
    if PACKAGES != REPOSITORY:
        trees.add(PACKAGES)
    for tree in sorted(trees):
        expected = reexports.expected_files(tree / "chromadb")
        actual = reexports.actual_files(tree / "vecturadb")
        missing = sorted(set(expected) - set(actual))
        extra = sorted(set(actual) - set(expected))
        different = sorted(
            r for r in set(expected) & set(actual) if expected[r] != actual[r]
        )
        assert not (missing or extra or different), (
            f"vecturadb/ in {tree} does not match the modules of chromadb (missing {missing}, extra {extra}, "
            f"different {different}): run {REGENERATE} from the root of the repository"
        )
        assert not [r for r in actual if r.startswith("test/")]


def test_pyproject_ships_vecturadb() -> None:
    pyproject = (REPOSITORY / "pyproject.toml").read_text(encoding="utf-8")
    for key in ("packages", "python-packages"):
        found = re.search(rf"^{key} = \[(.*)\]$", pyproject, re.MULTILINE)
        assert found is not None, key
        assert re.findall(r'"([^"]+)"', found.group(1)) == [
            "chromadb",
            "vecturadb",
        ], key
