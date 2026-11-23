# Copyright VecturaDB contributors. Licensed under the Apache License, Version 2.0.
"""Writes the re-exports of vecturadb/, so that type checkers and IDEs, which do not run the finder of
vecturadb/__init__.py, resolve vecturadb.X.Y as chromadb.X.Y, with the same names exported and the same names not
exported: one module for each module of chromadb, and the block of the names of chromadb/__init__.py in
vecturadb/__init__.py, under TYPE_CHECKING. At runtime the finder answers first and none of them runs.

For each module of chromadb, its names come from the module itself: "from chromadb.X import N as N" for the names that
type checkers export (those that it defines and those that it imports as the same name), "from chromadb.X import N"
for the names that it imports, which type checkers do not export unless __all__ lists them, its __all__, its plain
imports, with chromadb.Y as vecturadb.Y so that the submodule is an attribute, and its star imports.

Usage: python tools/vecturadb_reexports.py, from the root of the repository, after modules of chromadb are added,
moved or removed, or after their names change. chromadb/test/test_vecturadb_name.py fails when the files differ from
what this writes.
"""

import ast
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Set

HEADER = "# Copyright VecturaDB contributors. Licensed under the Apache License, Version 2.0.\n"
BEGIN = "    # The names of chromadb/__init__.py, written by tools/vecturadb_reexports.py.\n"
END = "    # End of the names written by tools/vecturadb_reexports.py.\n"
# The line length of black in pyproject.toml.
LINE_LENGTH = 88


def chromadb_modules(chromadb_dir: Path) -> List[str]:
    """The dotted names, without chromadb., of the modules of chromadb below the package: the .py files, and the .pyi
    files of the compiled modules. Not the tests, nor the modules generated from the .proto files, which are not in
    the repository."""
    names = set()
    for path in chromadb_dir.rglob("*.py*"):
        rel = path.relative_to(chromadb_dir)
        if (
            path.suffix not in (".py", ".pyi")
            or rel.parts[0] == "test"
            or "__pycache__" in rel.parts
        ):
            continue
        if path.stem.endswith(("_pb2", "_pb2_grpc")):
            continue
        if path.suffix == ".pyi" and path.with_suffix(".py").exists():
            continue
        parts = rel.with_suffix("").parts
        if parts[-1] == "__init__":
            parts = parts[:-1]
        if parts:
            names.add(".".join(parts))
    return sorted(names)


def _source(chromadb_dir: Path, name: Optional[str]) -> Path:
    base = chromadb_dir.joinpath(*name.split(".")) if name else chromadb_dir
    for candidate in (
        base / "__init__.py",
        base.with_suffix(".py"),
        base.with_suffix(".pyi"),
    ):
        if candidate.exists():
            return candidate
    raise FileNotFoundError(base)


def _statements(body: List[ast.stmt]) -> Iterator[ast.stmt]:
    """The statements at module level, also those in if, try and with blocks, which bind names of the module."""
    for node in body:
        yield node
        if isinstance(node, ast.If):
            yield from _statements(node.body)
            yield from _statements(node.orelse)
        elif isinstance(node, ast.Try):
            yield from _statements(node.body)
            for handler in node.handlers:
                yield from _statements(handler.body)
            yield from _statements(node.orelse)
            yield from _statements(node.finalbody)
        elif isinstance(node, ast.With):
            yield from _statements(node.body)


def _target_names(target: ast.expr) -> Iterator[str]:
    if isinstance(target, ast.Name):
        yield target.id
    elif isinstance(target, (ast.Tuple, ast.List)):
        for element in target.elts:
            yield from _target_names(element)


def _star_names(chromadb_dir: Path, module: str) -> List[str]:
    """The names that "from <module> import *" binds: the __all__ of a module of
    chromadb."""
    assert module.startswith("chromadb."), f"star import of {module}"
    name = module[len("chromadb.") :]
    tree = ast.parse(_source(chromadb_dir, name).read_text(encoding="utf-8"))
    for node in _statements(tree.body):
        if (
            isinstance(node, ast.Assign)
            and any(isinstance(t, ast.Name) and t.id == "__all__" for t in node.targets)
            and isinstance(node.value, (ast.List, ast.Tuple))
        ):
            return [ast.literal_eval(element) for element in node.value.elts]
    raise AssertionError(f"star import of {module}, which has no __all__")


def _imports_a_missing_module(chromadb_dir: Path, node: ast.Try) -> bool:
    for inner in node.body:
        if isinstance(inner, ast.ImportFrom) and str(inner.module).startswith(
            "chromadb."
        ):
            try:
                _source(chromadb_dir, str(inner.module)[len("chromadb.") :])
            except FileNotFoundError:
                return True
    return False


def _copy(node: ast.Try, source: List[str], indent: str) -> str:
    """The try block as it is in the module, its comments included, with those of
    flake8: its imports are unused (F401), and its lines bind again names of the lines
    above (F811)."""
    lines = []
    for line in source[node.lineno - 1 : node.end_lineno]:
        stripped = line.strip()
        if not stripped:
            lines.append("\n")
            continue
        if stripped.startswith(("from ", "import ")):
            line += "  # noqa: F401,F811"
        elif not stripped.startswith(("try:", "except", "else:", "finally:", "#")):
            line += "  # noqa: F811"
        lines.append(f"{indent}{line[node.col_offset :]}\n")
    return "".join(lines)


def reexport_lines(chromadb_dir: Path, name: Optional[str], indent: str) -> str:
    """The lines that re-export the module chromadb.<name> (chromadb itself for None), as black formats them."""
    module = "chromadb" + (f".{name}" if name else "")
    text = _source(chromadb_dir, name).read_text(encoding="utf-8")
    tree = ast.parse(text)
    exported: Set[str] = set()
    imported: Set[str] = set()
    # The names that it imports and then assigns: mypy keeps the import, which is not
    # exported, pyright the assignment, which is.
    reassigned: Set[str] = set()
    # The names that a star import binds first and an import binds again: mypy keeps
    # the star import, which exports them, pyright the import, which does not. They are
    # imported again after the star import, as in the module.
    starred: Set[str] = set()
    after_star: Set[str] = set()
    plain: List[str] = []
    star: List[str] = []
    all_names: Optional[List[str]] = None
    # The try blocks that import a module of chromadb that is not in the repository,
    # which type checkers resolve in ways of their own: copied as they are.
    copied: List[str] = []
    skip: Set[int] = set()
    for node in _statements(tree.body):
        if isinstance(node, ast.Try) and _imports_a_missing_module(chromadb_dir, node):
            copied.append(_copy(node, text.splitlines(), indent))
            skip |= {id(inner) for inner in ast.walk(node)}
    for node in _statements(tree.body):
        if id(node) in skip:
            continue
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.asname is None:
                    target = alias.name
                    if target.startswith("chromadb."):
                        target = "vecturadb." + target[len("chromadb.") :]
                    plain.append(f"import {target}")
                elif alias.asname == alias.name:
                    exported.add(alias.asname)
                else:
                    imported.add(alias.asname)
        elif isinstance(node, ast.ImportFrom):
            assert node.level == 0, f"relative import in {module}"
            for alias in node.names:
                if alias.name == "*":
                    star.append(f"from {node.module} import *")
                    # For mypy the first binding of a name counts: those that the star
                    # import binds first are exported, as the star import exports them.
                    first = _star_names(chromadb_dir, str(node.module))
                    starred |= {n for n in first if n not in imported | exported}
                elif alias.asname == alias.name:
                    exported.add(alias.name)
                elif (alias.asname or alias.name) in starred:
                    after_star.add(alias.asname or alias.name)
                else:
                    imported.add(alias.asname or alias.name)
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            exported.add(node.name)
        elif isinstance(node, (ast.Assign, ast.AnnAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            for assigned in targets:
                for target_name in _target_names(assigned):
                    if target_name == "__all__":
                        assert isinstance(
                            node.value, (ast.List, ast.Tuple)
                        ), f"__all__ of {module}"
                        all_names = [
                            ast.literal_eval(element) for element in node.value.elts
                        ]
                    elif target_name in imported and target_name not in exported:
                        reassigned.add(target_name)
                    else:
                        exported.add(target_name)
    after_star -= exported
    exported |= starred - after_star
    imported -= exported
    # For mypy in strict mode the names of __all__ are exported, the other imported
    # names are not.
    listed = set(all_names or [])
    lines = [
        _from_import(module, n, f"{n} as {n}", indent, True) for n in sorted(exported)
    ]
    lines += [_from_import(module, n, n, indent, n in listed) for n in sorted(imported)]
    lines += [
        f"{indent}{line}  # noqa: {_codes(line, indent)}\n"
        for line in sorted(set(plain))
    ]
    lines += [f"{indent}{line}  # noqa: F401,F403\n" for line in sorted(set(star))]
    # mypy exports them already, from the star import: no comment for mypy.
    lines += [_from_import(module, n, n, indent, True) for n in sorted(after_star)]
    body = copied + [f"{indent}{n} = {n}\n" for n in sorted(reassigned)]
    if all_names is not None:
        body.append(f"{indent}__all__ = [\n")
        body += [f'{indent}    "{n}",\n' for n in all_names]
        body.append(f"{indent}]\n")
    # black leaves a blank line between the imports and the next statement.
    if lines and body:
        lines.append("\n")
    return "".join(lines + body)


def _codes(bound: str, indent: str) -> str:
    """The flake8 codes that a re-export line silences: the import is unused (F401); in
    the block of vecturadb/__init__.py it can bind again a name that the finder imports
    (F811); and the names l, O and I that chromadb gives to type variables are ambiguous
    for flake8 (E741)."""
    codes = ["F401"]
    if indent:
        codes.append("F811")
    if bound.rsplit(" ", 1)[-1] in ("l", "O", "I"):
        codes.append("E741")
    return ",".join(codes)


def _from_import(
    module: str, bound: str, name: str, indent: str, exported: bool
) -> str:
    """One import as black writes it: on one line when it fits in 88 columns, else in
    parentheses. A name that chromadb.X imports without exporting it is not exported by
    chromadb.X for mypy in strict mode, which reports the import here as it reports it
    in the modules that import the name from chromadb.X: the comment silences it here,
    and the name stays not exported. exported is False for those names only."""
    noqa = f"# noqa: {_codes(bound, indent)}"
    if not exported:
        noqa = f"# type: ignore[attr-defined]  {noqa}"
    line = f"{indent}from {module} import {name}  {noqa}\n"
    if len(line) - 1 <= LINE_LENGTH:
        return line
    return f"{indent}from {module} import (  {noqa}\n{indent}    {name},\n{indent})\n"


def init_block(chromadb_dir: Path) -> str:
    """The block of vecturadb/__init__.py between BEGIN and END, those lines included."""
    return BEGIN + reexport_lines(chromadb_dir, None, "    ") + END


def init_block_of(text: str) -> str:
    start, end = text.find(BEGIN), text.find(END)
    return text[start : end + len(END)] if 0 <= start < end else ""


def expected_files(chromadb_dir: Path) -> Dict[str, str]:
    """The re-exports, by their path relative to vecturadb/; for __init__.py, only its block."""
    files = {"__init__.py": init_block(chromadb_dir)}
    for name in chromadb_modules(chromadb_dir):
        parts = name.split(".")
        is_package = (chromadb_dir.joinpath(*parts) / "__init__.py").exists()
        rel = "/".join(parts) + ("/__init__.py" if is_package else ".py")
        files[rel] = HEADER + reexport_lines(chromadb_dir, name, "")
    return files


def actual_files(vecturadb_dir: Path) -> Dict[str, str]:
    """The files of vecturadb/, by their path relative to it; for __init__.py, only its block."""
    files = {}
    for path in vecturadb_dir.rglob("*"):
        rel = path.relative_to(vecturadb_dir).as_posix()
        if not path.is_file() or "__pycache__" in path.parts or rel == "py.typed":
            continue
        text = path.read_text(encoding="utf-8").replace("\r\n", "\n")
        files[rel] = init_block_of(text) if rel == "__init__.py" else text
    return files


def main() -> None:
    root = Path(__file__).resolve().parent.parent
    vecturadb_dir = root / "vecturadb"
    expected = expected_files(root / "chromadb")
    actual = actual_files(vecturadb_dir)
    for rel in sorted(set(actual) - set(expected)):
        (vecturadb_dir / rel).unlink()
        print(f"removed vecturadb/{rel}")
    for rel, text in expected.items():
        if actual.get(rel) == text:
            continue
        path = vecturadb_dir / rel
        if rel == "__init__.py":
            current = path.read_text(encoding="utf-8").replace("\r\n", "\n")
            block = init_block_of(current)
            assert (
                block
            ), f"vecturadb/__init__.py has no block between {BEGIN.strip()!r} and {END.strip()!r}"
            text = current.replace(block, text)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8", newline="\n") as file:
            file.write(text)
        print(f"wrote vecturadb/{rel}")
    for path in sorted(vecturadb_dir.rglob("*"), reverse=True):
        if path.is_dir() and path.name != "__pycache__" and not any(path.iterdir()):
            path.rmdir()


if __name__ == "__main__":
    main()
