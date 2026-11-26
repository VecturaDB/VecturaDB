# Copyright VecturaDB contributors. Licensed under the Apache License, Version 2.0.
"""The import name vecturadb, the same package as chromadb.

``import vecturadb`` gives the module ``chromadb`` itself, and ``import vecturadb.X.Y`` the module ``chromadb.X.Y``: the
same objects, so the classes, isinstance, pickle and the class names in the settings work with either name, and
``import chromadb`` does not change. The finder below maps the names at runtime. For type checkers and IDEs, which do
not run finders, the block under TYPE_CHECKING re-exports the names of chromadb, and each other module of this folder
those of the module of chromadb with its name: at runtime the block does not run, and the finder answers first, so the
modules never run. ``python tools/vecturadb_reexports.py`` writes them again from the modules of chromadb.
"""

import importlib
import importlib.abc
import importlib.machinery
import importlib.util
import sys
from types import CodeType, ModuleType
from typing import TYPE_CHECKING, Optional, Sequence

import chromadb

if TYPE_CHECKING:
    # The names of chromadb/__init__.py, written by tools/vecturadb_reexports.py.
    from chromadb import AdminClient as AdminClient  # noqa: F401,F811
    from chromadb import AsyncHttpClient as AsyncHttpClient  # noqa: F401,F811
    from chromadb import Client as Client  # noqa: F401,F811
    from chromadb import CloudClient as CloudClient  # noqa: F401,F811
    from chromadb import EphemeralClient as EphemeralClient  # noqa: F401,F811
    from chromadb import HttpClient as HttpClient  # noqa: F401,F811
    from chromadb import IN_COLAB as IN_COLAB  # noqa: F401,F811
    from chromadb import PersistentClient as PersistentClient  # noqa: F401,F811
    from chromadb import RustClient as RustClient  # noqa: F401,F811
    from chromadb import __settings as __settings  # noqa: F401,F811
    from chromadb import __version__ as __version__  # noqa: F401,F811
    from chromadb import configure as configure  # noqa: F401,F811
    from chromadb import get_settings as get_settings  # noqa: F401,F811
    from chromadb import is_client as is_client  # noqa: F401,F811
    from chromadb import is_in_colab as is_in_colab  # noqa: F401,F811
    from chromadb import logger as logger  # noqa: F401,F811
    from chromadb import AdminAPI  # type: ignore[attr-defined]  # noqa: F401,F811
    from chromadb import (  # type: ignore[attr-defined]  # noqa: F401,F811
        AdminClientCreator,
    )
    from chromadb import AsyncClientAPI  # type: ignore[attr-defined]  # noqa: F401,F811
    from chromadb import (  # type: ignore[attr-defined]  # noqa: F401,F811
        AsyncClientCreator,
    )
    from chromadb import BoolInvertedIndexConfig  # noqa: F401,F811
    from chromadb import ClientAPI  # type: ignore[attr-defined]  # noqa: F401,F811
    from chromadb import ClientCreator  # type: ignore[attr-defined]  # noqa: F401,F811
    from chromadb import CloudClientArg  # type: ignore[attr-defined]  # noqa: F401,F811
    from chromadb import Cmek  # noqa: F401,F811
    from chromadb import Collection  # noqa: F401,F811
    from chromadb import CollectionMetadata  # noqa: F401,F811
    from chromadb import ConditionalCommitResult  # noqa: F401,F811
    from chromadb import (  # type: ignore[attr-defined]  # noqa: F401,F811
        DEFAULT_DATABASE,
    )
    from chromadb import DEFAULT_TENANT  # type: ignore[attr-defined]  # noqa: F401,F811
    from chromadb import Dict  # type: ignore[attr-defined]  # noqa: F401,F811
    from chromadb import Documents  # noqa: F401,F811
    from chromadb import EmbeddingFunction  # noqa: F401,F811
    from chromadb import Embeddings  # noqa: F401,F811
    from chromadb import FloatInvertedIndexConfig  # noqa: F401,F811
    from chromadb import FtsIndexConfig  # noqa: F401,F811
    from chromadb import GetResult  # noqa: F401,F811
    from chromadb import HnswIndexConfig  # noqa: F401,F811
    from chromadb import IDs  # noqa: F401,F811
    from chromadb import Include  # noqa: F401,F811
    from chromadb import IntInvertedIndexConfig  # noqa: F401,F811
    from chromadb import K  # noqa: F401,F811
    from chromadb import Key  # noqa: F401,F811
    from chromadb import Knn  # noqa: F401,F811
    from chromadb import Metadata  # noqa: F401,F811
    from chromadb import Metadatas  # noqa: F401,F811
    from chromadb import Optional  # type: ignore[attr-defined]  # noqa: F401,F811
    from chromadb import Path  # type: ignore[attr-defined]  # noqa: F401,F811
    from chromadb import QueryResult  # noqa: F401,F811
    from chromadb import ReadLevel  # type: ignore[attr-defined]  # noqa: F401,F811
    from chromadb import Rrf  # noqa: F401,F811
    from chromadb import Schema  # noqa: F401,F811
    from chromadb import Search  # noqa: F401,F811
    from chromadb import Settings  # type: ignore[attr-defined]  # noqa: F401,F811
    from chromadb import SpannIndexConfig  # noqa: F401,F811
    from chromadb import SparseEmbeddingFunction  # noqa: F401,F811
    from chromadb import SparseVector  # noqa: F401,F811
    from chromadb import SparseVectorIndexConfig  # noqa: F401,F811
    from chromadb import SparseVectors  # noqa: F401,F811
    from chromadb import StringInvertedIndexConfig  # noqa: F401,F811
    from chromadb import TokenTransportHeader  # noqa: F401,F811
    from chromadb import URI  # noqa: F401,F811
    from chromadb import URIs  # noqa: F401,F811
    from chromadb import Union  # type: ignore[attr-defined]  # noqa: F401,F811
    from chromadb import UpdateCollectionMetadata  # noqa: F401,F811
    from chromadb import UpdateMetadata  # noqa: F401,F811
    from chromadb import VectorIndexConfig  # noqa: F401,F811
    from chromadb import Where  # noqa: F401,F811
    from chromadb import WhereDocument  # noqa: F401,F811
    import logging  # noqa: F401,F811
    import os  # noqa: F401,F811
    import sqlite3  # noqa: F401,F811
    import subprocess  # noqa: F401,F811
    import sys  # noqa: F401,F811
    import vecturadb.config  # noqa: F401,F811

    try:
        from chromadb.is_thin_client import is_thin_client  # noqa: F401,F811

        is_client = is_thin_client  # noqa: F811
    except ImportError:
        is_client = False  # noqa: F811
    __all__ = [
        "Cmek",
        "Collection",
        "Metadata",
        "Metadatas",
        "Where",
        "WhereDocument",
        "Documents",
        "IDs",
        "URI",
        "URIs",
        "Embeddings",
        "EmbeddingFunction",
        "Include",
        "CollectionMetadata",
        "UpdateMetadata",
        "UpdateCollectionMetadata",
        "QueryResult",
        "GetResult",
        "ConditionalCommitResult",
        "TokenTransportHeader",
        "Search",
        "Key",
        "K",
        "Knn",
        "Rrf",
        "SparseVector",
        "SparseVectors",
        "SparseEmbeddingFunction",
        "Schema",
        "VectorIndexConfig",
        "HnswIndexConfig",
        "SpannIndexConfig",
        "FtsIndexConfig",
        "SparseVectorIndexConfig",
        "StringInvertedIndexConfig",
        "IntInvertedIndexConfig",
        "FloatInvertedIndexConfig",
        "BoolInvertedIndexConfig",
    ]
    # End of the names written by tools/vecturadb_reexports.py.
    pass

_ALIAS = "vecturadb."
_TARGET = "chromadb."


class _AliasLoader(importlib.abc.Loader):
    """Returns the module of chromadb itself, imported by its own name, for the vecturadb name."""

    def __init__(self, target: str) -> None:
        self._target = target
        self._target_spec: Optional[importlib.machinery.ModuleSpec] = None

    def create_module(self, spec: importlib.machinery.ModuleSpec) -> ModuleType:
        # Imported under the lock of the chromadb name: a thread that imports chromadb.X at the same time gets the
        # same module, and the file of the module is never loaded twice.
        module = importlib.import_module(self._target)
        self._target_spec = module.__spec__
        return module

    def exec_module(self, module: ModuleType) -> None:
        # module_from_spec has set the spec of the vecturadb name on the module: the module keeps its own, so that
        # importlib.reload and the spec of chromadb.X do not change.
        module.__spec__ = self._target_spec

    def get_code(self, fullname: str) -> Optional[CodeType]:
        # python -m vecturadb.X runs the code of chromadb.X.
        spec = importlib.util.find_spec(self._target)
        get_code = getattr(spec.loader if spec is not None else None, "get_code", None)
        return get_code(self._target) if get_code is not None else None


class _AliasFinder(importlib.abc.MetaPathFinder):
    """Maps vecturadb.X.Y to chromadb.X.Y; first in sys.meta_path, before the path finder, which would otherwise load
    the files of chromadb a second time under the vecturadb name, from the __path__ of chromadb.
    """

    def find_spec(
        self,
        fullname: str,
        path: Optional[Sequence[str]],
        target: Optional[ModuleType] = None,
    ) -> Optional[importlib.machinery.ModuleSpec]:
        if not fullname.startswith(_ALIAS):
            return None
        name = _TARGET + fullname[len(_ALIAS) :]
        target_spec = importlib.util.find_spec(name)
        if target_spec is None:
            return None
        return importlib.machinery.ModuleSpec(
            fullname,
            _AliasLoader(name),
            is_package=target_spec.submodule_search_locations is not None,
        )


if not any(isinstance(finder, _AliasFinder) for finder in sys.meta_path):
    sys.meta_path.insert(0, _AliasFinder())
sys.modules["vecturadb"] = chromadb
