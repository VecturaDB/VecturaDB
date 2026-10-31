# Changed from the original Chroma file; see the git history for the changes and their authors.
import hashlib
import io
import shutil
import os
import tarfile
import threading
import warnings
from pathlib import Path
from typing import List, Hashable, Type, cast

import hypothesis.strategies as st
import numpy as np
import onnxruntime
import pytest
from hypothesis import given, settings
from onnxruntime.datasets import get_example

import chromadb.utils.embedding_functions.onnx_mini_lm_l6_v2 as onnx_mini_lm_l6_v2
from chromadb.api.types import DefaultEmbeddingFunction, Documents, Embeddings
from chromadb.utils.embedding_functions.onnx_mini_lm_l6_v2 import (
    ONNXMiniLM_L6_V2,
)

from chromadb.utils.embedding_functions.onnx_mini_lm_l6_v2 import _verify_sha256


def unique_by(x: Hashable) -> Hashable:
    return x


ONNX_FILES = [
    "config.json",
    "model.onnx",
    "special_tokens_map.json",
    "tokenizer_config.json",
    "tokenizer.json",
    "vocab.txt",
]


def _write_tiny_onnx_archive(path: Path) -> str:
    model_bytes = Path(get_example("mul_1.onnx")).read_bytes()
    archive_files = {
        "config.json": b"{}",
        "model.onnx": model_bytes,
        "special_tokens_map.json": b"{}",
        "tokenizer_config.json": b"{}",
        "tokenizer.json": b"{}",
        "vocab.txt": b"",
    }

    with tarfile.open(path, "w:gz") as tar:
        for filename, contents in archive_files.items():
            tarinfo = tarfile.TarInfo(f"onnx/{filename}")
            tarinfo.size = len(contents)
            tar.addfile(tarinfo, io.BytesIO(contents))

    return hashlib.sha256(path.read_bytes()).hexdigest()


def _use_tiny_model_download(
    ef: ONNXMiniLM_L6_V2, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> str:
    archive = tmp_path / "onnx.tar.gz"
    expected_sha256 = _write_tiny_onnx_archive(archive)
    download_path = tmp_path / "model-cache"
    monkeypatch.setattr(ef, "DOWNLOAD_PATH", download_path)

    def download(url: str, fname: str, chunk_size: int = 1024) -> None:
        shutil.copyfile(archive, fname)
        if not _verify_sha256(fname, ef._MODEL_SHA256):
            os.remove(fname)
            raise ValueError(
                f"Downloaded file {fname} does not match expected SHA256 hash. Corrupted download or malicious file."
            )

    monkeypatch.setattr(ef, "_download", download)
    return expected_sha256


@settings(deadline=None)
@given(
    providers=st.lists(
        st.sampled_from(onnxruntime.get_all_providers()).filter(
            lambda x: x not in onnxruntime.get_available_providers()
        ),
        unique_by=unique_by,
        min_size=1,
    )
)
def test_unavailable_provider_multiple(providers: List[str]) -> None:
    with pytest.raises(ValueError) as e:
        ef = ONNXMiniLM_L6_V2(preferred_providers=providers)
        ef(["test"])
    assert "Preferred providers must be subset of available providers" in str(e.value)


@given(
    providers=st.lists(
        st.sampled_from(onnxruntime.get_available_providers()),
        min_size=1,
        unique_by=unique_by,
    )
)
def test_available_provider(providers: List[str]) -> None:
    ef = ONNXMiniLM_L6_V2(preferred_providers=providers)
    ef(["test"])


def test_warning_no_providers_supplied() -> None:
    ef = ONNXMiniLM_L6_V2()
    ef(["test"])


@given(
    providers=st.lists(
        st.sampled_from(onnxruntime.get_available_providers()),
        min_size=1,
    ).filter(lambda x: len(x) > len(set(x)))
)
def test_provider_repeating(providers: List[str]) -> None:
    with pytest.raises(ValueError) as e:
        ef = ONNXMiniLM_L6_V2(preferred_providers=providers)
        ef(["test"])
    assert "Preferred providers must be unique" in str(e.value)


def test_invalid_sha256(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    ef = ONNXMiniLM_L6_V2()
    _use_tiny_model_download(ef, tmp_path, monkeypatch)
    with pytest.raises(ValueError) as e:
        ef._MODEL_SHA256 = "invalid"
        ef._download_model_if_not_exists()
    assert "does not match expected SHA256 hash" in str(e.value)


def test_partial_download(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    ef = ONNXMiniLM_L6_V2()
    ef._MODEL_SHA256 = _use_tiny_model_download(ef, tmp_path, monkeypatch)
    os.makedirs(ef.DOWNLOAD_PATH, exist_ok=True)
    path = os.path.join(ef.DOWNLOAD_PATH, ef.ARCHIVE_FILENAME)
    with open(path, "wb") as f:  # create invalid file to simulate partial download
        f.write(b"invalid")
    ef._download_model_if_not_exists()  # re-download model
    assert os.path.exists(path)
    assert _verify_sha256(
        str(os.path.join(ef.DOWNLOAD_PATH, ef.ARCHIVE_FILENAME)),
        ef._MODEL_SHA256,
    )
    for filename in ONNX_FILES:
        assert os.path.exists(
            os.path.join(ef.DOWNLOAD_PATH, ef.EXTRACTED_FOLDER_NAME, filename)
        )


class _CountingModel:
    """Stands for ONNXMiniLM_L6_V2 without a model: notes each instance and each call."""

    built: List["_CountingModel"] = []

    def __init__(self) -> None:
        _CountingModel.built.append(self)
        self.calls: List[int] = []

    def __call__(self, input: Documents) -> Embeddings:
        self.calls.append(len(input))
        return [np.array([float(len(d))], dtype=np.float32) for d in input]


@pytest.fixture
def counting_model(monkeypatch: pytest.MonkeyPatch) -> Type[_CountingModel]:
    monkeypatch.setattr(onnx_mini_lm_l6_v2, "ONNXMiniLM_L6_V2", _CountingModel)
    monkeypatch.setattr(DefaultEmbeddingFunction, "models", {}, raising=False)
    monkeypatch.setattr(_CountingModel, "built", [])
    return _CountingModel


def _called(model: Type[_CountingModel]) -> List[_CountingModel]:
    return [m for m in model.built if m.calls]


def test_default_ef_keeps_one_model_across_calls_and_instances(
    counting_model: Type[_CountingModel],
) -> None:
    first, second = DefaultEmbeddingFunction(), DefaultEmbeddingFunction()
    # Nothing is loaded before the first call.
    assert counting_model.built == []
    first(["a"])
    first(["bb", "cc"])
    second(["ddd"])
    called = _called(counting_model)
    assert len(called) == 1
    assert called[0].calls == [1, 2, 1]


def test_default_ef_shares_its_model_between_threads(
    counting_model: Type[_CountingModel],
) -> None:
    ef = DefaultEmbeddingFunction()
    start = threading.Barrier(8)
    results: List[Embeddings] = []

    def call() -> None:
        start.wait()
        results.append(ef(["thread"]))

    threads = [threading.Thread(target=call) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    called = _called(counting_model)
    assert len(called) == 1
    assert len(called[0].calls) == 8
    assert len(results) == 8
    assert all(np.array_equal(r[0], results[0][0]) for r in results)


@pytest.mark.skipif(not hasattr(os, "fork"), reason="needs os.fork")
def test_default_ef_builds_its_own_model_after_fork(
    counting_model: Type[_CountingModel],
) -> None:
    ef = DefaultEmbeddingFunction()
    ef(["parent"])
    parent_model = DefaultEmbeddingFunction.models[os.getpid()]
    with warnings.catch_warnings():
        # Python 3.12 warns about fork() in a process with threads.
        warnings.simplefilter("ignore", DeprecationWarning)
        pid = os.fork()
    if pid == 0:
        code = 2
        try:
            ef(["child"])
            child_model = DefaultEmbeddingFunction.models[os.getpid()]
            code = 0 if child_model is not parent_model else 1
        finally:
            os._exit(code)
    _, status = os.waitpid(pid, 0)
    assert os.waitstatus_to_exitcode(status) == 0
    assert cast(_CountingModel, parent_model).calls == [1]


def test_default_ef_gives_the_vectors_of_onnx_mini_lm_l6_v2() -> None:
    documents = [
        "the bike will not come out of the dock",
        "E17",
        "a longer document " * 20,
    ]
    expected = ONNXMiniLM_L6_V2()(documents)
    ef = DefaultEmbeddingFunction()
    # The first call builds the model of the process, the second one uses it again.
    for _ in range(2):
        got = ef(documents)
        assert len(got) == len(expected)
        assert all(np.array_equal(g, e) for g, e in zip(got, expected))


def test_onnx_session_has_no_cpu_memory_arena() -> None:
    ef = ONNXMiniLM_L6_V2()
    ef(["test"])
    # A kept instance would otherwise hold the arena of its largest batch.
    assert ef.model.get_session_options().enable_cpu_mem_arena is False
