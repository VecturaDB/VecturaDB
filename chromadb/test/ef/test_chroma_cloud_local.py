"""chroma-cloud-qwen and chroma-cloud-splade computed on this machine: the configuration and the schema that Chroma Cloud stored,
the cache of the model files, and the vectors against those that Chroma Cloud returned on 25 October 2026.

The tests of the vectors need the model files: they run when the files are in ~/.cache/chroma/onnx_models, or when
CHROMA_TEST_HOSTED_MODELS names the models they may download, like qwen-float16,splade (1.7 GB; qwen-float32 is 2.4 GB more);
otherwise they are skipped. The CI downloads qwen-float16 and splade every time."""
import hashlib
import http.server
import json
import os
import threading
import time
import warnings
from pathlib import Path
from typing import Any, Dict, Iterator, List, Set

import numpy as np
import pytest

import chromadb.utils.embedding_functions.chroma_cloud_local as chroma_cloud_local
from chromadb.api.collection_configuration import (
    load_collection_configuration_from_json,
)
from chromadb.api.types import Schema
from chromadb.utils.embedding_functions.chroma_cloud_local import (
    HuggingFaceFile,
    HuggingFaceModel,
    LocalQwen,
    QWEN_MODELS,
    SPLADE_MODEL,
    _LocalModel,
)
from chromadb.utils.embedding_functions.chroma_cloud_qwen_embedding_function import (
    ChromaCloudQwenEmbeddingFunction,
    ChromaCloudQwenEmbeddingModel,
)
from chromadb.utils.embedding_functions.chroma_cloud_splade_embedding_function import (
    ChromaCloudSpladeEmbeddingFunction,
)

DATA = json.loads(
    (Path(__file__).parent / "data" / "chroma_cloud_hosted.json").read_text(
        encoding="utf-8"
    )
)
QWEN_CONFIG = DATA["configuration"]["embedding_function"]["config"]
SPLADE_CONFIG = DATA["schema"]["keys"]["splade"]["sparse_vector"][
    "sparse_vector_index"
]["config"]["embedding_function"]["config"]

# Measured against Chroma Cloud, in float16 and float32: 1 - cosine at most 4.5e-6, a value at most 5.4e-4. SPLADE: the service
# has a few more terms, below 0.016, and the values of the terms of both differ by up to 0.045.
COSINE_TOLERANCE = 1e-5
VALUE_TOLERANCE = 1e-3
SPLADE_CLOUD_ONLY_TOLERANCE = 0.02
SPLADE_VALUE_TOLERANCE = 0.06


def has_files(model: HuggingFaceModel) -> bool:
    return all(
        (model.folder / f.path).is_file()
        and (model.folder / f.path).stat().st_size == f.size
        for f in model.files
    )


def need_files(name: str, model: HuggingFaceModel) -> None:
    allowed = os.getenv("CHROMA_TEST_HOSTED_MODELS", "").split(",")
    if not has_files(model) and name not in allowed:
        pytest.skip(
            f"No files of {model.repository} in {model.folder}: CHROMA_TEST_HOSTED_MODELS={name} downloads them"
        )


# The configuration and the schema of a collection that Chroma Cloud created with the two functions: without an API key they
# build the functions, and give back the configuration as it was.


def test_qwen_configuration_of_chroma_cloud_round_trips_without_a_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("CHROMA_API_KEY", raising=False)
    ef = ChromaCloudQwenEmbeddingFunction.build_from_config(QWEN_CONFIG)
    assert ef.get_config() == QWEN_CONFIG
    ChromaCloudQwenEmbeddingFunction.validate_config(ef.get_config())


def test_splade_configuration_of_chroma_cloud_round_trips_without_a_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("CHROMA_API_KEY", raising=False)
    ef = ChromaCloudSpladeEmbeddingFunction.build_from_config(SPLADE_CONFIG)
    assert ef.get_config() == SPLADE_CONFIG
    ChromaCloudSpladeEmbeddingFunction.validate_config(ef.get_config())


def test_schema_of_chroma_cloud_builds_both_functions_without_a_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("CHROMA_API_KEY", raising=False)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        schema = Schema.deserialize_from_json(DATA["schema"])
    assert not [w for w in caught if "Could not reconstruct" in str(w.message)]
    serialized = schema.serialize_to_json()
    dense = serialized["keys"]["#embedding"]["float_list"]["vector_index"]["config"][
        "embedding_function"
    ]
    sparse = serialized["keys"]["splade"]["sparse_vector"]["sparse_vector_index"][
        "config"
    ]["embedding_function"]
    assert (
        dense
        == DATA["schema"]["keys"]["#embedding"]["float_list"]["vector_index"]["config"][
            "embedding_function"
        ]
    )
    assert (
        sparse
        == DATA["schema"]["keys"]["splade"]["sparse_vector"]["sparse_vector_index"][
            "config"
        ]["embedding_function"]
    )


def test_configuration_of_chroma_cloud_builds_qwen_without_a_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("CHROMA_API_KEY", raising=False)
    configuration = load_collection_configuration_from_json(DATA["configuration"])
    assert isinstance(
        configuration["embedding_function"], ChromaCloudQwenEmbeddingFunction
    )


def test_precision_is_float16_or_float32() -> None:
    with pytest.raises(ValueError, match="float16 or float32"):
        ChromaCloudQwenEmbeddingFunction(
            model=ChromaCloudQwenEmbeddingModel.QWEN3_EMBEDDING_0p6B,
            task="nl_to_code",
            precision="int8",
        )


# The cache of the files, with a mirror of Hugging Face on 127.0.0.1 and a model of two small files.

TOKENIZER = b'{"model": "a tokenizer"}'
WEIGHTS = bytes((i * 7) % 256 for i in range(1 << 16))
REPOSITORY = "owner/model"
REVISION = "0123456789abcdef0123456789abcdef01234567"


def small_model(folder: Path) -> HuggingFaceModel:
    return HuggingFaceModel(
        REPOSITORY,
        REVISION,
        folder,
        "onnx/model.onnx",
        [
            HuggingFaceFile(
                "tokenizer.json", hashlib.sha256(TOKENIZER).hexdigest(), len(TOKENIZER)
            ),
            HuggingFaceFile(
                "onnx/model.onnx", hashlib.sha256(WEIGHTS).hexdigest(), len(WEIGHTS)
            ),
        ],
    )


MODEL_ROUTE = f"/{REPOSITORY}/resolve/{REVISION}/onnx/model.onnx"


class Mirror:
    """The files of the mirror, the paths it was asked for, a delay before each answer, and the paths whose next request it drops
    without an answer, as a connection reset."""

    def __init__(self) -> None:
        self.routes: Dict[str, bytes] = {
            f"/{REPOSITORY}/resolve/{REVISION}/tokenizer.json": TOKENIZER,
            MODEL_ROUTE: WEIGHTS,
        }
        self.requests: List[str] = []
        self.delay = 0.0
        self.drop: Set[str] = set()


@pytest.fixture
def mirror(monkeypatch: pytest.MonkeyPatch) -> Iterator[Mirror]:
    state = Mirror()

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            state.requests.append(self.path)
            if self.path in state.drop:
                state.drop.remove(self.path)
                self.close_connection = True
                return
            time.sleep(state.delay)
            body = state.routes.get(self.path)
            self.send_response(200 if body is not None else 404)
            self.send_header("Content-Length", str(len(body or b"")))
            self.end_headers()
            self.wfile.write(body or b"")

        def log_message(self, *args: Any) -> None:
            pass

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    monkeypatch.setattr(
        chroma_cloud_local,
        "HUGGING_FACE",
        f"http://127.0.0.1:{server.server_address[1]}",
    )
    yield state
    server.shutdown()


def test_files_are_downloaded_once_then_used_from_the_cache(
    tmp_path: Path, mirror: Mirror
) -> None:
    local = _LocalModel(small_model(tmp_path))
    local.ensure_files()
    local.ensure_files()
    assert (tmp_path / "onnx" / "model.onnx").read_bytes() == WEIGHTS
    assert (tmp_path / "tokenizer.json").read_bytes() == TOKENIZER
    assert len(mirror.requests) == 2
    assert not list(tmp_path.rglob("*.tmp"))


def test_file_of_another_size_is_downloaded_again(
    tmp_path: Path, mirror: Mirror
) -> None:
    (tmp_path / "onnx").mkdir()
    (tmp_path / "tokenizer.json").write_bytes(TOKENIZER)
    (tmp_path / "onnx" / "model.onnx").write_bytes(WEIGHTS[:1000])
    _LocalModel(small_model(tmp_path)).ensure_files()
    assert (tmp_path / "onnx" / "model.onnx").read_bytes() == WEIGHTS
    assert mirror.requests == [MODEL_ROUTE]


def test_file_with_another_hash_is_not_kept(tmp_path: Path, mirror: Mirror) -> None:
    mirror.routes[MODEL_ROUTE] = WEIGHTS[:-1] + bytes([WEIGHTS[-1] ^ 1])
    with pytest.raises(ValueError, match="does not match expected SHA256"):
        _LocalModel(small_model(tmp_path)).ensure_files()
    assert not (tmp_path / "onnx" / "model.onnx").exists()
    assert not list(tmp_path.rglob("*.tmp"))


def test_threads_at_the_first_text_download_each_file_once(
    tmp_path: Path, mirror: Mirror
) -> None:
    mirror.delay = 0.3
    local = _LocalModel(small_model(tmp_path))
    errors: List[BaseException] = []

    def first_text() -> None:
        try:
            local.ensure_files()
        except BaseException as e:
            errors.append(e)

    threads = [threading.Thread(target=first_text) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert not errors
    assert len(mirror.requests) == 2
    assert (tmp_path / "onnx" / "model.onnx").read_bytes() == WEIGHTS


def test_a_dropped_connection_is_tried_again(tmp_path: Path, mirror: Mirror) -> None:
    mirror.drop.add(MODEL_ROUTE)
    _LocalModel(small_model(tmp_path)).ensure_files()
    assert (tmp_path / "onnx" / "model.onnx").read_bytes() == WEIGHTS
    assert mirror.requests.count(MODEL_ROUTE) == 2


def test_a_stale_temporary_file_is_removed(tmp_path: Path, mirror: Mirror) -> None:
    (tmp_path / "onnx").mkdir()
    stale = tmp_path / "onnx" / "model.onnx.4242.tmp"
    stale.write_bytes(b"a download that stopped")
    old = time.time() - 2 * _LocalModel.STALE_SECONDS
    os.utime(stale, (old, old))
    live = tmp_path / "onnx" / "model.onnx.4343.tmp"
    live.write_bytes(b"a download of another process")
    _LocalModel(small_model(tmp_path)).ensure_files()
    assert not stale.exists()
    assert live.exists()


# The vectors, with the files of the models.


def cosine(a: np.ndarray, b: np.ndarray) -> float:
    return float(np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b)))


@pytest.mark.parametrize("precision", ["float16", "float32"])
def test_qwen_vectors_are_those_of_chroma_cloud(precision: str) -> None:
    need_files(f"qwen-{precision}", QWEN_MODELS[precision])
    ef = ChromaCloudQwenEmbeddingFunction(
        model=ChromaCloudQwenEmbeddingModel.QWEN3_EMBEDDING_0p6B,
        task=QWEN_CONFIG["task"],
        precision=precision,
    )
    computed = [
        ("qwen-documents", ef(DATA["qwen-documents"]["texts"])),
        ("qwen-queries", ef.embed_query(DATA["qwen-queries"]["texts"])),
    ]
    for name, embeddings in computed:
        assert len(embeddings) == len(DATA[name]["texts"]) == len(DATA[name]["vectors"])
        for got, want in zip(embeddings, DATA[name]["vectors"]):
            expected = np.asarray(want, dtype=np.float64)
            assert (
                1 - cosine(got.astype(np.float64), expected) <= COSINE_TOLERANCE
            ), name
            assert float(np.abs(got - expected).max()) <= VALUE_TOLERANCE, name


def qwen_from_config() -> ChromaCloudQwenEmbeddingFunction:
    ef = ChromaCloudQwenEmbeddingFunction.build_from_config(QWEN_CONFIG)
    assert isinstance(ef, ChromaCloudQwenEmbeddingFunction)
    return ef


def test_qwen_refuses_a_text_over_8192_tokens() -> None:
    need_files("qwen-float16", QWEN_MODELS["float16"])
    ef = qwen_from_config()
    tokenizer, _ = ef._local().load()
    long_text = "def add(a, b):\n    return a + b\n" * 2000
    assert len(tokenizer.encode(long_text).ids) > LocalQwen.MAX_TOKENS
    with pytest.raises(ValueError, match="at most 8192 tokens"):
        ef(["A sour lemon", long_text])
    assert ef.max_tokens() == 8192


def test_qwen_keeps_one_model_for_each_process_and_precision() -> None:
    first = qwen_from_config()
    second = qwen_from_config()
    third = ChromaCloudQwenEmbeddingFunction(
        model=ChromaCloudQwenEmbeddingModel.QWEN3_EMBEDDING_0p6B,
        task=QWEN_CONFIG["task"],
        precision="float32",
    )
    assert first._local() is second._local()
    assert first._local() is not third._local()


def splade_dict(vector: Any) -> Dict[int, float]:
    return dict(zip(vector.indices, vector.values))


def test_splade_vectors_are_those_of_chroma_cloud() -> None:
    need_files("splade", SPLADE_MODEL)
    ef = ChromaCloudSpladeEmbeddingFunction.build_from_config(SPLADE_CONFIG)
    for name in ["splade-documents", "splade-queries"]:
        case = DATA[name]
        vectors = ef(case["texts"])
        assert len(vectors) == len(case["texts"]) == len(case["vectors"])
        for got, want in zip(vectors, case["vectors"]):
            got_d, want_d = splade_dict(got), dict(zip(want["indices"], want["values"]))
            assert set(got_d) <= set(
                want_d
            ), "every term here is also in the vector of Chroma Cloud"
            assert all(
                v <= SPLADE_CLOUD_ONLY_TOLERANCE
                for i, v in want_d.items()
                if i not in got_d
            )
            assert all(
                abs(got_d[i] - want_d[i]) <= SPLADE_VALUE_TOLERANCE for i in got_d
            )


def test_splade_gives_the_tokens_when_asked() -> None:
    need_files("splade", SPLADE_MODEL)
    config = dict(SPLADE_CONFIG, include_tokens=True)
    vector = ChromaCloudSpladeEmbeddingFunction.build_from_config(config)(
        ["A sour lemon"]
    )[0]
    assert vector.labels is not None and "lemon" in vector.labels
    assert len(vector.labels) == len(vector.indices)
    assert (
        ChromaCloudSpladeEmbeddingFunction.build_from_config(SPLADE_CONFIG)(
            ["A sour lemon"]
        )[0].labels
        is None
    )
