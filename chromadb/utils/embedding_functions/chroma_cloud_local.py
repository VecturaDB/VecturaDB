"""The models of chroma-cloud-qwen and chroma-cloud-splade, computed on this machine with onnxruntime as the default embedding
function computes all-MiniLM-L6-v2, instead of on the servers of Chroma Cloud.

The files come from Hugging Face at fixed revisions, into ~/.cache/chroma/onnx_models, checked against their SHA-256 hashes when
they are downloaded. They are used when they are all there with their sizes. Each text is computed alone, as the service did."""
import importlib
import os
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import httpx
import numpy as np
import numpy.typing as npt
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_random

from chromadb.api.types import SparseVector
from chromadb.utils.embedding_functions.onnx_mini_lm_l6_v2 import _verify_sha256
from chromadb.utils.sparse_embedding_utils import normalize_sparse_vector

ONNX_MODELS = Path.home() / ".cache" / "chroma" / "onnx_models"
HUGGING_FACE = "https://huggingface.co"


class HuggingFaceFile:
    """A file of a model repository of Hugging Face: its path in the repository, its SHA-256 hash and its size."""

    def __init__(self, path: str, sha256: str, size: int) -> None:
        self.path = path
        self.sha256 = sha256
        self.size = size


class HuggingFaceModel:
    """The files of a model at a fixed revision of its repository, in a folder of the cache with the layout of the repository."""

    def __init__(
        self,
        repository: str,
        revision: str,
        folder: Path,
        onnx: str,
        files: List[HuggingFaceFile],
    ) -> None:
        self.repository = repository
        self.revision = revision
        self.folder = folder
        self.onnx = onnx
        self.files = files


_QWEN_REPOSITORY = "onnx-community/Qwen3-Embedding-0.6B-ONNX"
_QWEN_REVISION = "c25a394dd583836952667c12f008335071b3f43d"
# The tokenizer of the ONNX export is the one of Qwen/Qwen3-Embedding-0.6B, byte for byte.
_QWEN_TOKENIZER = HuggingFaceFile(
    "tokenizer.json",
    "def76fb086971c7867b829c23a26261e38d9d74e02139253b38aeb9df8b4b50a",
    11_423_705,
)

QWEN_MODELS = {
    "float16": HuggingFaceModel(
        _QWEN_REPOSITORY,
        _QWEN_REVISION,
        ONNX_MODELS / "Qwen3-Embedding-0.6B" / "fp16",
        "onnx/model_fp16.onnx",
        [
            _QWEN_TOKENIZER,
            HuggingFaceFile(
                "onnx/model_fp16.onnx",
                "f376cf065d912675dd559270e19361367ebc28c938b9811dd61d5834c78d0834",
                583_522,
            ),
            HuggingFaceFile(
                "onnx/model_fp16.onnx_data",
                "06fbfded30166e3a33fe5252d7002ed1c370a4ec1bf45aba778cdf6cf7d31439",
                1_199_927_296,
            ),
        ],
    ),
    "float32": HuggingFaceModel(
        _QWEN_REPOSITORY,
        _QWEN_REVISION,
        ONNX_MODELS / "Qwen3-Embedding-0.6B" / "fp32",
        "onnx/model.onnx",
        [
            _QWEN_TOKENIZER,
            HuggingFaceFile(
                "onnx/model.onnx",
                "bf27b2f3f9ef9c32ca337d75b361fa99439deaeaefe82e4701b2dbd8439197cc",
                307_161_415,
            ),
            HuggingFaceFile(
                "onnx/model.onnx_data",
                "f0a61604465929a27e68aa6217c8c89ec6186572f0209fdb7711adda48a9b9a9",
                2_093_436_928,
            ),
        ],
    ),
}

# The hash of the tokenizer, a plain file of the repository, was computed on the file of that revision.
SPLADE_MODEL = HuggingFaceModel(
    "prithivida/Splade_PP_en_v1",
    "762be6a7206e2f299182705972a65e5c46e62be2",
    ONNX_MODELS / "Splade_PP_en_v1",
    "onnx/model.onnx",
    [
        HuggingFaceFile(
            "tokenizer.json",
            "2fc687b11de0bc1b3d8348f92e3b49ef1089a621506c7661fbf3248fcd54947e",
            711_649,
        ),
        HuggingFaceFile(
            "onnx/model.onnx",
            "0934583a27a031a66b2e847cbc260fbbef29689e969f500436460ef5146a43f2",
            532_126_852,
        ),
    ],
)


def _retryable(e: BaseException) -> bool:
    """A file is downloaded again after a network error, an error of the server, or a file with another hash."""
    if isinstance(e, httpx.TransportError):
        return True
    if isinstance(e, httpx.HTTPStatusError):
        return e.response.status_code >= 500
    return "does not match expected SHA256" in str(e)


class _LocalModel:
    """The files, the tokenizer and the ONNX session of a model, loaded at the first text, as ONNXMiniLM_L6_V2 loads them. With
    several threads at the first text, one downloads and loads, and the others wait for it.
    """

    # A temporary file that has not been written for this long belongs to a download that stopped, like a process killed.
    STALE_SECONDS = 3600

    def __init__(self, model: HuggingFaceModel) -> None:
        self._model = model
        self._lock = threading.RLock()
        self._loaded: Optional[Tuple[Any, Any]] = None
        try:
            self.ort = importlib.import_module("onnxruntime")
        except ImportError:
            raise ValueError(
                "The onnxruntime python package is not installed. Please install it with `pip install onnxruntime`"
            )
        try:
            self.Tokenizer = importlib.import_module("tokenizers").Tokenizer
        except ImportError:
            raise ValueError(
                "The tokenizers python package is not installed. Please install it with `pip install tokenizers`"
            )
        try:
            self.tqdm = importlib.import_module("tqdm").tqdm
        except ImportError:
            raise ValueError(
                "The tqdm python package is not installed. Please install it with `pip install tqdm`"
            )

    @retry(
        reraise=True,
        stop=stop_after_attempt(3),
        wait=wait_random(min=1, max=3),
        retry=retry_if_exception(_retryable),
    )
    def _download(
        self, url: str, fname: str, sha256: str, chunk_size: int = 1024 * 1024
    ) -> None:
        # Into a file of this process, renamed once its hash is checked: a file of the cache is whole.
        temporary = f"{fname}.{os.getpid()}.tmp"
        try:
            with httpx.stream("GET", url, follow_redirects=True) as resp:
                resp.raise_for_status()
                total = int(resp.headers.get("content-length", 0))
                with open(temporary, "wb") as file, self.tqdm(
                    desc=fname,
                    total=total,
                    unit="iB",
                    unit_scale=True,
                    unit_divisor=1024,
                ) as bar:
                    for data in resp.iter_bytes(chunk_size=chunk_size):
                        bar.update(file.write(data))
            if not _verify_sha256(temporary, sha256):
                raise ValueError(
                    f"Downloaded file {fname} does not match expected SHA256 hash. Corrupted download or malicious file."
                )
            os.replace(temporary, fname)
        finally:
            if os.path.exists(temporary):
                os.remove(temporary)

    def ensure_files(self) -> None:
        """Downloads each file that is missing, or that has another size, like a copy cut short."""
        with self._lock:
            for file in self._model.files:
                path = self._model.folder / file.path
                if path.is_file() and path.stat().st_size == file.size:
                    continue
                path.parent.mkdir(parents=True, exist_ok=True)
                for stale in path.parent.glob(f"{path.name}.*.tmp"):
                    if time.time() - stale.stat().st_mtime > self.STALE_SECONDS:
                        stale.unlink(missing_ok=True)
                self._download(
                    f"{HUGGING_FACE}/{self._model.repository}/resolve/{self._model.revision}/{file.path}",
                    str(path),
                    file.sha256,
                )

    def _new_tokenizer(self) -> Any:
        raise NotImplementedError

    def _new_session(self) -> Any:
        providers = [
            p
            for p in self.ort.get_available_providers()
            if p != "CoreMLExecutionProvider"
        ]
        so = self.ort.SessionOptions()
        so.log_severity_level = 3
        so.graph_optimization_level = self.ort.GraphOptimizationLevel.ORT_ENABLE_ALL
        # No CPU memory arena, as for ONNXMiniLM_L6_V2: a model kept for the process would hold the arena of its longest text.
        so.enable_cpu_mem_arena = False
        return self.ort.InferenceSession(
            str(self._model.folder / self._model.onnx),
            providers=providers,
            sess_options=so,
        )

    def load(self) -> Tuple[Any, Any]:
        """The tokenizer and the session, with the files checked and downloaded once for the model."""
        with self._lock:
            if self._loaded is None:
                self.ensure_files()
                self._loaded = (self._new_tokenizer(), self._new_session())
            return self._loaded


class LocalQwen(_LocalModel):
    """Qwen3-Embedding-0.6B as the service computed chroma-cloud-qwen: each text alone, as "Instruct: {instruction}\\nQuery:{text}"
    when there is an instruction, the last hidden state of its last token, normalized to length 1.
    """

    # onnxruntime refuses longer texts: the attention mask of the export would need more than 4 GiB.
    MAX_TOKENS = 8192

    def _new_tokenizer(self) -> Any:
        tokenizer = self.Tokenizer.from_file(str(self._model.folder / "tokenizer.json"))
        tokenizer.no_truncation()
        tokenizer.no_padding()
        return tokenizer

    def embed(
        self, texts: List[str], instruction: str
    ) -> List[npt.NDArray[np.float32]]:
        tokenizer, session = self.load()
        encoded = [
            tokenizer.encode(
                f"Instruct: {instruction}\nQuery:{text}" if instruction else text
            ).ids
            for text in texts
        ]
        for index, ids in enumerate(encoded):
            if len(ids) > self.MAX_TOKENS:
                raise ValueError(
                    f"The text at index {index} has {len(ids)} tokens: chroma-cloud-qwen computes at most {self.MAX_TOKENS} tokens on "
                    "this machine. Split the text into shorter ones."
                )
        past = {
            node.name: node
            for node in session.get_inputs()
            if node.name.startswith("past_key_values.")
        }
        embeddings = []
        for ids in encoded:
            n = len(ids)
            feed: Dict[str, npt.NDArray[Any]] = {
                "input_ids": np.asarray([ids], dtype=np.int64),
                "attention_mask": np.ones((1, n), dtype=np.int64),
                "position_ids": np.arange(n, dtype=np.int64)[None, :],
            }
            for name, node in past.items():
                dtype: Any = np.float16 if "float16" in node.type else np.float32
                feed[name] = np.zeros([1, node.shape[1], 0, node.shape[3]], dtype=dtype)
            last = np.asarray(
                session.run(["last_hidden_state"], feed)[0][0, -1],
                dtype=np.float64,
            )
            embeddings.append((last / np.linalg.norm(last)).astype(np.float32))
        return embeddings


class LocalSplade(_LocalModel):
    """SPLADE as the service computed chroma-cloud-splade: each text alone, truncated to 512 tokens with [CLS] and [SEP], for each
    token of the vocabulary the largest log(1 + ReLU(logit)) over the tokens of the text, the weights above zero.
    """

    MAX_TOKENS = 512

    def _new_tokenizer(self) -> Any:
        tokenizer = self.Tokenizer.from_file(str(self._model.folder / "tokenizer.json"))
        tokenizer.no_padding()
        tokenizer.enable_truncation(self.MAX_TOKENS)
        return tokenizer

    def embed(self, texts: List[str], include_tokens: bool) -> List[SparseVector]:
        tokenizer, session = self.load()
        vectors = []
        for text in texts:
            ids = tokenizer.encode(text).ids
            n = len(ids)
            logits = session.run(
                None,
                {
                    "input_ids": np.asarray([ids], dtype=np.int64),
                    "input_mask": np.ones((1, n), dtype=np.int64),
                    "segment_ids": np.zeros((1, n), dtype=np.int64),
                },
            )[0][0]
            weights = (
                np.log1p(np.maximum(logits.astype(np.float64), 0))
                .max(axis=0)
                .astype(np.float32)
            )
            indices = np.nonzero(weights > 0)[0]
            labels: Optional[List[str]] = (
                [tokenizer.id_to_token(int(i)) for i in indices]
                if include_tokens
                else None
            )
            vectors.append(
                normalize_sparse_vector(
                    indices=indices.tolist(),
                    values=weights[indices].tolist(),
                    labels=labels,
                )
            )
        return vectors
