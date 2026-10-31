# Changed from the original Chroma file; see the git history for the changes and their authors.
from chromadb.api.types import (
    SparseEmbeddingFunction,
    SparseVectors,
    Documents,
)
from typing import Dict, Any
from enum import Enum
from chromadb.utils.embedding_functions.chroma_cloud_local import (
    LocalSplade,
    SPLADE_MODEL,
)
from chromadb.utils.embedding_functions.schemas import validate_config_schema
import os


class ChromaCloudSpladeEmbeddingModel(Enum):
    SPLADE_PP_EN_V1 = "prithivida/Splade_PP_en_v1"


class ChromaCloudSpladeEmbeddingFunction(SparseEmbeddingFunction[Documents]):
    """chroma-cloud-splade, computed on this machine: SPLADE (prithivida/Splade_PP_en_v1) in ONNX, as the service of Chroma Cloud
    computed it, truncated to 512 tokens. The first call downloads the model from Hugging Face, 533 MB, into
    ~/.cache/chroma/onnx_models. No API key: api_key_env_var stays in the configuration, as Chroma Cloud stored it.
    """

    # One model for each process, kept as DefaultEmbeddingFunction keeps its model. By process id, so that a child of fork() does
    # not use the session of its parent.
    models: Dict[int, LocalSplade] = {}

    def __init__(
        self,
        api_key_env_var: str = "CHROMA_API_KEY",
        model: ChromaCloudSpladeEmbeddingModel = ChromaCloudSpladeEmbeddingModel.SPLADE_PP_EN_V1,
        include_tokens: bool = False,
    ):
        """
        Initialize the ChromaCloudSpladeEmbeddingFunction.

        Args:
            api_key_env_var (str, optional): Kept in the configuration, as Chroma Cloud stored it; no key is read.
                Defaults to "CHROMA_API_KEY".
            include_tokens (bool, optional): Whether each sparse vector has the token of each index in its labels.
        """
        self.api_key_env_var = api_key_env_var
        self.model = model
        self.include_tokens = bool(include_tokens)

    def close(self) -> None:
        """
        Kept for the code that called it: there is no HTTP client session to close.
        """

    def __call__(self, input: Documents) -> SparseVectors:
        """
        Generate embeddings for the given documents.

        Args:
            input (Documents): The documents to generate embeddings for.
        """
        if not input:
            return []
        local = self.models.get(os.getpid())
        if local is None:
            # Two threads here at once: setdefault keeps the first one for both.
            local = self.models.setdefault(os.getpid(), LocalSplade(SPLADE_MODEL))
        return local.embed(list(input), self.include_tokens)

    @staticmethod
    def name() -> str:
        return "chroma-cloud-splade"

    @staticmethod
    def build_from_config(
        config: Dict[str, Any]
    ) -> "SparseEmbeddingFunction[Documents]":
        api_key_env_var = config.get("api_key_env_var")
        model = config.get("model")
        if model is None:
            raise ValueError("model must be provided in config")
        if not api_key_env_var:
            raise ValueError("api_key_env_var must be provided in config")
        return ChromaCloudSpladeEmbeddingFunction(
            api_key_env_var=api_key_env_var,
            model=ChromaCloudSpladeEmbeddingModel(model),
            include_tokens=config.get("include_tokens", False),
        )

    def get_config(self) -> Dict[str, Any]:
        return {
            "api_key_env_var": self.api_key_env_var,
            "model": self.model.value,
            "include_tokens": self.include_tokens,
        }

    def validate_config_update(
        self, old_config: Dict[str, Any], new_config: Dict[str, Any]
    ) -> None:
        immutable_keys = {"include_tokens", "model"}
        for key in immutable_keys:
            if key in new_config and new_config[key] != old_config.get(key):
                raise ValueError(
                    f"Updating '{key}' is not supported for chroma-cloud-splade"
                )

    @staticmethod
    def validate_config(config: Dict[str, Any]) -> None:
        validate_config_schema(config, "chroma-cloud-splade")
