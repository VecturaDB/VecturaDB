# Changed from the original Chroma file; see the git history for the changes and their authors.
from chromadb.api.types import Embeddings, Documents, EmbeddingFunction, Space
from typing import List, Dict, Any, Optional, Tuple, cast
import os
from chromadb.utils.embedding_functions.chroma_cloud_local import LocalQwen, QWEN_MODELS
from chromadb.utils.embedding_functions.schemas import validate_config_schema
from enum import Enum


class ChromaCloudQwenEmbeddingModel(Enum):
    QWEN3_EMBEDDING_0p6B = "Qwen/Qwen3-Embedding-0.6B"


class ChromaCloudQwenEmbeddingTarget(Enum):
    DOCUMENTS = "documents"
    QUERY = "query"


ChromaCloudQwenEmbeddingInstructions = Dict[
    str, Dict[ChromaCloudQwenEmbeddingTarget, str]
]

CHROMA_CLOUD_QWEN_DEFAULT_INSTRUCTIONS: ChromaCloudQwenEmbeddingInstructions = {
    "nl_to_code": {
        ChromaCloudQwenEmbeddingTarget.DOCUMENTS: "",
        # Taken from https://github.com/QwenLM/Qwen3-Embedding/blob/main/evaluation/task_prompts.json
        ChromaCloudQwenEmbeddingTarget.QUERY: "Given a question about coding, retrieval code or passage that can solve user's question",
    }
}


class ChromaCloudQwenEmbeddingFunction(EmbeddingFunction[Documents]):
    """chroma-cloud-qwen, computed on this machine: Qwen3-Embedding-0.6B in ONNX, with the instructions of the task, as the service
    of Chroma Cloud computed it. The first call downloads the model from Hugging Face, 1.2 GB in float16 or 2.4 GB in float32, into
    ~/.cache/chroma/onnx_models. No API key: api_key_env_var stays in the configuration, as Chroma Cloud stored it.
    """

    # One model for each process and precision, kept as DefaultEmbeddingFunction keeps its model: loading it takes seconds. By
    # process id, so that a child of fork() does not use the session of its parent.
    models: Dict[Tuple[int, str], LocalQwen] = {}

    def __init__(
        self,
        model: ChromaCloudQwenEmbeddingModel,
        task: Optional[str],
        instructions: ChromaCloudQwenEmbeddingInstructions = CHROMA_CLOUD_QWEN_DEFAULT_INSTRUCTIONS,
        api_key_env_var: str = "CHROMA_API_KEY",
        precision: str = "float16",
    ):
        """
        Initialize the ChromaCloudQwenEmbeddingFunction.

        Args:
            model (ChromaCloudQwenEmbeddingModel): The specific Qwen model to use for embeddings.
            task (str, optional): The task for which embeddings are being generated. If None or empty,
                empty instructions will be used for both documents and queries.
            instructions (ChromaCloudQwenEmbeddingInstructions, optional): A dictionary containing
                custom instructions to use for the specified Qwen model. Defaults to CHROMA_CLOUD_QWEN_DEFAULT_INSTRUCTIONS.
            api_key_env_var (str, optional): Kept in the configuration, as Chroma Cloud stored it; no key is read.
                Defaults to "CHROMA_API_KEY".
            precision (str, optional): "float16", the default, or "float32", which downloads twice the bytes. It is not part of
                the configuration: a collection opened again computes in float16.
        """
        if precision not in QWEN_MODELS:
            raise ValueError(f"precision is float16 or float32, not {precision}")
        self.api_key_env_var = api_key_env_var
        self.model = model
        self.task = task
        self.instructions = instructions
        self.precision = precision

    def _local(self) -> LocalQwen:
        key = (os.getpid(), self.precision)
        local = self.models.get(key)
        if local is None:
            # Two threads here at once: setdefault keeps the first one for both.
            local = self.models.setdefault(key, LocalQwen(QWEN_MODELS[self.precision]))
        return local

    def _instruction(self, target: ChromaCloudQwenEmbeddingTarget) -> str:
        if self.task and self.task in self.instructions:
            return self.instructions[self.task][target]
        return ""

    def __call__(self, input: Documents) -> Embeddings:
        """
        Generate embeddings for the given documents.

        Args:
            input: Documents to generate embeddings for.

        Returns:
            Embeddings for the documents.
        """
        if not input:
            return []
        return cast(
            Embeddings,
            self._local().embed(
                list(input), self._instruction(ChromaCloudQwenEmbeddingTarget.DOCUMENTS)
            ),
        )

    def embed_query(self, input: Documents) -> Embeddings:
        """
        Get the embeddings for a query input.
        """
        if not input:
            return []
        return cast(
            Embeddings,
            self._local().embed(
                list(input), self._instruction(ChromaCloudQwenEmbeddingTarget.QUERY)
            ),
        )

    def max_tokens(self) -> int:
        return LocalQwen.MAX_TOKENS

    @staticmethod
    def name() -> str:
        return "chroma-cloud-qwen"

    def default_space(self) -> Space:
        return "cosine"

    def supported_spaces(self) -> List[Space]:
        return ["cosine", "l2", "ip"]

    @staticmethod
    def build_from_config(config: Dict[str, Any]) -> "EmbeddingFunction[Documents]":
        model = config.get("model")
        task = config.get("task")
        instructions = config.get("instructions")
        api_key_env_var = config.get("api_key_env_var")

        if model is None or task is None:
            assert False, "Config is missing a required field"

        # Deserialize instructions dict from string keys to enum keys
        deserialized_instructions = CHROMA_CLOUD_QWEN_DEFAULT_INSTRUCTIONS
        if instructions is not None:
            deserialized_instructions = {}
            for task_key, targets in instructions.items():
                deserialized_instructions[task_key] = {}
                for target_key, instruction in targets.items():
                    # Convert string key to enum
                    target_enum = ChromaCloudQwenEmbeddingTarget(target_key)
                    deserialized_instructions[task_key][target_enum] = instruction
                    deserialized_instructions[task_key][target_enum] = instruction

        return ChromaCloudQwenEmbeddingFunction(
            model=ChromaCloudQwenEmbeddingModel(model),
            task=task,
            instructions=deserialized_instructions,
            api_key_env_var=api_key_env_var or "CHROMA_API_KEY",
        )

    def get_config(self) -> Dict[str, Any]:
        # Serialize instructions dict with enum keys to string keys for JSON compatibility
        serialized_instructions = {
            task: {target.value: instruction for target, instruction in targets.items()}
            for task, targets in self.instructions.items()
        }
        return {
            "api_key_env_var": self.api_key_env_var,
            "model": self.model.value,
            "task": self.task,
            "instructions": serialized_instructions,
        }

    def validate_config_update(
        self, old_config: Dict[str, Any], new_config: Dict[str, Any]
    ) -> None:
        if "model" in new_config:
            raise ValueError(
                "The model cannot be changed after the embedding function has been initialized."
            )
        elif "task" in new_config:
            raise ValueError(
                "The task cannot be changed after the embedding function has been initialized."
            )
        elif "instructions" in new_config:
            raise ValueError(
                "The instructions cannot be changed after the embedding function has been initialized."
            )

    @staticmethod
    def validate_config(config: Dict[str, Any]) -> None:
        """
        Validate the configuration using the JSON schema.

        Args:
            config: Configuration to validate

        Raises:
            ValidationError: If the configuration does not match the schema
        """
        validate_config_schema(config, "chroma-cloud-qwen")
