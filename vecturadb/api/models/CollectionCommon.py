# Copyright VecturaDB contributors. Licensed under the Apache License, Version 2.0.
from chromadb.api.models.CollectionCommon import ClientT as ClientT  # noqa: F401
from chromadb.api.models.CollectionCommon import (  # noqa: F401
    CollectionCommon as CollectionCommon,
)
from chromadb.api.models.CollectionCommon import T as T  # noqa: F401
from chromadb.api.models.CollectionCommon import logger as logger  # noqa: F401
from chromadb.api.models.CollectionCommon import (  # noqa: F401
    validation_context as validation_context,
)
from chromadb.api.models.CollectionCommon import (  # type: ignore[attr-defined]  # noqa: F401
    AddRequest,
)
from chromadb.api.models.CollectionCommon import (  # type: ignore[attr-defined]  # noqa: F401
    Any,
)
from chromadb.api.models.CollectionCommon import (  # type: ignore[attr-defined]  # noqa: F401
    AsyncServerAPI,
)
from chromadb.api.models.CollectionCommon import (  # type: ignore[attr-defined]  # noqa: F401
    BaseRecordSet,
)
from chromadb.api.models.CollectionCommon import (  # type: ignore[attr-defined]  # noqa: F401
    Callable,
)
from chromadb.api.models.CollectionCommon import (  # type: ignore[attr-defined]  # noqa: F401
    CollectionConfiguration,
)
from chromadb.api.models.CollectionCommon import (  # type: ignore[attr-defined]  # noqa: F401
    CollectionMetadata,
)
from chromadb.api.models.CollectionCommon import (  # type: ignore[attr-defined]  # noqa: F401
    CollectionModel,
)
from chromadb.api.models.CollectionCommon import (  # type: ignore[attr-defined]  # noqa: F401
    DOCUMENT_KEY,
)
from chromadb.api.models.CollectionCommon import (  # type: ignore[attr-defined]  # noqa: F401
    DataLoader,
)
from chromadb.api.models.CollectionCommon import (  # type: ignore[attr-defined]  # noqa: F401
    DefaultEmbeddingFunction,
)
from chromadb.api.models.CollectionCommon import (  # type: ignore[attr-defined]  # noqa: F401
    DeleteRequest,
)
from chromadb.api.models.CollectionCommon import (  # type: ignore[attr-defined]  # noqa: F401
    Dict,
)
from chromadb.api.models.CollectionCommon import (  # type: ignore[attr-defined]  # noqa: F401
    Document,
)
from chromadb.api.models.CollectionCommon import (  # type: ignore[attr-defined]  # noqa: F401
    EMBEDDING_KEY,
)
from chromadb.api.models.CollectionCommon import (  # type: ignore[attr-defined]  # noqa: F401
    Embeddable,
)
from chromadb.api.models.CollectionCommon import (  # type: ignore[attr-defined]  # noqa: F401
    Embedding,
)
from chromadb.api.models.CollectionCommon import (  # type: ignore[attr-defined]  # noqa: F401
    EmbeddingFunction,
)
from chromadb.api.models.CollectionCommon import (  # type: ignore[attr-defined]  # noqa: F401
    Embeddings,
)
from chromadb.api.models.CollectionCommon import (  # type: ignore[attr-defined]  # noqa: F401
    FilterSet,
)
from chromadb.api.models.CollectionCommon import (  # type: ignore[attr-defined]  # noqa: F401
    Generic,
)
from chromadb.api.models.CollectionCommon import (  # type: ignore[attr-defined]  # noqa: F401
    GetRequest,
)
from chromadb.api.models.CollectionCommon import (  # type: ignore[attr-defined]  # noqa: F401
    GetResult,
)
from chromadb.api.models.CollectionCommon import (  # type: ignore[attr-defined]  # noqa: F401
    ID,
)
from chromadb.api.models.CollectionCommon import (  # type: ignore[attr-defined]  # noqa: F401
    IDs,
)
from chromadb.api.models.CollectionCommon import (  # type: ignore[attr-defined]  # noqa: F401
    Image,
)
from chromadb.api.models.CollectionCommon import (  # type: ignore[attr-defined]  # noqa: F401
    Include,
)
from chromadb.api.models.CollectionCommon import (  # type: ignore[attr-defined]  # noqa: F401
    List,
)
from chromadb.api.models.CollectionCommon import (  # type: ignore[attr-defined]  # noqa: F401
    Loadable,
)
from chromadb.api.models.CollectionCommon import (  # type: ignore[attr-defined]  # noqa: F401
    Metadata,
)
from chromadb.api.models.CollectionCommon import (  # type: ignore[attr-defined]  # noqa: F401
    OneOrMany,
)
from chromadb.api.models.CollectionCommon import (  # type: ignore[attr-defined]  # noqa: F401
    Optional,
)
from chromadb.api.models.CollectionCommon import (  # type: ignore[attr-defined]  # noqa: F401
    PyEmbedding,
)
from chromadb.api.models.CollectionCommon import (  # type: ignore[attr-defined]  # noqa: F401
    QueryRequest,
)
from chromadb.api.models.CollectionCommon import (  # type: ignore[attr-defined]  # noqa: F401
    QueryResult,
)
from chromadb.api.models.CollectionCommon import (  # type: ignore[attr-defined]  # noqa: F401
    Schema,
)
from chromadb.api.models.CollectionCommon import (  # type: ignore[attr-defined]  # noqa: F401
    ServerAPI,
)
from chromadb.api.models.CollectionCommon import (  # type: ignore[attr-defined]  # noqa: F401
    Set,
)
from chromadb.api.models.CollectionCommon import (  # type: ignore[attr-defined]  # noqa: F401
    SparseEmbeddingFunction,
)
from chromadb.api.models.CollectionCommon import (  # type: ignore[attr-defined]  # noqa: F401
    SparseVectorIndexConfig,
)
from chromadb.api.models.CollectionCommon import (  # type: ignore[attr-defined]  # noqa: F401
    TYPE_CHECKING,
)
from chromadb.api.models.CollectionCommon import (  # type: ignore[attr-defined]  # noqa: F401
    TypeVar,
)
from chromadb.api.models.CollectionCommon import (  # type: ignore[attr-defined]  # noqa: F401
    URI,
)
from chromadb.api.models.CollectionCommon import (  # type: ignore[attr-defined]  # noqa: F401
    URIs,
)
from chromadb.api.models.CollectionCommon import (  # type: ignore[attr-defined]  # noqa: F401
    UUID,
)
from chromadb.api.models.CollectionCommon import (  # type: ignore[attr-defined]  # noqa: F401
    Union,
)
from chromadb.api.models.CollectionCommon import (  # type: ignore[attr-defined]  # noqa: F401
    UpdateCollectionConfiguration,
)
from chromadb.api.models.CollectionCommon import (  # type: ignore[attr-defined]  # noqa: F401
    UpdateRequest,
)
from chromadb.api.models.CollectionCommon import (  # type: ignore[attr-defined]  # noqa: F401
    UpsertRequest,
)
from chromadb.api.models.CollectionCommon import (  # type: ignore[attr-defined]  # noqa: F401
    Where,
)
from chromadb.api.models.CollectionCommon import (  # type: ignore[attr-defined]  # noqa: F401
    WhereDocument,
)
from chromadb.api.models.CollectionCommon import (  # type: ignore[attr-defined]  # noqa: F401
    cast,
)
from chromadb.api.models.CollectionCommon import (  # type: ignore[attr-defined]  # noqa: F401
    get_default_embeddable_record_set_fields,
)
from chromadb.api.models.CollectionCommon import (  # type: ignore[attr-defined]  # noqa: F401
    load_collection_configuration_from_json,
)
from chromadb.api.models.CollectionCommon import (  # type: ignore[attr-defined]  # noqa: F401
    maybe_cast_one_to_many,
)
from chromadb.api.models.CollectionCommon import (  # type: ignore[attr-defined]  # noqa: F401
    normalize_base_record_set,
)
from chromadb.api.models.CollectionCommon import (  # type: ignore[attr-defined]  # noqa: F401
    normalize_insert_record_set,
)
from chromadb.api.models.CollectionCommon import (  # type: ignore[attr-defined]  # noqa: F401
    np,
)
from chromadb.api.models.CollectionCommon import (  # type: ignore[attr-defined]  # noqa: F401
    overwrite_collection_configuration,
)
from chromadb.api.models.CollectionCommon import (  # type: ignore[attr-defined]  # noqa: F401
    validate_base_record_set,
)
from chromadb.api.models.CollectionCommon import (  # type: ignore[attr-defined]  # noqa: F401
    validate_embedding_function,
)
from chromadb.api.models.CollectionCommon import (  # type: ignore[attr-defined]  # noqa: F401
    validate_filter_set,
)
from chromadb.api.models.CollectionCommon import (  # type: ignore[attr-defined]  # noqa: F401
    validate_ids,
)
from chromadb.api.models.CollectionCommon import (  # type: ignore[attr-defined]  # noqa: F401
    validate_include,
)
from chromadb.api.models.CollectionCommon import (  # type: ignore[attr-defined]  # noqa: F401
    validate_insert_record_set,
)
from chromadb.api.models.CollectionCommon import (  # type: ignore[attr-defined]  # noqa: F401
    validate_metadata,
)
from chromadb.api.models.CollectionCommon import (  # type: ignore[attr-defined]  # noqa: F401
    validate_metadatas,
)
from chromadb.api.models.CollectionCommon import (  # type: ignore[attr-defined]  # noqa: F401
    validate_n_results,
)
from chromadb.api.models.CollectionCommon import (  # type: ignore[attr-defined]  # noqa: F401
    validate_record_set_contains_any,
)
from chromadb.api.models.CollectionCommon import (  # type: ignore[attr-defined]  # noqa: F401
    validate_record_set_for_embedding,
)
from chromadb.api.models.CollectionCommon import (  # type: ignore[attr-defined]  # noqa: F401
    validate_sparse_embedding_function,
)
import functools  # noqa: F401
import logging  # noqa: F401
