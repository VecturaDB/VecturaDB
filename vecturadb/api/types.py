# Copyright VecturaDB contributors. Licensed under the Apache License, Version 2.0.
from chromadb.api.types import AddRequest as AddRequest  # noqa: F401
from chromadb.api.types import (  # noqa: F401
    BOOL_INVERTED_INDEX_NAME as BOOL_INVERTED_INDEX_NAME,
)
from chromadb.api.types import BOOL_VALUE_NAME as BOOL_VALUE_NAME  # noqa: F401
from chromadb.api.types import BaseRecordSet as BaseRecordSet  # noqa: F401
from chromadb.api.types import (  # noqa: F401
    BoolInvertedIndexConfig as BoolInvertedIndexConfig,
)
from chromadb.api.types import (  # noqa: F401
    BoolInvertedIndexType as BoolInvertedIndexType,
)
from chromadb.api.types import BoolValueType as BoolValueType  # noqa: F401
from chromadb.api.types import Cmek as Cmek  # noqa: F401
from chromadb.api.types import CmekProvider as CmekProvider  # noqa: F401
from chromadb.api.types import CollectionMetadata as CollectionMetadata  # noqa: F401
from chromadb.api.types import (  # noqa: F401
    ConditionalCommitResult as ConditionalCommitResult,
)
from chromadb.api.types import D as D  # noqa: F401
from chromadb.api.types import DOCUMENT_KEY as DOCUMENT_KEY  # noqa: F401
from chromadb.api.types import DataLoader as DataLoader  # noqa: F401
from chromadb.api.types import (  # noqa: F401
    DefaultEmbeddingFunction as DefaultEmbeddingFunction,
)
from chromadb.api.types import DeleteRequest as DeleteRequest  # noqa: F401
from chromadb.api.types import DeleteResult as DeleteResult  # noqa: F401
from chromadb.api.types import Document as Document  # noqa: F401
from chromadb.api.types import Documents as Documents  # noqa: F401
from chromadb.api.types import EMBEDDING_KEY as EMBEDDING_KEY  # noqa: F401
from chromadb.api.types import Embeddable as Embeddable  # noqa: F401
from chromadb.api.types import Embedding as Embedding  # noqa: F401
from chromadb.api.types import EmbeddingFunction as EmbeddingFunction  # noqa: F401
from chromadb.api.types import Embeddings as Embeddings  # noqa: F401
from chromadb.api.types import (  # noqa: F401
    FLOAT_INVERTED_INDEX_NAME as FLOAT_INVERTED_INDEX_NAME,
)
from chromadb.api.types import (  # noqa: F401
    FLOAT_LIST_VALUE_NAME as FLOAT_LIST_VALUE_NAME,
)
from chromadb.api.types import FLOAT_VALUE_NAME as FLOAT_VALUE_NAME  # noqa: F401
from chromadb.api.types import FTS_INDEX_NAME as FTS_INDEX_NAME  # noqa: F401
from chromadb.api.types import FilterSet as FilterSet  # noqa: F401
from chromadb.api.types import (  # noqa: F401
    FloatInvertedIndexConfig as FloatInvertedIndexConfig,
)
from chromadb.api.types import (  # noqa: F401
    FloatInvertedIndexType as FloatInvertedIndexType,
)
from chromadb.api.types import FloatListValueType as FloatListValueType  # noqa: F401
from chromadb.api.types import FloatValueType as FloatValueType  # noqa: F401
from chromadb.api.types import FtsIndexConfig as FtsIndexConfig  # noqa: F401
from chromadb.api.types import FtsIndexType as FtsIndexType  # noqa: F401
from chromadb.api.types import GetRequest as GetRequest  # noqa: F401
from chromadb.api.types import GetResult as GetResult  # noqa: F401
from chromadb.api.types import HNSW_INDEX_NAME as HNSW_INDEX_NAME  # noqa: F401
from chromadb.api.types import HnswIndexConfig as HnswIndexConfig  # noqa: F401
from chromadb.api.types import ID as ID  # noqa: F401
from chromadb.api.types import IDs as IDs  # noqa: F401
from chromadb.api.types import (  # noqa: F401
    INT_INVERTED_INDEX_NAME as INT_INVERTED_INDEX_NAME,
)
from chromadb.api.types import INT_VALUE_NAME as INT_VALUE_NAME  # noqa: F401
from chromadb.api.types import Image as Image  # noqa: F401
from chromadb.api.types import ImageDType as ImageDType  # noqa: F401
from chromadb.api.types import Images as Images  # noqa: F401
from chromadb.api.types import Include as Include  # noqa: F401
from chromadb.api.types import (  # noqa: F401
    IncludeMetadataDocuments as IncludeMetadataDocuments,
)
from chromadb.api.types import (  # noqa: F401
    IncludeMetadataDocumentsDistances as IncludeMetadataDocumentsDistances,
)
from chromadb.api.types import (  # noqa: F401
    IncludeMetadataDocumentsEmbeddings as IncludeMetadataDocumentsEmbeddings,
)
from chromadb.api.types import (  # noqa: F401
    IncludeMetadataDocumentsEmbeddingsDistances as IncludeMetadataDocumentsEmbeddingsDistances,
)
from chromadb.api.types import IndexConfig as IndexConfig  # noqa: F401
from chromadb.api.types import IndexMetadata as IndexMetadata  # noqa: F401
from chromadb.api.types import IndexingStatus as IndexingStatus  # noqa: F401
from chromadb.api.types import InsertRecordSet as InsertRecordSet  # noqa: F401
from chromadb.api.types import (  # noqa: F401
    IntInvertedIndexConfig as IntInvertedIndexConfig,
)
from chromadb.api.types import (  # noqa: F401
    IntInvertedIndexType as IntInvertedIndexType,
)
from chromadb.api.types import IntValueType as IntValueType  # noqa: F401
from chromadb.api.types import L as L  # noqa: F401
from chromadb.api.types import Loadable as Loadable  # noqa: F401
from chromadb.api.types import (  # noqa: F401
    META_KEY_CHROMA_DOCUMENT as META_KEY_CHROMA_DOCUMENT,
)
from chromadb.api.types import Metadatas as Metadatas  # noqa: F401
from chromadb.api.types import OneOrMany as OneOrMany  # noqa: F401
from chromadb.api.types import Parameter as Parameter  # noqa: F401
from chromadb.api.types import PyEmbedding as PyEmbedding  # noqa: F401
from chromadb.api.types import PyEmbeddings as PyEmbeddings  # noqa: F401
from chromadb.api.types import QueryRequest as QueryRequest  # noqa: F401
from chromadb.api.types import QueryResult as QueryResult  # noqa: F401
from chromadb.api.types import ReadLevel as ReadLevel  # noqa: F401
from chromadb.api.types import SPANN_INDEX_NAME as SPANN_INDEX_NAME  # noqa: F401
from chromadb.api.types import (  # noqa: F401
    SPARSE_VECTOR_INDEX_NAME as SPARSE_VECTOR_INDEX_NAME,
)
from chromadb.api.types import (  # noqa: F401
    SPARSE_VECTOR_TYPE_VALUE as SPARSE_VECTOR_TYPE_VALUE,
)
from chromadb.api.types import (  # noqa: F401
    SPARSE_VECTOR_VALUE_NAME as SPARSE_VECTOR_VALUE_NAME,
)
from chromadb.api.types import (  # noqa: F401
    STRING_INVERTED_INDEX_NAME as STRING_INVERTED_INDEX_NAME,
)
from chromadb.api.types import STRING_VALUE_NAME as STRING_VALUE_NAME  # noqa: F401
from chromadb.api.types import Schema as Schema  # noqa: F401
from chromadb.api.types import SearchResult as SearchResult  # noqa: F401
from chromadb.api.types import SearchResultRow as SearchResultRow  # noqa: F401
from chromadb.api.types import Space as Space  # noqa: F401
from chromadb.api.types import SpannIndexConfig as SpannIndexConfig  # noqa: F401
from chromadb.api.types import (  # noqa: F401
    SparseEmbeddingFunction as SparseEmbeddingFunction,
)
from chromadb.api.types import (  # noqa: F401
    SparseVectorIndexConfig as SparseVectorIndexConfig,
)
from chromadb.api.types import (  # noqa: F401
    SparseVectorIndexType as SparseVectorIndexType,
)
from chromadb.api.types import (  # noqa: F401
    SparseVectorValueType as SparseVectorValueType,
)
from chromadb.api.types import SparseVectors as SparseVectors  # noqa: F401
from chromadb.api.types import (  # noqa: F401
    StringInvertedIndexConfig as StringInvertedIndexConfig,
)
from chromadb.api.types import (  # noqa: F401
    StringInvertedIndexType as StringInvertedIndexType,
)
from chromadb.api.types import StringValueType as StringValueType  # noqa: F401
from chromadb.api.types import T as T  # noqa: F401
from chromadb.api.types import TYPE_KEY as TYPE_KEY  # noqa: F401
from chromadb.api.types import URI as URI  # noqa: F401
from chromadb.api.types import URIs as URIs  # noqa: F401
from chromadb.api.types import (  # noqa: F401
    UpdateCollectionMetadata as UpdateCollectionMetadata,
)
from chromadb.api.types import UpdateRequest as UpdateRequest  # noqa: F401
from chromadb.api.types import UpsertRequest as UpsertRequest  # noqa: F401
from chromadb.api.types import VECTOR_INDEX_NAME as VECTOR_INDEX_NAME  # noqa: F401
from chromadb.api.types import ValueTypes as ValueTypes  # noqa: F401
from chromadb.api.types import VectorIndexConfig as VectorIndexConfig  # noqa: F401
from chromadb.api.types import VectorIndexType as VectorIndexType  # noqa: F401
from chromadb.api.types import _CMEK_GCP_PATTERN as _CMEK_GCP_PATTERN  # noqa: F401
from chromadb.api.types import (  # noqa: F401
    _create_extra_fields_validator as _create_extra_fields_validator,
)
from chromadb.api.types import _get_struct as _get_struct  # noqa: F401
from chromadb.api.types import _to_f32 as _to_f32  # noqa: F401
from chromadb.api.types import (  # noqa: F401
    _validate_metadata_list_value as _validate_metadata_list_value,
)
from chromadb.api.types import (  # noqa: F401
    _validate_record_set_contains as _validate_record_set_contains,
)
from chromadb.api.types import (  # noqa: F401
    _validate_record_set_length_consistency as _validate_record_set_length_consistency,
)
from chromadb.api.types import (  # noqa: F401
    convert_list_embeddings_to_np as convert_list_embeddings_to_np,
)
from chromadb.api.types import (  # noqa: F401
    convert_np_embeddings_to_list as convert_np_embeddings_to_list,
)
from chromadb.api.types import (  # noqa: F401
    deserialize_metadata as deserialize_metadata,
)
from chromadb.api.types import (  # noqa: F401
    get_default_embeddable_record_set_fields as get_default_embeddable_record_set_fields,
)
from chromadb.api.types import is_document as is_document  # noqa: F401
from chromadb.api.types import is_image as is_image  # noqa: F401
from chromadb.api.types import (  # noqa: F401
    maybe_cast_one_to_many as maybe_cast_one_to_many,
)
from chromadb.api.types import (  # noqa: F401
    normalize_base_record_set as normalize_base_record_set,
)
from chromadb.api.types import (  # noqa: F401
    normalize_embeddings as normalize_embeddings,
)
from chromadb.api.types import (  # noqa: F401
    normalize_insert_record_set as normalize_insert_record_set,
)
from chromadb.api.types import normalize_metadata as normalize_metadata  # noqa: F401
from chromadb.api.types import normalize_metadatas as normalize_metadatas  # noqa: F401
from chromadb.api.types import (  # noqa: F401
    optional_base64_strings_to_embeddings as optional_base64_strings_to_embeddings,
)
from chromadb.api.types import (  # noqa: F401
    optional_embeddings_to_base64_strings as optional_embeddings_to_base64_strings,
)
from chromadb.api.types import (  # noqa: F401
    pack_embedding_safely as pack_embedding_safely,
)
from chromadb.api.types import serialize_metadata as serialize_metadata  # noqa: F401
from chromadb.api.types import (  # noqa: F401
    validate_base_record_set as validate_base_record_set,
)
from chromadb.api.types import validate_batch as validate_batch  # noqa: F401
from chromadb.api.types import validate_documents as validate_documents  # noqa: F401
from chromadb.api.types import (  # noqa: F401
    validate_embedding_function as validate_embedding_function,
)
from chromadb.api.types import validate_embeddings as validate_embeddings  # noqa: F401
from chromadb.api.types import validate_filter_set as validate_filter_set  # noqa: F401
from chromadb.api.types import validate_ids as validate_ids  # noqa: F401
from chromadb.api.types import validate_images as validate_images  # noqa: F401
from chromadb.api.types import validate_include as validate_include  # noqa: F401
from chromadb.api.types import (  # noqa: F401
    validate_insert_record_set as validate_insert_record_set,
)
from chromadb.api.types import validate_metadata as validate_metadata  # noqa: F401
from chromadb.api.types import validate_metadatas as validate_metadatas  # noqa: F401
from chromadb.api.types import validate_n_results as validate_n_results  # noqa: F401
from chromadb.api.types import (  # noqa: F401
    validate_record_set_contains_any as validate_record_set_contains_any,
)
from chromadb.api.types import (  # noqa: F401
    validate_record_set_contains_one as validate_record_set_contains_one,
)
from chromadb.api.types import (  # noqa: F401
    validate_record_set_for_embedding as validate_record_set_for_embedding,
)
from chromadb.api.types import (  # noqa: F401
    validate_sparse_embedding_function as validate_sparse_embedding_function,
)
from chromadb.api.types import (  # noqa: F401
    validate_sparse_vectors as validate_sparse_vectors,
)
from chromadb.api.types import (  # noqa: F401
    validate_update_metadata as validate_update_metadata,
)
from chromadb.api.types import validate_where as validate_where  # noqa: F401
from chromadb.api.types import (  # noqa: F401
    validate_where_document as validate_where_document,
)
from chromadb.api.types import Any  # type: ignore[attr-defined]  # noqa: F401
from chromadb.api.types import BaseModel  # type: ignore[attr-defined]  # noqa: F401
from chromadb.api.types import Dict  # type: ignore[attr-defined]  # noqa: F401
from chromadb.api.types import Enum  # type: ignore[attr-defined]  # noqa: F401
from chromadb.api.types import Final  # type: ignore[attr-defined]  # noqa: F401
from chromadb.api.types import Key  # type: ignore[attr-defined]  # noqa: F401
from chromadb.api.types import List  # type: ignore[attr-defined]  # noqa: F401
from chromadb.api.types import Literal  # type: ignore[attr-defined]  # noqa: F401
from chromadb.api.types import LiteralValue  # type: ignore[attr-defined]  # noqa: F401
from chromadb.api.types import (  # type: ignore[attr-defined]  # noqa: F401
    LogicalOperator,
)
from chromadb.api.types import Metadata  # noqa: F401
from chromadb.api.types import NDArray  # type: ignore[attr-defined]  # noqa: F401
from chromadb.api.types import (  # type: ignore[attr-defined]  # noqa: F401
    OperatorExpression,
)
from chromadb.api.types import Optional  # type: ignore[attr-defined]  # noqa: F401
from chromadb.api.types import Protocol  # type: ignore[attr-defined]  # noqa: F401
from chromadb.api.types import PyVector  # type: ignore[attr-defined]  # noqa: F401
from chromadb.api.types import (  # type: ignore[attr-defined]  # noqa: F401
    PydanticCustomError,
)
from chromadb.api.types import Set  # type: ignore[attr-defined]  # noqa: F401
from chromadb.api.types import SparseVector  # noqa: F401
from chromadb.api.types import TYPE_CHECKING  # type: ignore[attr-defined]  # noqa: F401
from chromadb.api.types import Tuple  # type: ignore[attr-defined]  # noqa: F401
from chromadb.api.types import Type  # type: ignore[attr-defined]  # noqa: F401
from chromadb.api.types import TypeAlias  # type: ignore[attr-defined]  # noqa: F401
from chromadb.api.types import TypeVar  # type: ignore[attr-defined]  # noqa: F401
from chromadb.api.types import TypedDict  # type: ignore[attr-defined]  # noqa: F401
from chromadb.api.types import Union  # type: ignore[attr-defined]  # noqa: F401
from chromadb.api.types import UpdateMetadata  # noqa: F401
from chromadb.api.types import Vector  # type: ignore[attr-defined]  # noqa: F401
from chromadb.api.types import Where  # noqa: F401
from chromadb.api.types import WhereDocument  # noqa: F401
from chromadb.api.types import (  # type: ignore[attr-defined]  # noqa: F401
    WhereDocumentOperator,
)
from chromadb.api.types import WhereOperator  # type: ignore[attr-defined]  # noqa: F401
from chromadb.api.types import (  # type: ignore[attr-defined]  # noqa: F401
    abstractmethod,
)
from chromadb.api.types import cast  # type: ignore[attr-defined]  # noqa: F401
from chromadb.api.types import dataclass  # type: ignore[attr-defined]  # noqa: F401
from chromadb.api.types import deepcopy  # type: ignore[attr-defined]  # noqa: F401
from chromadb.api.types import errors  # type: ignore[attr-defined]  # noqa: F401
from chromadb.api.types import (  # type: ignore[attr-defined]  # noqa: F401
    field_validator,
)
from chromadb.api.types import get_args  # type: ignore[attr-defined]  # noqa: F401
from chromadb.api.types import lru_cache  # type: ignore[attr-defined]  # noqa: F401
from chromadb.api.types import (  # type: ignore[attr-defined]  # noqa: F401
    model_validator,
)
from chromadb.api.types import np  # type: ignore[attr-defined]  # noqa: F401
from chromadb.api.types import retry  # type: ignore[attr-defined]  # noqa: F401
from chromadb.api.types import (  # type: ignore[attr-defined]  # noqa: F401
    runtime_checkable,
)
from chromadb.api.types import signature  # type: ignore[attr-defined]  # noqa: F401
import math  # noqa: F401
import os  # noqa: F401
import pybase64  # noqa: F401
import re  # noqa: F401
import struct  # noqa: F401
import warnings  # noqa: F401

try:
    from chromadb.is_thin_client import is_thin_client  # noqa: F401,F811
except ImportError:
    is_thin_client = False  # noqa: F811
LiteralValue = LiteralValue
LogicalOperator = LogicalOperator
OperatorExpression = OperatorExpression
Where = Where
WhereDocumentOperator = WhereDocumentOperator
WhereOperator = WhereOperator
__all__ = [
    "Metadata",
    "Where",
    "WhereDocument",
    "UpdateCollectionMetadata",
    "UpdateMetadata",
    "SearchResult",
    "SearchResultRow",
    "IndexingStatus",
    "SparseVector",
    "FtsIndexConfig",
    "HnswIndexConfig",
    "SpannIndexConfig",
    "VectorIndexConfig",
    "SparseVectorIndexConfig",
    "StringInvertedIndexConfig",
    "IntInvertedIndexConfig",
    "FloatInvertedIndexConfig",
    "BoolInvertedIndexConfig",
    "IndexConfig",
    "Schema",
    "ValueTypes",
    "StringValueType",
    "FloatListValueType",
    "SparseVectorValueType",
    "IntValueType",
    "FloatValueType",
    "BoolValueType",
    "FtsIndexType",
    "VectorIndexType",
    "SparseVectorIndexType",
    "StringInvertedIndexType",
    "IntInvertedIndexType",
    "FloatInvertedIndexType",
    "BoolInvertedIndexType",
    "STRING_VALUE_NAME",
    "INT_VALUE_NAME",
    "BOOL_VALUE_NAME",
    "FLOAT_VALUE_NAME",
    "FLOAT_LIST_VALUE_NAME",
    "SPARSE_VECTOR_VALUE_NAME",
    "FTS_INDEX_NAME",
    "VECTOR_INDEX_NAME",
    "SPARSE_VECTOR_INDEX_NAME",
    "STRING_INVERTED_INDEX_NAME",
    "INT_INVERTED_INDEX_NAME",
    "FLOAT_INVERTED_INDEX_NAME",
    "BOOL_INVERTED_INDEX_NAME",
    "HNSW_INDEX_NAME",
    "SPANN_INDEX_NAME",
    "DOCUMENT_KEY",
    "EMBEDDING_KEY",
    "TYPE_KEY",
    "SPARSE_VECTOR_TYPE_VALUE",
    "Cmek",
    "CmekProvider",
    "Space",
    "EmbeddingFunction",
    "SparseEmbeddingFunction",
    "validate_embedding_function",
    "validate_sparse_embedding_function",
    "SparseVector",
    "SparseVectors",
    "validate_sparse_vectors",
]
