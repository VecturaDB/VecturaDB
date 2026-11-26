# Copyright VecturaDB contributors. Licensed under the Apache License, Version 2.0.
from chromadb.api.client import AdminClient as AdminClient  # noqa: F401
from chromadb.api.client import Client as Client  # noqa: F401
from chromadb.api.client import AdminAPI  # type: ignore[attr-defined]  # noqa: F401
from chromadb.api.client import (  # type: ignore[attr-defined]  # noqa: F401
    ChromaAuthError,
)
from chromadb.api.client import ChromaError  # type: ignore[attr-defined]  # noqa: F401
from chromadb.api.client import ClientAPI  # type: ignore[attr-defined]  # noqa: F401
from chromadb.api.client import Collection  # type: ignore[attr-defined]  # noqa: F401
from chromadb.api.client import (  # type: ignore[attr-defined]  # noqa: F401
    CollectionMetadata,
)
from chromadb.api.client import (  # type: ignore[attr-defined]  # noqa: F401
    ConditionalCommitResult,
)
from chromadb.api.client import (  # type: ignore[attr-defined]  # noqa: F401
    CreateCollectionConfiguration,
)
from chromadb.api.client import (  # type: ignore[attr-defined]  # noqa: F401
    DEFAULT_DATABASE,
)
from chromadb.api.client import (  # type: ignore[attr-defined]  # noqa: F401
    DEFAULT_TENANT,
)
from chromadb.api.client import DataLoader  # type: ignore[attr-defined]  # noqa: F401
from chromadb.api.client import Database  # type: ignore[attr-defined]  # noqa: F401
from chromadb.api.client import (  # type: ignore[attr-defined]  # noqa: F401
    DefaultEmbeddingFunction,
)
from chromadb.api.client import DeleteResult  # type: ignore[attr-defined]  # noqa: F401
from chromadb.api.client import Documents  # type: ignore[attr-defined]  # noqa: F401
from chromadb.api.client import Embeddable  # type: ignore[attr-defined]  # noqa: F401
from chromadb.api.client import (  # type: ignore[attr-defined]  # noqa: F401
    EmbeddingFunction,
)
from chromadb.api.client import Embeddings  # type: ignore[attr-defined]  # noqa: F401
from chromadb.api.client import GetResult  # type: ignore[attr-defined]  # noqa: F401
from chromadb.api.client import IDs  # type: ignore[attr-defined]  # noqa: F401
from chromadb.api.client import Include  # type: ignore[attr-defined]  # noqa: F401
from chromadb.api.client import (  # type: ignore[attr-defined]  # noqa: F401
    IncludeMetadataDocuments,
)
from chromadb.api.client import (  # type: ignore[attr-defined]  # noqa: F401
    IncludeMetadataDocumentsDistances,
)
from chromadb.api.client import Loadable  # type: ignore[attr-defined]  # noqa: F401
from chromadb.api.client import Metadatas  # type: ignore[attr-defined]  # noqa: F401
from chromadb.api.client import Optional  # type: ignore[attr-defined]  # noqa: F401
from chromadb.api.client import QueryResult  # type: ignore[attr-defined]  # noqa: F401
from chromadb.api.client import Schema  # type: ignore[attr-defined]  # noqa: F401
from chromadb.api.client import Sequence  # type: ignore[attr-defined]  # noqa: F401
from chromadb.api.client import ServerAPI  # type: ignore[attr-defined]  # noqa: F401
from chromadb.api.client import Settings  # type: ignore[attr-defined]  # noqa: F401
from chromadb.api.client import (  # type: ignore[attr-defined]  # noqa: F401
    SharedSystemClient,
)
from chromadb.api.client import System  # type: ignore[attr-defined]  # noqa: F401
from chromadb.api.client import Tenant  # type: ignore[attr-defined]  # noqa: F401
from chromadb.api.client import (  # type: ignore[attr-defined]  # noqa: F401
    TracebackType,
)
from chromadb.api.client import URIs  # type: ignore[attr-defined]  # noqa: F401
from chromadb.api.client import UUID  # type: ignore[attr-defined]  # noqa: F401
from chromadb.api.client import (  # type: ignore[attr-defined]  # noqa: F401
    UpdateCollectionConfiguration,
)
from chromadb.api.client import UserIdentity  # type: ignore[attr-defined]  # noqa: F401
from chromadb.api.client import Where  # type: ignore[attr-defined]  # noqa: F401
from chromadb.api.client import (  # type: ignore[attr-defined]  # noqa: F401
    WhereDocument,
)
from chromadb.api.client import (  # type: ignore[attr-defined]  # noqa: F401
    maybe_set_tenant_and_database,
)
from chromadb.api.client import override  # type: ignore[attr-defined]  # noqa: F401
from chromadb.api.client import (  # type: ignore[attr-defined]  # noqa: F401
    validate_embedding_function_conflict_on_create,
)
from chromadb.api.client import (  # type: ignore[attr-defined]  # noqa: F401
    validate_embedding_function_conflict_on_get,
)
import httpx  # noqa: F401
