from typing import Callable, Generator, List

import numpy as np
import pytest

from chromadb.api import ServerAPI
from chromadb.api.types import Embeddings
from chromadb.config import Settings, System
from chromadb.errors import NotFoundError


@pytest.fixture
def api() -> Generator[ServerAPI, None, None]:
    system = System(
        Settings(
            chroma_api_impl="chromadb.api.segment.SegmentAPI",
            chroma_sysdb_impl="chromadb.db.impl.sqlite.SqliteDB",
            chroma_producer_impl="chromadb.db.impl.sqlite.SqliteDB",
            chroma_consumer_impl="chromadb.db.impl.sqlite.SqliteDB",
            chroma_segment_manager_impl="chromadb.segment.impl.manager.local.LocalSegmentManager",
            is_persistent=False,
            allow_reset=True,
        )
    )
    api = system.instance(ServerAPI)
    system.start()
    yield api
    system.stop()


# The sysdb finds a collection by its id in any tenant: a request in another tenant, or in another
# database of the same tenant, must find nothing, as in the frontend of the Rust server.
def test_collection_of_another_tenant_or_database_does_not_exist(
    api: ServerAPI,
) -> None:
    api.create_tenant("tenant_a")
    api.create_database("db_a", tenant="tenant_a")
    api.create_tenant("tenant_b")
    api.create_database("db_b", tenant="tenant_b")
    api.create_database("db_other", tenant="tenant_b")
    collection = api.create_collection(
        "collection_b", tenant="tenant_b", database="db_b"
    )
    embeddings: Embeddings = [np.array([1.0, 2.0], dtype=np.float32)]
    api._add(["x"], collection.id, embeddings, tenant="tenant_b", database="db_b")

    for tenant, database in (("tenant_a", "db_a"), ("tenant_b", "db_other")):
        operations: List[Callable[[], object]] = [
            lambda: api.get_collection_by_id(
                collection.id, tenant=tenant, database=database
            ),
            lambda: api._modify(
                collection.id, new_name="renamed", tenant=tenant, database=database
            ),
            lambda: api._add(
                ["y"], collection.id, embeddings, tenant=tenant, database=database
            ),
            lambda: api._update(
                collection.id,
                ["x"],
                embeddings=embeddings,
                tenant=tenant,
                database=database,
            ),
            lambda: api._upsert(
                collection.id, ["x"], embeddings, tenant=tenant, database=database
            ),
            lambda: api._get(
                collection.id, ids=["x"], tenant=tenant, database=database
            ),
            lambda: api._delete(
                collection.id, ids=["x"], tenant=tenant, database=database
            ),
            lambda: api._count(collection.id, tenant=tenant, database=database),
            lambda: api._query(
                collection.id,
                query_embeddings=embeddings,
                n_results=1,
                tenant=tenant,
                database=database,
            ),
            lambda: api._peek(collection.id, tenant=tenant, database=database),
        ]
        for operation in operations:
            with pytest.raises(NotFoundError):
                operation()

    # Nothing changed, and the collection answers in its own tenant and database.
    assert (
        api.get_collection_by_id(collection.id, "tenant_b", "db_b").name
        == "collection_b"
    )
    assert api._count(collection.id, tenant="tenant_b", database="db_b") == 1
    assert api._get(collection.id, tenant="tenant_b", database="db_b")["ids"] == ["x"]
