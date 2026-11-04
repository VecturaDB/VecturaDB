import uuid
from pathlib import Path
from typing import Any, Callable, Dict, List

import orjson
import pytest
from anyio import to_thread
from fastapi.testclient import TestClient

import chromadb.server.fastapi as fastapi_server
from chromadb.auth import AuthzAction
from chromadb.config import Settings
from chromadb.server.fastapi import FastAPI


class FakeRequest:
    headers: Dict[str, str] = {}

    def __init__(self, body: Dict[str, Any]) -> None:
        self._body = orjson.dumps(body)

    async def body(self) -> bytes:
        return self._body


class ExplodingApi:
    def create_collection(self, **_kwargs: Any) -> None:
        raise AssertionError("collection creation should not be reached")


class NoopRateLimitEnforcer:
    def rate_limit(self, func: Callable[..., Any]) -> Callable[..., Any]:
        return func


async def run_sync_immediately(
    func: Callable[..., Any], *args: Any, limiter: Any = None
) -> Any:
    del limiter
    return func(*args)


def create_collection_body() -> Dict[str, Any]:
    return {
        "name": "poisoned",
        "configuration": {
            "embedding_function": {
                "name": "sentence_transformer",
                "type": "known",
                "config": {
                    "model_name": "attacker/model",
                    "device": "cpu",
                    "normalize_embeddings": False,
                    "kwargs": {"model_kwargs": {"trust_remote_code": True}},
                },
            }
        },
    }


def make_uninitialized_fastapi() -> Any:
    server: Any = FastAPI.__new__(FastAPI)
    server._api = ExplodingApi()
    server._capacity_limiter = None
    server._async_rate_limit_enforcer = NoopRateLimitEnforcer()
    server._set_request_context = lambda request: None
    return server


@pytest.mark.asyncio
async def test_v2_create_collection_authenticates_before_loading_configuration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: List[str] = []
    server = make_uninitialized_fastapi()

    def fail_auth(*_args: Any, **_kwargs: Any) -> None:
        calls.append("auth")
        raise RuntimeError("unauthorized")

    def load_configuration(config: Dict[str, Any]) -> Dict[str, Any]:
        del config
        calls.append("load_configuration")
        return {}

    server.sync_auth_request = fail_auth
    monkeypatch.setattr(to_thread, "run_sync", run_sync_immediately)
    monkeypatch.setattr(
        fastapi_server,
        "load_create_collection_configuration_from_json",
        load_configuration,
    )

    with pytest.raises(RuntimeError, match="unauthorized"):
        await server.create_collection(
            FakeRequest(create_collection_body()),
            tenant="default_tenant",
            database_name="default_database",
        )

    assert calls == ["auth"]


@pytest.mark.asyncio
async def test_v1_create_collection_authenticates_before_loading_configuration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: List[str] = []
    server = make_uninitialized_fastapi()

    def fail_auth(*_args: Any, **_kwargs: Any) -> None:
        calls.append("auth")
        raise RuntimeError("unauthorized")

    def load_configuration(config: Dict[str, Any]) -> Dict[str, Any]:
        del config
        calls.append("load_configuration")
        return {}

    server.sync_auth_and_get_tenant_and_database_for_request = fail_auth
    monkeypatch.setattr(to_thread, "run_sync", run_sync_immediately)
    monkeypatch.setattr(
        fastapi_server,
        "load_create_collection_configuration_from_json",
        load_configuration,
    )

    with pytest.raises(RuntimeError, match="unauthorized"):
        await server.create_collection_v1(
            FakeRequest(create_collection_body()),
            tenant="default_tenant",
            database="default_database",
        )

    assert calls == ["auth"]


def segment_api_settings(**settings: Any) -> Settings:
    return Settings(
        chroma_api_impl="chromadb.api.segment.SegmentAPI",
        chroma_sysdb_impl="chromadb.db.impl.sqlite.SqliteDB",
        chroma_producer_impl="chromadb.db.impl.sqlite.SqliteDB",
        chroma_consumer_impl="chromadb.db.impl.sqlite.SqliteDB",
        chroma_segment_manager_impl="chromadb.segment.impl.manager.local.LocalSegmentManager",
        is_persistent=False,
        allow_reset=True,
        **settings,
    )


# As the Rust server, every v1 route answers 410: the v1 routes authorized a collection
# without its tenant and database.
def test_v1_routes_answer_gone() -> None:
    with TestClient(FastAPI(segment_api_settings()).app()) as client:
        for method, path in (
            ("GET", "/api/v1/heartbeat"),
            ("POST", "/api/v1/collections"),
            ("POST", f"/api/v1/collections/{uuid.uuid4()}/get"),
            ("POST", f"/api/v1/collections/{uuid.uuid4()}/add"),
            ("DELETE", "/api/v1/collections/name"),
        ):
            response = client.request(method, path, json={})
            assert response.status_code == 410, path
            assert response.json() == {
                "error": "Unimplemented",
                "message": "The v1 API is deprecated. Please use /v2 apis",
            }


# With SimpleRBAC, and with authentication only: in both, a user of a tenant
# reaches that tenant only, as with the API keys of the Rust server.
@pytest.mark.parametrize("rbac", [True, False], ids=["rbac", "authn_only"])
def test_user_reaches_its_tenant_only(tmp_path: Path, rbac: bool) -> None:
    authn = tmp_path / "authn.yaml"
    authn.write_text(
        """
users:
  - id: admin
    tokens:
      - admin-token
  - id: team_a
    tenant: tenant_a
    databases:
      - db_a
    tokens:
      - token-a
"""
    )
    actions = "\n".join(f"      - {action.value}" for action in AuthzAction)
    authz = tmp_path / "authz.yaml"
    authz.write_text(
        f"""
roles_mapping:
  all:
    actions:
{actions}
users:
  - id: admin
    role: all
  - id: team_a
    role: all
"""
    )
    authorization = (
        {
            "chroma_server_authz_provider": "chromadb.auth.simple_rbac_authz.SimpleRBACAuthorizationProvider",
            "chroma_server_authz_config_file": str(authz),
        }
        if rbac
        else {}
    )
    server = FastAPI(
        segment_api_settings(
            chroma_server_authn_provider="chromadb.auth.token_authn.TokenAuthenticationServerProvider",
            chroma_server_authn_credentials_file=str(authn),
            **authorization,
        )
    )
    admin = {"Authorization": "Bearer admin-token"}
    team_a = {"Authorization": "Bearer token-a"}
    record = {"ids": ["x"], "embeddings": [[1.0, 2.0]]}
    with TestClient(server.app()) as client:
        ids = {}
        for tenant, database in (("tenant_a", "db_a"), ("tenant_b", "db_b")):
            collections = f"/api/v2/tenants/{tenant}/databases/{database}/collections"
            client.post(
                "/api/v2/tenants", json={"name": tenant}, headers=admin
            ).raise_for_status()
            client.post(
                f"/api/v2/tenants/{tenant}/databases",
                json={"name": database},
                headers=admin,
            ).raise_for_status()
            created = client.post(
                collections, json={"name": "collection"}, headers=admin
            )
            created.raise_for_status()
            ids[tenant] = created.json()["id"]
            client.post(
                f"{collections}/{ids[tenant]}/add", json=record, headers=admin
            ).raise_for_status()

        a = "/api/v2/tenants/tenant_a/databases/db_a/collections"
        b = "/api/v2/tenants/tenant_b/databases/db_b/collections"
        get = {"ids": ["x"]}
        # Its own collection.
        response = client.post(f"{a}/{ids['tenant_a']}/get", json=get, headers=team_a)
        assert response.status_code == 200
        assert response.json()["ids"] == ["x"]
        # The path of another tenant: the server refuses it.
        response = client.post(f"{b}/{ids['tenant_b']}/get", json=get, headers=team_a)
        assert response.status_code == 403
        response = client.post(
            "/api/v2/tenants", json={"name": "tenant_c"}, headers=team_a
        )
        assert response.status_code == 403
        # Its own path with the id of a collection of another tenant: there the
        # collection does not exist.
        for operation, body in (("get", get), ("add", record), ("delete", get)):
            response = client.post(
                f"{a}/{ids['tenant_b']}/{operation}", json=body, headers=team_a
            )
            assert response.status_code == 404, operation
        response = client.get(f"{b}/{ids['tenant_b']}/count", headers=admin)
        assert response.json() == 1
