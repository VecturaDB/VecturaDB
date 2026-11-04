from pathlib import Path
from typing import Optional

import pytest
from fastapi import HTTPException

from chromadb.auth import (
    AuthzAction,
    AuthzResource,
    UserIdentity,
    user_can_access_resource,
)
from chromadb.auth.simple_rbac_authz import SimpleRBACAuthorizationProvider
from chromadb.config import Settings, System

# The users of the credentials file of the token authentication provider.
ADMIN = UserIdentity(user_id="admin", tenant="*", databases=["*"])
TEAM_A = UserIdentity(user_id="team_a", tenant="tenant_a", databases=["*"])
TEAM_A_DB = UserIdentity(user_id="team_a_db", tenant="tenant_a", databases=["db_a"])


def resource(tenant: Optional[str], database: Optional[str] = None) -> AuthzResource:
    return AuthzResource(tenant=tenant, database=database, collection=None)


# The same answers as the API keys of the Rust server (rust/frontend/src/token_auth.rs).
def test_users_reach_their_tenant_and_databases_only() -> None:
    can = user_can_access_resource
    list_collections = AuthzAction.LIST_COLLECTIONS
    # The user of a tenant: every database of its tenant, no other tenant.
    assert can(TEAM_A, list_collections, resource("tenant_a", "db_a"))
    assert can(TEAM_A, list_collections, resource("tenant_a", "other"))
    assert can(TEAM_A, AuthzAction.CREATE_DATABASE, resource("tenant_a", "new"))
    assert not can(TEAM_A, list_collections, resource("tenant_b", "db_b"))
    assert not can(TEAM_A, AuthzAction.GET_TENANT, resource("default_tenant"))
    # A resource without a tenant, as the v1 routes gave, is in no tenant.
    assert not can(TEAM_A, AuthzAction.GET, resource(None))
    assert not can(TEAM_A, AuthzAction.RESET, resource(None))
    # The user of one database: that database, the list of the databases, nothing else.
    assert can(TEAM_A_DB, list_collections, resource("tenant_a", "db_a"))
    assert can(TEAM_A_DB, AuthzAction.LIST_DATABASES, resource("tenant_a"))
    assert not can(TEAM_A_DB, list_collections, resource("tenant_a", "other"))
    assert not can(TEAM_A_DB, AuthzAction.CREATE_DATABASE, resource("tenant_a", "new"))
    assert not can(TEAM_A_DB, AuthzAction.DELETE_DATABASE, resource("tenant_a", "db_a"))
    # The user of every tenant.
    assert can(ADMIN, list_collections, resource("tenant_b", "db_b"))
    assert can(ADMIN, AuthzAction.CREATE_TENANT, resource("new"))
    assert not can(TEAM_A, AuthzAction.CREATE_TENANT, resource("new"))
    # A user without a tenant, as the basic authentication provider gives, is not limited.
    assert can(
        UserIdentity(user_id="basic"), list_collections, resource("tenant_b", "db_b")
    )


def test_simple_rbac_checks_the_tenant_and_the_database(tmp_path: Path) -> None:
    config = tmp_path / "authz.yaml"
    config.write_text(
        f"""
roles_mapping:
  reader:
    actions:
      - {AuthzAction.GET.value}
users:
  - id: admin
    role: reader
  - id: team_a
    role: reader
  - id: team_a_db
    role: reader
"""
    )
    provider = SimpleRBACAuthorizationProvider(
        System(
            Settings(
                chroma_server_authz_provider="chromadb.auth.simple_rbac_authz.SimpleRBACAuthorizationProvider",
                chroma_server_authz_config_file=str(config),
            )
        )
    )

    provider.authorize_or_raise(TEAM_A, AuthzAction.GET, resource("tenant_a", "db_a"))
    provider.authorize_or_raise(
        TEAM_A_DB, AuthzAction.GET, resource("tenant_a", "db_a")
    )
    provider.authorize_or_raise(ADMIN, AuthzAction.GET, resource("tenant_b", "db_b"))
    for user, action, denied in [
        (TEAM_A, AuthzAction.GET, resource("tenant_b", "db_b")),
        (TEAM_A, AuthzAction.GET, resource(None)),
        (TEAM_A_DB, AuthzAction.GET, resource("tenant_a", "other")),
        # The role still decides the actions.
        (TEAM_A, AuthzAction.ADD, resource("tenant_a", "db_a")),
    ]:
        with pytest.raises(HTTPException) as error:
            provider.authorize_or_raise(user, action, denied)
        assert error.value.status_code == 403
