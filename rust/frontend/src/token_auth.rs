//! API keys on a single node. The users and their keys come from a file in the format of Chroma 0.5
//! (chromadb/auth/token_authn: users with an id, a tenant, databases and tokens), and the server answers as Chroma
//! Cloud does: 401 without a valid key, 403 outside the tenant and the databases of the key, and the identity of the key.

use std::collections::{HashMap, HashSet};
use std::future::{ready, Future};
use std::pin::Pin;

use axum::http::{HeaderMap, StatusCode};
use chroma_api_types::GetUserIdentityResponse;
use chroma_types::Collection;
use figment::providers::{Format, Yaml};
use serde::Deserialize;

use crate::auth::{AuthError, AuthenticateAndAuthorize, AuthzAction, AuthzResource};
use crate::config::FrontendServerConfig;

/// Every tenant, or every database of a tenant, as in Chroma 0.5.
const ALL: &str = "*";

/// The header that carries the key, as `chroma_auth_token_transport_header` in Chroma 0.5. The default is the header of
/// Chroma Cloud.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub enum TokenTransportHeader {
    /// `Authorization: Bearer <key>`
    Authorization,
    /// `X-Chroma-Token: <key>`
    #[default]
    XChromaToken,
}

impl TryFrom<&str> for TokenTransportHeader {
    type Error = TokenAuthenticationError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "Authorization" => Ok(Self::Authorization),
            "X-Chroma-Token" => Ok(Self::XChromaToken),
            _ => Err(TokenAuthenticationError::InvalidHeader(value.to_string())),
        }
    }
}

#[derive(Debug, Deserialize)]
struct CredentialsFile {
    users: Vec<CredentialsUser>,
}

#[derive(Debug, Deserialize)]
struct CredentialsUser {
    id: String,
    tenant: Option<String>,
    databases: Option<Vec<String>>,
    tokens: Vec<String>,
}

#[derive(Clone, Debug, PartialEq)]
struct User {
    id: String,
    tenant: String,
    databases: Vec<String>,
}

#[derive(thiserror::Error, Debug)]
pub enum TokenAuthenticationError {
    #[error("Both credentials file and credentials found. Please provide only one.")]
    Both,
    #[error("Could not read the credentials file {0}: {1}")]
    File(String, String),
    #[error("Invalid token. Must contain only ASCII letters, digits, and punctuation.")]
    InvalidToken,
    #[error("Token already in use: wanted to use it for user {0} but it's already in use by user {1}")]
    TokenInUse(String, String),
    #[error("Invalid token transport header: {0}. Must be one of [Authorization, X-Chroma-Token]")]
    InvalidHeader(String),
}

// As Chroma 0.5: ASCII letters, digits and punctuation.
fn valid_token(token: &str) -> bool {
    token
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c.is_ascii_punctuation())
}

#[derive(Debug)]
pub struct TokenAuthentication {
    users: HashMap<String, User>,
    header: TokenTransportHeader,
}

impl TokenAuthentication {
    /// The keys of the server configuration, or None when it names neither a credentials file nor a key: the server
    /// then allows every request, as Chroma without an authentication provider.
    pub fn from_config(
        config: &FrontendServerConfig,
    ) -> Result<Option<Self>, TokenAuthenticationError> {
        let header = config
            .auth_token_transport_header
            .as_deref()
            .map(TokenTransportHeader::try_from)
            .transpose()?
            .unwrap_or_default();
        let users = match (
            &config.server_authn_credentials_file,
            &config.server_authn_credentials,
        ) {
            (Some(_), Some(_)) => return Err(TokenAuthenticationError::Both),
            (Some(path), None) => {
                let read_error =
                    |err: &dyn std::fmt::Display| TokenAuthenticationError::File(path.clone(), err.to_string());
                let content = std::fs::read_to_string(path).map_err(|err| read_error(&err))?;
                let file: CredentialsFile =
                    figment::Figment::from(Yaml::string(&content))
                        .extract()
                        .map_err(|err| read_error(&err))?;
                file.users
                    .into_iter()
                    .map(|user| {
                        (
                            User {
                                id: user.id,
                                tenant: user.tenant.unwrap_or_else(|| ALL.to_string()),
                                databases: user.databases.unwrap_or_else(|| vec![ALL.to_string()]),
                            },
                            user.tokens,
                        )
                    })
                    .collect::<Vec<_>>()
            }
            // A single key, as `chroma_server_authn_credentials`: the anonymous user of every tenant.
            (None, Some(key)) => vec![(
                User {
                    id: "anonymous".to_string(),
                    tenant: ALL.to_string(),
                    databases: vec![ALL.to_string()],
                },
                vec![key.clone()],
            )],
            (None, None) => return Ok(None),
        };
        let mut by_token: HashMap<String, User> = HashMap::new();
        for (user, tokens) in users {
            for token in tokens {
                if !valid_token(&token) {
                    return Err(TokenAuthenticationError::InvalidToken);
                }
                if let Some(other) = by_token.get(&token) {
                    if *other != user {
                        return Err(TokenAuthenticationError::TokenInUse(
                            user.id.clone(),
                            other.id.clone(),
                        ));
                    }
                }
                by_token.insert(token, user.clone());
            }
        }
        Ok(Some(Self {
            users: by_token,
            header,
        }))
    }

    /// The user of the key in the request, or 401.
    fn authenticate(&self, headers: &HeaderMap) -> Result<&User, AuthError> {
        let unauthorized = || AuthError(StatusCode::UNAUTHORIZED);
        let token = match self.header {
            TokenTransportHeader::XChromaToken => headers
                .get("x-chroma-token")
                .and_then(|value| value.to_str().ok())
                .ok_or_else(unauthorized)?,
            TokenTransportHeader::Authorization => headers
                .get("authorization")
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.strip_prefix("Bearer "))
                .ok_or_else(unauthorized)?,
        }
        .trim();
        if !valid_token(token) {
            return Err(unauthorized());
        }
        self.users.get(token).ok_or_else(unauthorized)
    }

    /// As the keys of Chroma Cloud: a key sees its tenant only, and a key of some databases sees those databases only,
    /// may list the databases of its tenant but not create or delete one. A key of every tenant may do anything.
    fn authorize(
        &self,
        user: &User,
        action: AuthzAction,
        resource: &AuthzResource,
    ) -> Result<(), AuthError> {
        let forbidden = || Err(AuthError(StatusCode::FORBIDDEN));
        if user.tenant == ALL {
            return Ok(());
        }
        // Tenants and the whole server belong to no tenant key.
        if matches!(action, AuthzAction::CreateTenant | AuthzAction::Reset) {
            return forbidden();
        }
        if resource.tenant.as_deref() != Some(user.tenant.as_str()) {
            return forbidden();
        }
        if user.databases.iter().any(|database| database == ALL) {
            return Ok(());
        }
        match action {
            AuthzAction::GetTenant | AuthzAction::ListDatabases => Ok(()),
            AuthzAction::CreateDatabase | AuthzAction::DeleteDatabase | AuthzAction::UpdateTenant => {
                forbidden()
            }
            _ => match &resource.database {
                Some(database) if user.databases.contains(database) => Ok(()),
                _ => forbidden(),
            },
        }
    }

    // As Chroma Cloud, an empty list of databases for a key of every database.
    fn identity(user: &User) -> GetUserIdentityResponse {
        GetUserIdentityResponse {
            user_id: user.id.clone(),
            tenant: user.tenant.clone(),
            databases: if user.databases.iter().any(|database| database == ALL) {
                HashSet::new()
            } else {
                user.databases.iter().cloned().collect()
            },
        }
    }

    fn check(
        &self,
        headers: &HeaderMap,
        action: AuthzAction,
        resource: &AuthzResource,
    ) -> Result<GetUserIdentityResponse, AuthError> {
        let user = self.authenticate(headers)?;
        self.authorize(user, action, resource)?;
        Ok(Self::identity(user))
    }
}

impl AuthenticateAndAuthorize for TokenAuthentication {
    fn authenticate_and_authorize(
        &self,
        headers: &HeaderMap,
        action: AuthzAction,
        resource: AuthzResource,
    ) -> Pin<Box<dyn Future<Output = Result<GetUserIdentityResponse, AuthError>> + Send>> {
        Box::pin(ready(self.check(headers, action, &resource)))
    }

    fn authenticate_and_authorize_collection(
        &self,
        headers: &HeaderMap,
        action: AuthzAction,
        resource: AuthzResource,
        _collection: Collection,
    ) -> Pin<Box<dyn Future<Output = Result<GetUserIdentityResponse, AuthError>> + Send>> {
        Box::pin(ready(self.check(headers, action, &resource)))
    }

    fn get_user_identity(
        &self,
        headers: &HeaderMap,
    ) -> Pin<Box<dyn Future<Output = Result<GetUserIdentityResponse, AuthError>> + Send>> {
        Box::pin(ready(self.authenticate(headers).map(Self::identity)))
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use axum::http::{HeaderMap, HeaderValue, StatusCode};

    use super::{TokenAuthentication, TokenAuthenticationError};
    use crate::auth::{AuthzAction, AuthzResource};
    use crate::config::FrontendServerConfig;

    const CREDENTIALS: &str = r#"
users:
  - id: admin
    tokens:
      - admin-token
  - id: team_a
    role: writer
    tenant: tenant_a
    tokens:
      - token-a
  - id: team_a_db
    tenant: tenant_a
    databases:
      - db_a
    tokens:
      - token-a-db
"#;

    fn auth_with(credentials: &str, header: Option<&str>) -> TokenAuthentication {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(credentials.as_bytes()).unwrap();
        let mut config = FrontendServerConfig::single_node_default();
        config.server_authn_credentials_file = Some(file.path().to_str().unwrap().to_string());
        config.auth_token_transport_header = header.map(str::to_string);
        TokenAuthentication::from_config(&config).unwrap().unwrap()
    }

    fn token(key: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("x-chroma-token", HeaderValue::from_str(key).unwrap());
        headers
    }

    fn resource(tenant: &str, database: Option<&str>) -> AuthzResource {
        AuthzResource {
            tenant: Some(tenant.to_string()),
            database: database.map(str::to_string),
            collection: None,
        }
    }

    fn status(
        auth: &TokenAuthentication,
        headers: &HeaderMap,
        action: AuthzAction,
        resource: AuthzResource,
    ) -> StatusCode {
        match auth.check(headers, action, &resource) {
            Ok(_) => StatusCode::OK,
            Err(err) => err.0,
        }
    }

    // The answers measured on Chroma Cloud: 401 without a valid key, 403 outside the tenant and the databases of the key.
    #[test]
    fn keys_answer_as_chroma_cloud() {
        let auth = auth_with(CREDENTIALS, None);
        let list = AuthzAction::ListCollections;
        assert_eq!(status(&auth, &HeaderMap::new(), list, resource("tenant_a", Some("db_a"))), StatusCode::UNAUTHORIZED);
        assert_eq!(status(&auth, &token("wrong"), list, resource("tenant_a", Some("db_a"))), StatusCode::UNAUTHORIZED);
        // The tenant key: every database of its tenant, no other tenant.
        let a = token("token-a");
        assert_eq!(status(&auth, &a, list, resource("tenant_a", Some("db_a"))), StatusCode::OK);
        assert_eq!(status(&auth, &a, list, resource("tenant_a", Some("other"))), StatusCode::OK);
        assert_eq!(status(&auth, &a, AuthzAction::CreateDatabase, resource("tenant_a", Some("new"))), StatusCode::OK);
        assert_eq!(status(&auth, &a, list, resource("tenant_b", Some("db_b"))), StatusCode::FORBIDDEN);
        assert_eq!(status(&auth, &a, AuthzAction::GetTenant, resource("default_tenant", None)), StatusCode::FORBIDDEN);
        // The key of one database: that database, the list of the databases, nothing else.
        let db = token("token-a-db");
        assert_eq!(status(&auth, &db, list, resource("tenant_a", Some("db_a"))), StatusCode::OK);
        assert_eq!(status(&auth, &db, AuthzAction::ListDatabases, resource("tenant_a", None)), StatusCode::OK);
        assert_eq!(status(&auth, &db, list, resource("tenant_a", Some("other"))), StatusCode::FORBIDDEN);
        assert_eq!(status(&auth, &db, AuthzAction::CreateDatabase, resource("tenant_a", Some("new"))), StatusCode::FORBIDDEN);
        // The key of every tenant.
        let admin = token("admin-token");
        assert_eq!(status(&auth, &admin, list, resource("tenant_b", Some("db_b"))), StatusCode::OK);
        assert_eq!(status(&auth, &admin, AuthzAction::CreateTenant, resource("new", None)), StatusCode::OK);
        assert_eq!(status(&auth, &a, AuthzAction::CreateTenant, resource("new", None)), StatusCode::FORBIDDEN);
    }

    #[test]
    fn identity_is_the_identity_of_the_key() {
        let auth = auth_with(CREDENTIALS, None);
        let identity = TokenAuthentication::identity(auth.authenticate(&token("token-a-db")).unwrap());
        assert_eq!(identity.user_id, "team_a_db");
        assert_eq!(identity.tenant, "tenant_a");
        assert_eq!(identity.databases, ["db_a".to_string()].into_iter().collect());
        // As Chroma Cloud, a key of every database has an empty list of databases.
        let identity = TokenAuthentication::identity(auth.authenticate(&token("token-a")).unwrap());
        assert!(identity.databases.is_empty());
    }

    // The header of Chroma Cloud by default; `Authorization: Bearer` with the setting of Chroma 0.5, and then only that.
    #[test]
    fn the_key_travels_in_the_configured_header() {
        let mut bearer = HeaderMap::new();
        bearer.insert("authorization", HeaderValue::from_static("Bearer token-a"));
        let default = auth_with(CREDENTIALS, None);
        assert!(default.authenticate(&token("token-a")).is_ok());
        assert!(default.authenticate(&bearer).is_err());
        let authorization = auth_with(CREDENTIALS, Some("Authorization"));
        assert!(authorization.authenticate(&bearer).is_ok());
        assert!(authorization.authenticate(&token("token-a")).is_err());
        let mut no_bearer = HeaderMap::new();
        no_bearer.insert("authorization", HeaderValue::from_static("token-a"));
        assert!(authorization.authenticate(&no_bearer).is_err());
    }

    // The checks of Chroma 0.5 on the file.
    #[test]
    fn invalid_credentials_are_rejected_at_startup() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(b"users:\n  - id: a\n    tokens: [same]\n  - id: b\n    tokens: [same]\n").unwrap();
        let mut config = FrontendServerConfig::single_node_default();
        config.server_authn_credentials_file = Some(file.path().to_str().unwrap().to_string());
        assert!(matches!(
            TokenAuthentication::from_config(&config),
            Err(TokenAuthenticationError::TokenInUse(_, _))
        ));
        config.server_authn_credentials = Some("key".to_string());
        assert!(matches!(
            TokenAuthentication::from_config(&config),
            Err(TokenAuthenticationError::Both)
        ));
        config.server_authn_credentials_file = None;
        config.auth_token_transport_header = Some("Cookie".to_string());
        assert!(matches!(
            TokenAuthentication::from_config(&config),
            Err(TokenAuthenticationError::InvalidHeader(_))
        ));
        config.auth_token_transport_header = None;
        let single = TokenAuthentication::from_config(&config).unwrap().unwrap();
        assert_eq!(single.authenticate(&token("key")).unwrap().id, "anonymous");
        config.server_authn_credentials = None;
        assert!(TokenAuthentication::from_config(&config).unwrap().is_none());
    }
}
