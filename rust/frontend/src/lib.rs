// Changed from the original Chroma file; see the git history for the changes and their authors.
#![recursion_limit = "256"]
use std::{path::Path, sync::Arc};

pub mod config;
#[allow(dead_code)]
pub mod executor;
pub mod folder_lock;
pub mod get_collection_with_segments_provider;
pub mod impls;
pub mod quota;
pub mod server;
pub mod token_auth;
mod types;

// Re-export scaffolding from frontend-core so that internal modules can keep
// referring to `crate::ac::*`, `crate::auth::*`, `crate::traced_json::*`, and
// `crate::server_middleware::*`. External consumers also benefit from the
// re-exports for `ac`, `auth`, and `traced_json`.
pub use frontend_core::middleware as server_middleware;
pub use frontend_core::{ac, auth, traced_json};

use chroma_config::{registry::Registry, Configurable};
use chroma_error::ChromaError;
use chroma_sqlite::db::SqliteDb;
use chroma_system::System;
use config::FrontendServerConfig;
use get_collection_with_segments_provider::*;
use mdac::{Pattern, Rule};
use quota::QuotaEnforcer;
use server::FrontendServer;

pub use config::{FrontendConfig, ScorecardRule};
pub use impls::Frontend;

pub const CONFIG_PATH_ENV_VAR: &str = "CONFIG_PATH";

#[derive(thiserror::Error, Debug)]
pub enum ScorecardRuleError {
    #[error("Invalid pattern: {0}")]
    InvalidPattern(String),
}

impl ChromaError for ScorecardRuleError {
    fn code(&self) -> chroma_error::ErrorCodes {
        match self {
            ScorecardRuleError::InvalidPattern(_) => chroma_error::ErrorCodes::InvalidArgument,
        }
    }
}

pub async fn frontend_service_entrypoint(
    auth: Arc<dyn auth::AuthenticateAndAuthorize>,
    quota_enforcer: Arc<dyn QuotaEnforcer>,
    init_otel_tracing: bool,
) {
    let config = match std::env::var(CONFIG_PATH_ENV_VAR) {
        Ok(config_path) => FrontendServerConfig::load_from_path(&config_path),
        Err(_) => FrontendServerConfig::load(),
    };
    frontend_service_entrypoint_with_config(auth, quota_enforcer, &config, init_otel_tracing).await;
}

// The lock of the data folder of a server of the single node, or the end of the process with its message: a server on
// a folder that another program holds, a server or an embedded client, waits folder_lock::FOLDER_WAIT, then stops
// here, before it opens the database.
async fn hold_folder_for_server(folder: std::path::PathBuf) -> folder_lock::FolderLock {
    let held = tokio::task::spawn_blocking(move || {
        folder_lock::lock_folder(&folder, folder_lock::FOLDER_WAIT)
    })
    .await
    .expect("the wait for the lock of the data folder");
    match held {
        Ok(lock) => lock,
        Err(err) => {
            tracing::error!(error = %err, "The data folder is in use");
            eprintln!("{err}");
            std::process::exit(1);
        }
    }
}

/// The configuration of the frontend from the one of the server: on a single node, the folder of the data, the SQLite
/// file in it and the shortest time between two saves of a local HNSW index, where the frontend block gives none.
pub fn local_frontend_config(config: &FrontendServerConfig) -> FrontendConfig {
    let mut fe_cfg = config.frontend.clone();
    if let (Some(sql_cfg), Some(local_segman_cfg)) =
        (fe_cfg.sqlitedb.as_mut(), fe_cfg.segment_manager.as_mut())
    {
        let persist_root = Path::new(&config.persist_path);
        let sqlite_url = persist_root.join(&config.sqlite_filename);
        local_segman_cfg.persist_path.get_or_insert(
            persist_root
                .to_str()
                .expect("Persist path should be valid")
                .to_string(),
        );
        if let Some(seconds) = config.hnsw_min_save_interval_seconds {
            local_segman_cfg
                .hnsw_min_save_interval_seconds
                .get_or_insert(seconds);
        }
        if let Some(mib) = config.spann_block_cache_mib {
            local_segman_cfg.spann_block_cache_mib.get_or_insert(mib);
        }
        sql_cfg.url.get_or_insert(
            sqlite_url
                .to_str()
                .expect("Sqlite path should be valid")
                .to_string(),
        );
    }
    fe_cfg
}

pub async fn frontend_service_entrypoint_with_config_system_registry(
    auth: Arc<dyn auth::AuthenticateAndAuthorize>,
    quota_enforcer: Arc<dyn QuotaEnforcer>,
    system: System,
    registry: Registry,
    config: &FrontendServerConfig,
) {
    let fe_cfg = local_frontend_config(config);
    // The lock of the data folder, held until the server has stopped and closed its database (folder_lock).
    let mut _held_folder = None;
    if fe_cfg.sqlitedb.is_some() {
        if let Some(folder) = fe_cfg
            .segment_manager
            .as_ref()
            .and_then(|manager| manager.persist_path.as_ref())
        {
            _held_folder = Some(hold_folder_for_server(folder.into()).await);
        }
    }

    let frontend = Frontend::try_from_config(&(fe_cfg, system.clone()), &registry)
        .await
        .expect("Error creating Frontend Config");
    fn rule_to_rule(rule: &ScorecardRule) -> Result<Rule, ScorecardRuleError> {
        let patterns = rule
            .patterns
            .iter()
            .map(|p| Pattern::new(p).ok_or_else(|| ScorecardRuleError::InvalidPattern(p.clone())))
            .collect::<Result<Vec<_>, ScorecardRuleError>>()?;
        Ok(Rule {
            patterns,
            limit: rule.score as usize,
        })
    }
    let rules = config
        .scorecard
        .iter()
        .map(rule_to_rule)
        .collect::<Result<Vec<_>, ScorecardRuleError>>()
        .expect("error creating scorecard");
    FrontendServer::new(
        config.clone(),
        frontend,
        rules,
        auth,
        quota_enforcer,
        system,
    )
    .run(None)
    .await;
    // A normal stop closes the database: in WAL its log goes back into the database file, which is then the only one
    // left, as with a rollback journal. A graceful stop: the work of the compaction manager out of its actor ends within
    // the grace, then the database closes within what is left of it, and anyway after it (SqliteDb::close_within).
    if let Ok(db) = registry.get::<SqliteDb>() {
        let started = std::time::Instant::now();
        let grace = match registry.get::<chroma_log::LocalCompactionManager>() {
            Ok(compactor) => {
                let grace = compactor.shutdown_grace();
                if !compactor.drain(grace).await {
                    tracing::warn!(
                        "Work of the compaction manager still running {} s after the stop",
                        grace.as_secs()
                    );
                }
                grace
            }
            Err(_) => std::time::Duration::from_secs(30),
        };
        db.close_within(grace.saturating_sub(started.elapsed()))
            .await;
    }
}

pub fn init_frontend_otel_tracing(config: &FrontendServerConfig) {
    frontend_core::tracing::init_server_otel_tracing(
        config.open_telemetry.as_ref(),
        config.stdout_tracing,
    );
}

pub async fn frontend_service_entrypoint_with_config(
    auth: Arc<dyn auth::AuthenticateAndAuthorize>,
    quota_enforcer: Arc<dyn QuotaEnforcer>,
    config: &FrontendServerConfig,
    init_otel_tracing: bool,
) {
    let system = System::new();
    let registry = Registry::new();

    if init_otel_tracing {
        init_frontend_otel_tracing(config);
    }

    frontend_service_entrypoint_with_config_system_registry(
        auth,
        quota_enforcer,
        system,
        registry,
        config,
    )
    .await;
}
