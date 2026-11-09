// Changed by VecturaDB contributors for the VecturaDB branding.
// Copyright VecturaDB contributors. Licensed under the Apache License, Version 2.0.
use crate::terminal::{SystemTerminal, Terminal};
use crate::ui_utils::{HOLIDAY_LOGO, LOGO};
use crate::utils::CliError;
use chroma_frontend::auth::AuthenticateAndAuthorize;
use chroma_frontend::config::FrontendServerConfig;
use chroma_frontend::executor::config::ExecutorConfig;
use chroma_frontend::frontend_service_entrypoint_with_config;
use chroma_frontend::token_auth::TokenAuthentication;
use chroma_types::ServerInfo;
use chrono::{Datelike, Local};
use clap::Parser;
use colored::Colorize;
use std::net::TcpListener;
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RunError {
    #[error("Config file {0} does not exist")]
    ConfigFileNotFound(String),
    #[error("Address {0}:{1} is not available")]
    AddressUnavailable(String, u16),
    #[error("Failed to start a VecturaDB server")]
    ServerStartFailed,
}

#[derive(Parser, Debug)]
pub struct RunArgs {
    #[clap(
        index = 1,
        conflicts_with_all = &["path", "host", "port"],
        help = "The path to the VecturaDB config file"
    )]
    config_path: Option<String>,

    #[clap(
        long,
        conflicts_with = "config_path",
        help = "The persistence path to your VecturaDB database"
    )]
    path: Option<String>,

    #[clap(
        long,
        default_value = "localhost",
        conflicts_with = "config_path",
        help = "The host to listen to. Default: localhost"
    )]
    host: Option<String>,

    #[clap(
        long,
        conflicts_with = "config_path",
        help = "The port to run the server on"
    )]
    port: Option<u16>,
}

fn validate_host(address: &String, port: u16) -> bool {
    let socket = format!("{}:{}", address, port);
    TcpListener::bind(&socket).is_ok()
}

fn override_default_config_with_args(args: RunArgs) -> Result<FrontendServerConfig, CliError> {
    let mut config = FrontendServerConfig::single_node_default();

    if let Some(path) = args.path {
        config.persist_path = path;
    }

    if let Some(port) = args.port {
        config.port = port;
    }

    if let Some(host) = args.host {
        config.listen_address = host;
    }

    // As `chroma run` in Chroma 0.5, the API keys come from the environment.
    if let Ok(path) = std::env::var("CHROMA_SERVER_AUTHN_CREDENTIALS_FILE") {
        config.server_authn_credentials_file = Some(path);
    }
    if let Ok(key) = std::env::var("CHROMA_SERVER_AUTHN_CREDENTIALS") {
        config.server_authn_credentials = Some(key);
    }
    if let Ok(header) = std::env::var("CHROMA_AUTH_TOKEN_TRANSPORT_HEADER") {
        config.auth_token_transport_header = Some(header);
    }

    if !validate_host(&config.listen_address, config.port) {
        return Err(RunError::AddressUnavailable(config.listen_address, config.port).into());
    }

    Ok(config)
}

/// The `server` block of `/api/v2/pre-flight-checks`: what VecturaDB has beyond the single node of Chroma. The
/// release sets the version of this crate to the tag's (.github/workflows/ci.yml, "CLI version from the release tag").
/// spann when the server gives SPANN to the collections that ask for it: its local executor has the option on, and the
/// server always has a data path. conditional_transactions when the server takes them: enable_transactions on.
pub(crate) fn server_info(config: &FrontendServerConfig) -> ServerInfo {
    let mut features: Vec<String> = [
        "sparse_vector_index",
        "bm25",
        "search",
        "fork",
        "api_keys",
        "indexing_status",
        "attached_functions",
    ]
    .map(String::from)
    .to_vec();
    if matches!(&config.frontend.executor, ExecutorConfig::Local(local) if local.spann) {
        features.push("spann".to_string());
    }
    if config.frontend.enable_transactions {
        features.push("conditional_transactions".to_string());
    }
    ServerInfo {
        name: "VecturaDB".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        features,
    }
}

fn display_run_message(config: &FrontendServerConfig, term: &mut dyn Terminal) {
    let now = Local::now();

    if now.month() == 12 {
        term.println(HOLIDAY_LOGO);
    } else {
        term.println(LOGO);
    }

    term.println(&format!("Saving data to: {}", config.persist_path.bold()));
    term.println(&format!(
        "Connect to VecturaDB at: {}",
        format!("http://localhost:{}", config.port)
            .underline()
            .blue()
    ));
    term.println(&format!(
        "Getting started guide: {}",
        "https://vecturadb.org\n".underline().blue()
    ));
}

pub fn run(args: RunArgs) -> Result<(), CliError> {
    let mut term = SystemTerminal;

    let mut config = match &args.config_path {
        Some(config_path) => {
            if !std::path::Path::new(config_path).exists() {
                eprintln!(
                    "Could not find {config_path:?} in {:?}",
                    std::env::current_dir()
                        .map(
                            |p| String::from_utf8_lossy(p.as_os_str().as_encoded_bytes())
                                .to_string()
                        )
                        .unwrap_or("<unknown>".to_string())
                );
                return Err(RunError::ConfigFileNotFound(config_path.to_string()).into());
            }
            FrontendServerConfig::load_from_path(config_path)
        }
        None => {
            let mut config = override_default_config_with_args(args)?;
            config.stdout_tracing = true;
            config
        }
    };

    config.server_info = Some(server_info(&config));

    display_run_message(&config, &mut term);

    let auth: Arc<dyn AuthenticateAndAuthorize> = match TokenAuthentication::from_config(&config) {
        Ok(Some(keys)) => Arc::new(keys),
        Ok(None) => Arc::new(()),
        Err(err) => {
            eprintln!("Could not load the API keys: {err}");
            return Err(RunError::ServerStartFailed.into());
        }
    };

    let runtime = tokio::runtime::Runtime::new().map_err(|_| RunError::ServerStartFailed)?;
    runtime.block_on(async {
        frontend_service_entrypoint_with_config(auth, Arc::new(()), &config, true).await;
    });
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::terminal::test_terminal::TestTerminal;

    #[test]
    fn test_validate_host_available() {
        let result = validate_host(&"127.0.0.1".to_string(), 0);
        assert!(result);
    }

    #[test]
    fn test_validate_host_unavailable() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        assert!(!validate_host(&"127.0.0.1".to_string(), port));
    }

    // The pre-flight checks declare spann when the server gives SPANN to the collections that ask for it, which it does
    // by default, and not when the option is off.
    #[test]
    fn the_server_declares_spann_when_it_has_it() {
        let mut config = FrontendServerConfig::single_node_default();
        assert!(server_info(&config).features.contains(&"spann".to_string()));
        if let ExecutorConfig::Local(local) = &mut config.frontend.executor {
            local.spann = false;
        }
        assert!(!server_info(&config).features.contains(&"spann".to_string()));
    }

    // The pre-flight checks declare the attached functions always, and the conditional transactions while
    // enable_transactions is on, as by default.
    #[test]
    fn the_server_declares_its_transactions_and_attached_functions() {
        let mut config = FrontendServerConfig::single_node_default();
        let features = server_info(&config).features;
        assert!(features.contains(&"attached_functions".to_string()));
        assert!(features.contains(&"conditional_transactions".to_string()));
        config.frontend.enable_transactions = false;
        let features = server_info(&config).features;
        assert!(features.contains(&"attached_functions".to_string()));
        assert!(!features.contains(&"conditional_transactions".to_string()));
    }

    #[test]
    fn test_display_run_message() {
        let config = FrontendServerConfig::single_node_default();
        let mut term = TestTerminal::new();
        display_run_message(&config, &mut term);

        let output = term.output.join("\n");
        assert!(output.contains("Connect to VecturaDB at"));
        assert!(output.contains("Saving data to"));
    }
}
