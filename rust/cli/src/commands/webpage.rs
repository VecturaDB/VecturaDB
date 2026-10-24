// Changed by VecturaDB contributors for the VecturaDB branding.
// Copyright VecturaDB contributors. Licensed under the Apache License, Version 2.0.
use crate::utils::CliError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum WebPageError {
    #[error("Failed to open browser. {0}")]
    BrowserOpenFailed(String),
}

pub enum WebPageCommand {
    Docs,
}

impl WebPageCommand {
    fn url(&self) -> &'static str {
        match self {
            WebPageCommand::Docs => "https://github.com/VecturaDB/VecturaDB",
        }
    }
}

pub fn open_browser(command: WebPageCommand) -> Result<(), CliError> {
    let url = command.url();
    let error_message = format!("Visit {}", url);
    webbrowser::open(url).map_err(|_| WebPageError::BrowserOpenFailed(error_message))?;
    Ok(())
}
