// Changed by VecturaDB contributors for the VecturaDB branding.
// Copyright VecturaDB contributors. Licensed under the Apache License, Version 2.0.
mod client;
mod commands;
mod config_store;
pub mod hnsw_config_repair;
pub mod hnsw_integrity_check;
mod terminal;
mod tui;
mod ui_utils;
mod utils;

use crate::commands::browse::{browse, BrowseArgs};
use crate::commands::copy::{copy, CopyArgs};
use crate::commands::login::{login, LoginArgs};
use crate::commands::profile::{profile_command, ProfileCommand};
use crate::commands::run::{run, RunArgs};
use crate::commands::update::update;
use crate::commands::vacuum::{vacuum, VacuumArgs};
use crate::commands::webpage::{open_browser, WebPageCommand};
use crate::hnsw_integrity_check::{hnsw_integrity_check, HnswIntegrityCheckArgs};
use clap::{Parser, Subcommand};
use colored::Colorize;

#[derive(Subcommand, Debug)]
enum Command {
    #[command(about = "Browse collections, local or on Chroma Cloud", long_about = None)]
    Browse(BrowseArgs),
    #[command(about = "Copy collections between Chroma Cloud and a local VecturaDB", long_about = None)]
    Copy(CopyArgs),
    #[command(about = "Open the VecturaDB documentation", long_about = None)]
    Docs,
    #[command(
        name = "hnsw-integrity-check",
        about = "Check local HNSW startup fast-forward and integrity hazards",
        long_about = None
    )]
    HnswIntegrityCheck(HnswIntegrityCheckArgs),
    #[command(about = "Repair invalid local HNSW construction settings in a new copy")]
    HnswConfigRepair(hnsw_config_repair::HnswConfigRepairArgs),
    #[command(about = "Log in to Chroma Cloud, to copy data from it", long_about = None)]
    Login(LoginArgs),
    #[command(about = "Manage Chroma Cloud profiles, to copy data from it", long_about = None)]
    #[command(subcommand)]
    Profile(ProfileCommand),
    #[command(about = "Start a local VecturaDB server", long_about = None)]
    Run(RunArgs),
    #[command(about = "Check for VecturaDB CLI updates", long_about = None)]
    Update,
    #[command(about = "Vacuum a local VecturaDB persistent directory", long_about = None)]
    Vacuum(VacuumArgs),
}

#[derive(Parser, Debug)]
#[command(name = "vecturadb")]
#[command(version)]
#[command(about = "VecturaDB, compatible with the Chroma API", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

pub fn chroma_cli(args: Vec<String>) {
    let cli = Cli::parse_from(args);

    println!();

    let result = match cli.command {
        Command::Browse(args) => browse(args),
        Command::Copy(args) => copy(args),
        Command::Docs => open_browser(WebPageCommand::Docs),
        Command::HnswIntegrityCheck(args) => hnsw_integrity_check(args),
        Command::HnswConfigRepair(args) => hnsw_config_repair::run(args),
        Command::Login(args) => login(args),
        Command::Profile(profile_subcommand) => profile_command(profile_subcommand),
        Command::Run(args) => run(args),
        Command::Update => update(),
        Command::Vacuum(args) => vacuum(args),
    };

    if result.is_err() {
        let error_message = result.err().unwrap().to_string();
        eprintln!("{}", error_message.red());
        println!();
        std::process::exit(1);
    }

    println!();
}
