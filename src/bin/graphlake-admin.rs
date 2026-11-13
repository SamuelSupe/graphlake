use std::path::PathBuf;

use anyhow::Context;
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(version, about = "Run the GraphLake local administration UI")]
struct Args {
    #[arg(long)]
    catalog_dir: PathBuf,
    #[arg(long)]
    assets_dir: Option<PathBuf>,
    #[arg(long, default_value_t = 7337)]
    port: u16,
    #[arg(long, default_value_t = 60_000, value_parser = clap::value_parser!(u64).range(1..=60_000))]
    timeout_ms: u64,
    #[arg(long)]
    insecure_dev: bool,
    #[arg(long, default_value_t = 1, value_parser = clap::value_parser!(u16).range(1..=8))]
    query_slots: u16,
    /// Published model ID exposed to the read-only driver endpoints.
    #[arg(long)]
    driver_model: Option<String>,
    /// Environment variable containing the independent driver token.
    #[arg(long, default_value = "GRAPHLAKE_DRIVER_TOKEN")]
    driver_token_env: String,
    /// Enable a loopback Bolt listener.
    #[arg(long, requires = "driver_model")]
    bolt_port: Option<u16>,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Manage the local administrator password in an interactive terminal.
    Auth {
        #[command(subcommand)]
        action: AuthAction,
    },
}

#[derive(Subcommand)]
enum AuthAction {
    /// Create administrator credentials for this catalog.
    Init,
    /// Replace administrator credentials after stopping the server.
    ResetPassword,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    if let Some(Command::Auth { action }) = args.command {
        let catalog = args
            .catalog_dir
            .canonicalize()
            .context("opening catalog directory")?;
        return graphlake::admin::setup_auth(&catalog, matches!(action, AuthAction::ResetPassword));
    }
    let assets_dir = args
        .assets_dir
        .context("--assets-dir is required to serve")?;
    graphlake::admin::serve_configured(
        args.catalog_dir,
        assets_dir,
        graphlake::admin::ServiceOptions {
            port: args.port,
            timeout_ms: args.timeout_ms,
            insecure_dev: args.insecure_dev,
            query_slots: usize::from(args.query_slots),
            bolt_port: args.bolt_port,
            driver_model: args.driver_model,
            driver_token_env: args.driver_token_env,
        },
    )
    .await
}
