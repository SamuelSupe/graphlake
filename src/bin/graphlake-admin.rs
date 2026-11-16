use std::path::PathBuf;

use anyhow::Context;
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(version, long_version = option_env!("GRAPHLAKE_BUILD_VERSION"), about = "Run the GraphLake local administration UI")]
struct Args {
    #[arg(long)]
    catalog_dir: Option<PathBuf>,
    #[arg(long)]
    assets_dir: Option<PathBuf>,
    #[arg(long)]
    cluster_config: Option<PathBuf>,
    /// Optional YAML for operational logging and OTLP/HTTP export.
    #[arg(long, value_name = "FILE")]
    telemetry_config: Option<PathBuf>,
    #[arg(long, default_value_t = 7337)]
    port: u16,
    /// Expose process role on a separate loopback port, including while waiting for leadership.
    #[arg(long, value_parser = clap::value_parser!(u16).range(1..))]
    status_port: Option<u16>,
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
    /// Canonical HTTPS origin accepted from the reverse proxy.
    #[arg(long)]
    public_url: Option<String>,
    /// Use etcd leadership and publish Coordinator state to shared object storage.
    #[arg(long, requires_all = ["cluster_config", "public_url"], conflicts_with = "insecure_dev")]
    coordinator_failover: bool,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Run a cluster worker on a private loopback endpoint.
    Worker {
        #[arg(long)]
        source_dir: PathBuf,
    },
    /// Check or renew ownership of an ingress VIP.
    Ingress {
        #[arg(long)]
        node: String,
    },
    /// Export or restore an HA control-plane backup.
    Backup {
        #[command(subcommand)]
        action: BackupAction,
    },
    /// Manage the local administrator password in an interactive terminal.
    Auth {
        #[command(subcommand)]
        action: AuthAction,
    },
}

#[derive(Subcommand)]
enum BackupAction {
    /// Export control state and local tables from a running or stopped cluster.
    Export {
        #[arg(long)]
        file: PathBuf,
        #[arg(long)]
        status_file: Option<PathBuf>,
    },
    /// Restore into an unused cluster name with all Coordinators stopped.
    Restore {
        #[arg(long)]
        file: PathBuf,
        /// Directory containing manifest.json and the recovery artifacts.
        #[arg(long)]
        recovery_dir: PathBuf,
    },
    /// Hash complete PostgreSQL relations in one read-only snapshot.
    FingerprintPostgres {
        #[arg(long)]
        dsn_env: String,
        #[arg(long, required = true)]
        relation: Vec<String>,
    },
    /// Verify a copied backup offline before a disaster recovery restore.
    Verify {
        #[arg(long)]
        file: PathBuf,
        #[arg(long)]
        status_file: Option<PathBuf>,
    },
}

#[derive(Subcommand)]
enum AuthAction {
    /// Create administrator credentials for this catalog.
    Init,
    /// Replace administrator credentials after stopping the server.
    ResetPassword,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let role = match &args.command {
        None if args.cluster_config.is_some() => Some("coordinator"),
        None => Some("standalone"),
        Some(Command::Worker { .. }) => Some("worker"),
        Some(Command::Backup {
            action: BackupAction::Export { .. } | BackupAction::Restore { .. },
        }) => Some("backup"),
        _ => None,
    };
    if let Some(role) = role {
        let explicit = args.telemetry_config.clone().or_else(|| {
            std::env::var_os("GRAPHLAKE_TELEMETRY_CONFIG")
                .filter(|s| !s.is_empty())
                .map(PathBuf::from)
        });
        let default = match &args.command {
            None if !args.coordinator_failover => args.catalog_dir.as_deref(),
            _ => None,
        }
        .map(|catalog| catalog.join(graphlake::admin::DEFAULT_TELEMETRY_FILE));
        graphlake::admin::init_telemetry(explicit.as_deref(), default.as_deref(), role)?;
    }
    let failover = args.coordinator_failover;
    let mut runtime = if matches!(args.command, Some(Command::Ingress { .. })) {
        tokio::runtime::Builder::new_current_thread()
    } else {
        let mut runtime = tokio::runtime::Builder::new_multi_thread();
        // Keep the alternate planner stack as a fallback for unusually deep plans.
        runtime.thread_stack_size(8 * 1024 * 1024);
        runtime
    };
    let runtime = runtime.enable_all().build()?;
    let result = runtime.block_on(run(args));
    if failover {
        // A syscall on an unavailable hard-mounted source can outlast shutdown.
        // Leadership and all listeners have already been relinquished by run().
        runtime.shutdown_timeout(std::time::Duration::from_secs(1));
    }
    graphlake::admin::report_telemetry_exit(result.is_ok());
    graphlake::admin::shutdown_telemetry();
    result
}

async fn run(args: Args) -> anyhow::Result<()> {
    if let Some(Command::Worker { source_dir }) = args.command {
        return graphlake::admin::serve_worker(
            args.cluster_config
                .context("worker requires --cluster-config")?,
            source_dir,
            args.port,
            usize::from(args.query_slots),
        )
        .await;
    }
    if let Some(Command::Ingress { node }) = args.command {
        let config = args
            .cluster_config
            .context("ingress requires --cluster-config")?;
        return graphlake::admin::check_ingress(&config, &node).await;
    }
    if let Some(Command::Backup { action }) = args.command {
        let (result, status_file) = match action {
            BackupAction::Export { file, status_file } => {
                let result = async {
                    let config = args
                        .cluster_config
                        .context("backup requires --cluster-config")?;
                    let catalog = args.catalog_dir.or_else(|| {
                        std::env::var_os("GRAPHLAKE_BACKUP_CATALOG_DIR").map(PathBuf::from)
                    });
                    graphlake::admin::cluster_backup_with_catalog(
                        &config,
                        &file,
                        None,
                        catalog.as_deref(),
                    )
                    .await
                }
                .await;
                (result, status_file)
            }
            BackupAction::FingerprintPostgres { dsn_env, relation } => {
                let hashes =
                    graphlake::admin::recovery_fingerprint_postgres(&dsn_env, &relation).await?;
                println!("{}", serde_json::to_string_pretty(&hashes)?);
                return Ok(());
            }
            BackupAction::Restore { file, recovery_dir } => {
                let config = args
                    .cluster_config
                    .context("backup requires --cluster-config")?;
                return graphlake::admin::cluster_backup_with_catalog(
                    &config,
                    &file,
                    Some(&recovery_dir),
                    args.catalog_dir.as_deref(),
                )
                .await
                .map(|_| ());
            }
            BackupAction::Verify { file, status_file } => (
                graphlake::admin::verify_backup_checkpoint(&file),
                status_file,
            ),
        };
        return match status_file {
            Some(path) => graphlake::admin::record_backup_status(&path, result),
            None => result.map(|_| ()),
        };
    }
    let catalog_dir = args.catalog_dir.context("--catalog-dir is required")?;
    if let Some(Command::Auth { action }) = args.command {
        let catalog = catalog_dir
            .canonicalize()
            .context("opening catalog directory")?;
        return graphlake::admin::setup_auth(&catalog, matches!(action, AuthAction::ResetPassword));
    }
    let assets_dir = args
        .assets_dir
        .context("--assets-dir is required to serve")?;
    graphlake::admin::serve_clustered_with_status(
        catalog_dir,
        assets_dir,
        graphlake::admin::ServiceOptions {
            port: args.port,
            timeout_ms: args.timeout_ms,
            insecure_dev: args.insecure_dev,
            query_slots: usize::from(args.query_slots),
            bolt_port: args.bolt_port,
            driver_model: args.driver_model,
            driver_token_env: args.driver_token_env,
            public_url: args.public_url,
            coordinator_failover: args.coordinator_failover,
        },
        args.cluster_config,
        args.status_port,
    )
    .await
}
