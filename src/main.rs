use std::{
    io::{self, BufWriter},
    path::PathBuf,
    time::Duration,
};

use anyhow::{Result, anyhow, ensure};
use clap::{Args, Parser, Subcommand, ValueEnum};
use futures::TryStreamExt;
use graphlake::{
    Catalog, CommunityOptions, Direction, Engine, EngineLimits, GraphAlgorithm, GraphProjection,
    Language, Node2VecOptions, ProjectionAlgorithm, QueryOptions, QueryResult, Route,
    RouteStrategy, ScalarValue, Traversal, TraversalKind, TraversalStrategy, VertexKey, arrow,
};

#[derive(Parser)]
#[command(version, about)]
struct Cli {
    #[arg(long)]
    catalog: PathBuf,
    #[arg(long, requires = "cache_scope")]
    cache_dir: Option<PathBuf>,
    #[arg(long, requires = "cache_dir")]
    cache_scope: Option<String>,
    #[arg(long, default_value_t = 60_000)]
    timeout_ms: u64,
    #[arg(long, default_value_t = 256)]
    memory_mib: usize,
    #[arg(long, default_value_t = 1024)]
    spill_mib: u64,
    #[arg(long, default_value_t = 2)]
    partitions: usize,
    #[arg(long, default_value_t = 1024)]
    batch_size: usize,
    #[arg(long, value_enum, default_value_t = Format::Jsonl)]
    format: Format,
    #[arg(long)]
    diagnostics: bool,
    /// JSON list of graph-wide predicates, each with column, op and value.
    #[arg(long)]
    logical_partition: Option<String>,
    /// Query SCD2 data at a date, timestamp, or 'current'.
    #[arg(long)]
    snapshot_time: Option<String>,
    /// Require every table in the query to share one verifiable source snapshot.
    #[arg(long)]
    require_consistent_snapshot: bool,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run a read-only SQL, Cypher or Gremlin query.
    Query {
        #[arg(long, value_enum, default_value_t = Dialect::Cypher)]
        language: Dialect,
        /// JSON object of Cypher parameters.
        #[arg(long)]
        parameters: Option<String>,
        query: String,
    },
    /// Analyze selected node labels and edge types.
    Analyze {
        #[command(flatten)]
        graph: ProjectionFlags,
        #[arg(long, value_enum, default_value_t = Strategy::Auto)]
        strategy: Strategy,
        #[command(subcommand)]
        algorithm: Algorithm,
    },
    /// Find one or more weighted, loopless shortest paths.
    Route {
        #[command(flatten)]
        graph: ProjectionFlags,
        #[arg(long)]
        start_label: String,
        #[arg(long, required = true, allow_hyphen_values = true)]
        start: Vec<String>,
        #[arg(long)]
        target_label: String,
        #[arg(long, required = true, allow_hyphen_values = true)]
        target: Vec<String>,
        #[arg(long, default_value_t = 1)]
        k: usize,
        #[arg(long, default_value = "auto")]
        strategy: RouteStrategy,
    },
    /// Build and publish a local CSR generation.
    Refresh {
        #[arg(long)]
        edge: String,
    },
    /// Run a bounded traversal over one directed edge type.
    Traverse {
        #[arg(long)]
        edge: String,
        #[arg(long, required = true, allow_hyphen_values = true)]
        start: Vec<String>,
        #[arg(long)]
        max_hops: usize,
        #[arg(long)]
        incoming: bool,
        #[arg(long, value_enum, default_value_t = Strategy::Auto)]
        strategy: Strategy,
        #[command(subcommand)]
        mode: Walk,
    },
}

#[derive(Subcommand)]
enum Algorithm {
    #[command(name = "pagerank")]
    PageRank {
        #[arg(long, default_value_t = 0.85)]
        damping: f64,
        #[arg(long, default_value_t = 1e-9)]
        tolerance: f64,
        #[arg(long, default_value_t = 100)]
        max_iterations: usize,
    },
    WeakComponents,
    Louvain(CommunityFlags),
    Leiden(CommunityFlags),
    LabelPropagation {
        #[arg(long, default_value_t = 20)]
        max_iterations: usize,
    },
    #[command(name = "node2vec")]
    Node2Vec {
        #[arg(long, default_value_t = 32)]
        dimensions: usize,
        #[arg(long, default_value_t = 20)]
        walk_length: usize,
        #[arg(long, default_value_t = 4)]
        walks_per_node: usize,
        #[arg(long, default_value_t = 4)]
        window: usize,
        #[arg(long, default_value_t = 2)]
        epochs: usize,
        #[arg(long, default_value_t = 5)]
        negative_samples: usize,
        #[arg(long, default_value_t = 1.0)]
        return_bias: f64,
        #[arg(long, default_value_t = 1.0)]
        explore_bias: f64,
        #[arg(long, default_value_t = 0.025)]
        learning_rate: f64,
        #[arg(long, default_value_t = 42)]
        seed: u64,
    },
}

#[derive(Args)]
struct CommunityFlags {
    #[arg(long, default_value_t = 1.0)]
    resolution: f64,
    #[arg(long, default_value_t = 20)]
    max_iterations: usize,
    #[arg(long, default_value_t = 42)]
    seed: u64,
}

impl From<CommunityFlags> for CommunityOptions {
    fn from(flags: CommunityFlags) -> Self {
        Self {
            resolution: flags.resolution,
            max_iterations: flags.max_iterations,
            seed: flags.seed,
        }
    }
}

#[derive(Args)]
struct ProjectionFlags {
    #[arg(long = "edge")]
    edges: Vec<String>,
    #[arg(long = "node")]
    nodes: Vec<String>,
    #[arg(long)]
    weight: Option<String>,
    #[arg(long, value_enum, default_value_t = Orientation::Outgoing)]
    direction: Orientation,
}

#[derive(Clone, Copy, ValueEnum)]
enum Orientation {
    Outgoing,
    Incoming,
    Undirected,
}

impl From<ProjectionFlags> for GraphProjection {
    fn from(flags: ProjectionFlags) -> Self {
        Self {
            nodes: flags.nodes,
            edges: flags.edges,
            weight: flags.weight,
            direction: match flags.direction {
                Orientation::Outgoing => Direction::Outgoing,
                Orientation::Incoming => Direction::Incoming,
                Orientation::Undirected => Direction::Undirected,
            },
        }
    }
}

#[derive(Subcommand)]
enum Walk {
    Reachable,
    Shortest {
        #[arg(long, required = true, allow_hyphen_values = true)]
        target: Vec<String>,
    },
    Paths {
        #[arg(long, default_value_t = 1)]
        min_hops: usize,
    },
}

#[derive(Clone, Copy, ValueEnum)]
enum Dialect {
    Cypher,
    Sql,
    Gremlin,
}

#[derive(Clone, Copy, ValueEnum)]
enum Strategy {
    Auto,
    Scan,
    Batch,
    Cached,
}

impl From<Strategy> for TraversalStrategy {
    fn from(strategy: Strategy) -> Self {
        match strategy {
            Strategy::Auto => Self::Auto,
            Strategy::Scan => Self::Scan,
            Strategy::Batch => Self::Batch,
            Strategy::Cached => Self::Cached,
        }
    }
}

#[derive(Clone, Copy, ValueEnum)]
enum Format {
    Jsonl,
    Arrow,
}

fn text_key(values: Vec<String>) -> Vec<ScalarValue> {
    values
        .into_iter()
        .map(|value| ScalarValue::Utf8(Some(value)))
        .collect()
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let options = QueryOptions {
        timeout: Duration::from_millis(cli.timeout_ms),
        memory_bytes: cli
            .memory_mib
            .checked_mul(1024 * 1024)
            .ok_or_else(|| anyhow!("memory limit is too large"))?,
        spill_bytes: cli
            .spill_mib
            .checked_mul(1024 * 1024)
            .ok_or_else(|| anyhow!("spill limit is too large"))?,
        partitions: cli.partitions,
        batch_size: cli.batch_size,
        graph: graphlake::GraphOptions {
            logical_partition: cli
                .logical_partition
                .as_deref()
                .map(serde_json::from_str)
                .transpose()?
                .unwrap_or_default(),
            snapshot_time: cli.snapshot_time,
            require_consistent_snapshot: cli.require_consistent_snapshot,
        },
        ..Default::default()
    };
    let mut engine = Engine::new_with_limits(
        Catalog::load(&cli.catalog)?,
        EngineLimits {
            max_queries: 1,
            memory_bytes: options.memory_bytes,
            spill_bytes: options.spill_bytes,
        },
    )?;
    if let (Some(directory), Some(scope)) = (&cli.cache_dir, &cli.cache_scope) {
        engine = engine.with_cache(directory, scope)?;
    }
    let cancellation = options.cancellation.clone();
    let signal = tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            cancellation.cancel();
        }
    });
    let result = async {
        let mut result = match cli.command {
            Command::Query {
                language,
                query,
                parameters,
            } => {
                let language = match language {
                    Dialect::Cypher => Language::Cypher,
                    Dialect::Sql => Language::Sql,
                    Dialect::Gremlin => Language::Gremlin,
                };
                let mut options = options;
                if let Some(parameters) = parameters {
                    options.parameters = serde_json::from_str(&parameters)?;
                }
                engine.query(language, &query, options).await?
            }
            Command::Analyze {
                graph,
                strategy,
                algorithm,
            } => {
                let projection = GraphProjection::from(graph);
                let (algorithm, original) = match algorithm {
                    Algorithm::PageRank {
                        damping,
                        tolerance,
                        max_iterations,
                    } => (
                        ProjectionAlgorithm::PageRank {
                            damping,
                            tolerance,
                            max_iterations,
                        },
                        Some(GraphAlgorithm::PageRank {
                            damping,
                            tolerance,
                            max_iterations,
                        }),
                    ),
                    Algorithm::WeakComponents => (
                        ProjectionAlgorithm::WeaklyConnectedComponents,
                        Some(GraphAlgorithm::WeaklyConnectedComponents),
                    ),
                    Algorithm::Louvain(flags) => (ProjectionAlgorithm::Louvain(flags.into()), None),
                    Algorithm::Leiden(flags) => (ProjectionAlgorithm::Leiden(flags.into()), None),
                    Algorithm::LabelPropagation { max_iterations } => (
                        ProjectionAlgorithm::LabelPropagation { max_iterations },
                        None,
                    ),
                    Algorithm::Node2Vec {
                        dimensions,
                        walk_length,
                        walks_per_node,
                        window,
                        epochs,
                        negative_samples,
                        return_bias,
                        explore_bias,
                        learning_rate,
                        seed,
                    } => (
                        ProjectionAlgorithm::Node2Vec(Node2VecOptions {
                            dimensions,
                            walk_length,
                            walks_per_node,
                            window,
                            epochs,
                            negative_samples,
                            return_bias,
                            explore_bias,
                            learning_rate,
                            seed,
                        }),
                        None,
                    ),
                };
                if let Some(original) = original
                    && let [edge] = projection.edges.as_slice()
                    && projection.nodes.is_empty()
                    && projection.weight.is_none()
                    && matches!(projection.direction, Direction::Outgoing)
                    && engine.has_homogeneous_edge(edge)
                {
                    engine
                        .analyze_graph(edge, original, options, strategy.into())
                        .await?
                } else {
                    ensure!(
                        matches!(strategy, Strategy::Auto | Strategy::Scan),
                        "projected algorithms read a live graph; use auto or scan"
                    );
                    engine
                        .analyze_projection(projection, algorithm, options)
                        .await?
                }
            }
            Command::Route {
                graph,
                start_label,
                start,
                target_label,
                target,
                k,
                strategy,
            } => {
                let request = Route {
                    start: VertexKey {
                        label: start_label,
                        key: text_key(start),
                    },
                    target: VertexKey {
                        label: target_label,
                        key: text_key(target),
                    },
                    k,
                };
                engine
                    .route_with_strategy(graph.into(), request, strategy, options)
                    .await?
            }
            Command::Refresh { edge } => {
                let report = engine.refresh(&edge, options).await?;
                println!("{}", serde_json::to_string(&report)?);
                return Ok(());
            }
            Command::Traverse {
                edge,
                start,
                max_hops,
                incoming,
                strategy,
                mode,
            } => {
                let kind = match mode {
                    Walk::Reachable => TraversalKind::Reachable,
                    Walk::Shortest { target } => TraversalKind::Shortest {
                        target: text_key(target),
                    },
                    Walk::Paths { min_hops } => TraversalKind::Paths { min_hops },
                };
                engine
                    .traverse_with_strategy(
                        Traversal {
                            edge,
                            start: text_key(start),
                            max_hops,
                            kind,
                            direction: if incoming {
                                Direction::Incoming
                            } else {
                                Direction::Outgoing
                            },
                        },
                        options,
                        strategy.into(),
                    )
                    .await?
            }
        };
        let output = write_batches(&mut result, cli.format).await;
        if cli.diagnostics {
            eprintln!(
                "{}",
                serde_json::json!({
                    "graph": result.graph,
                    "sources": result.versions,
                    "plan": result.physical_plan,
                    "metrics": result.metrics(),
                })
            );
        }
        output
    }
    .await;
    signal.abort();
    result
}

async fn write_batches(result: &mut QueryResult, format: Format) -> Result<()> {
    let out = io::stdout();
    let mut out = BufWriter::new(out.lock());
    match format {
        Format::Jsonl => {
            let mut writer = arrow::json::WriterBuilder::new()
                .with_explicit_nulls(true)
                .build::<_, arrow::json::writer::LineDelimited>(&mut out);
            while let Some(batch) = result.batches.try_next().await? {
                writer.write(&batch)?;
            }
            writer.finish()?;
        }
        Format::Arrow => {
            let mut writer =
                arrow::ipc::writer::StreamWriter::try_new(&mut out, &result.batches.schema())?;
            while let Some(batch) = result.batches.try_next().await? {
                writer.write(&batch)?;
            }
            writer.finish()?;
        }
    }
    std::io::Write::flush(&mut out)?;
    Ok(())
}
