//! Embedded, read-only graph analytics over databases, text files and lakehouse tables.

#[cfg(feature = "admin")]
pub mod admin;

mod catalog;
mod cypher;
mod engine;
mod graph_options;
mod gremlin;
mod json_output;
mod scalar;
mod source;
mod traversal;

pub use catalog::{
    Catalog, Comparison, ConflictPolicy, DeltaTable, Edge, Endpoint, Freshness, FreshnessPolicy,
    GraphInput, MysqlConnection, Node, ObjectFormat, Predicate, Scd2, Source, StorageConfig,
    TextColumn, TextTable, TrinoCatalog,
};
pub use datafusion::{arrow, common::ScalarValue, execution::SendableRecordBatchStream};
pub use engine::{Engine, EngineLimits, Language, QueryMetrics, QueryOptions, QueryResult};
pub use graph_options::GraphOptions;
#[doc(hidden)]
pub use json_output::json_writer;
pub use source::{ReadMetrics, SourceVersion};
pub use tokio_util::sync::CancellationToken;
pub use traversal::{
    ALGORITHM_CAPABILITIES, AlgorithmCapability, CommunityOptions, Direction, GraphAlgorithm,
    GraphAlgorithmMetrics, GraphProjection, Node2VecOptions, ProjectionAlgorithm, RefreshReport,
    Route, RouteStrategy, Traversal, TraversalKind, TraversalStrategy, VertexKey,
};
