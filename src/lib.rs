//! Embedded, read-only graph analytics over PostgreSQL and Parquet.

#[cfg(feature = "admin")]
pub mod admin;

mod catalog;
mod cypher;
mod engine;
mod graph_options;
mod gremlin;
mod scalar;
mod source;
mod traversal;

pub use catalog::{
    Catalog, Comparison, ConflictPolicy, DeltaTable, Edge, Endpoint, GraphInput, Node, Predicate,
    Scd2, Source, TrinoCatalog,
};
pub use datafusion::{arrow, common::ScalarValue, execution::SendableRecordBatchStream};
pub use engine::{Engine, EngineLimits, Language, QueryMetrics, QueryOptions, QueryResult};
pub use graph_options::GraphOptions;
pub use source::{ReadMetrics, SourceVersion};
pub use tokio_util::sync::CancellationToken;
pub use traversal::{
    CommunityOptions, Direction, GraphAlgorithm, GraphAlgorithmMetrics, GraphProjection,
    Node2VecOptions, ProjectionAlgorithm, RefreshReport, Route, RouteStrategy, Traversal,
    TraversalKind, TraversalStrategy, VertexKey,
};
