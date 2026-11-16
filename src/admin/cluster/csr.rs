mod bounds;
mod build;
mod execute;
mod freshness;

pub(in crate::admin) use freshness::{RefreshStatus, health, watch};

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, ensure};
use axum::{
    Extension, Json,
    extract::{Path, State},
    http::StatusCode,
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use datafusion::arrow::{
    datatypes::Schema,
    ipc::{reader::StreamReader, writer::StreamWriter},
};
use object_store::path::Path as Key;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::{Objects, storage};
use crate::{
    Direction, GraphOptions, SourceVersion,
    admin::{ApiResult, App, Error, access::Principal, query},
    engine::QueryScope,
};

pub(in crate::admin) struct ExplorationSnapshot {
    objects: Objects,
    prefix: Key,
    manifest: Arc<Manifest>,
}

impl ExplorationSnapshot {
    pub(in crate::admin) async fn open(
        app: &App,
        model: &str,
        revision: &str,
        generation: Uuid,
    ) -> ApiResult<Self> {
        let cluster = app.cluster.as_ref().ok_or_else(Error::not_found)?;
        let objects = cluster.shared.objects.clone();
        let manifest = manifest(&objects, model, revision, generation)
            .await
            .map_err(|error| {
                if error.chain().any(|cause| {
                    matches!(
                        cause.downcast_ref::<object_store::Error>(),
                        Some(object_store::Error::NotFound { .. })
                    )
                }) {
                    Error::new(
                        StatusCode::CONFLICT,
                        "csr_snapshot_expired",
                        "原图快照已不可用，请选择当前代次",
                    )
                } else {
                    storage(error)
                }
            })?;
        if manifest.format < 3 {
            return Err(Error::new(
                StatusCode::CONFLICT,
                "csr_refresh_required",
                "请刷新 CSR 以生成探索所需的属性快照",
            ));
        }
        Ok(Self {
            prefix: objects.graph(model, revision, generation),
            objects,
            manifest: Arc::new(manifest),
        })
    }

    pub(in crate::admin) fn options(&self) -> &GraphOptions {
        &self.manifest.graph_options
    }

    pub(in crate::admin) async fn prepare(
        &self,
        context: &datafusion::prelude::SessionContext,
        scope: &Arc<QueryScope>,
        catalog: &crate::Catalog,
        labels: std::collections::BTreeSet<String>,
    ) -> Result<crate::engine::Prepared> {
        execute::register_tables(
            &self.objects,
            self.prefix.clone(),
            &self.manifest,
            context,
            scope,
            catalog,
            labels,
        )
        .await
    }
}

const MANIFEST_LIMIT: u64 = 32 * 1024 * 1024;

#[derive(Debug)]
pub(in crate::admin) struct CurrentValidity;

impl std::fmt::Display for CurrentValidity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("current SCD2 validity requires live data or an explicit CSR snapshot_time")
    }
}

impl std::error::Error for CurrentValidity {}
const BLOCK_BYTES: usize = 256 * 1024;
const MAX_HOPS: u32 = 1024;
const LOOKUP_ID: &str = "__graphlake_csr_lookup_id";

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct Identity {
    label: String,
    key: Vec<String>,
}

type Vertex = u64;

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
struct ArcEntry {
    from: Vertex,
    to: Vertex,
    edge: u64,
    kind: u32,
    reverse: bool,
}

#[derive(Clone, Serialize, Deserialize)]
struct Block {
    bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    decoded_bytes: Option<u64>,
    sha256: [u8; 32],
    first: u64,
    last: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    ids: Option<[String; 2]>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    numbers: BTreeMap<String, Option<[String; 2]>>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    columns: Vec<ColumnPage>,
}

impl Block {
    fn decoded_size(&self) -> usize {
        self.decoded_bytes.unwrap_or(self.bytes) as usize
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct ColumnPage {
    bytes: u32,
    sha256: [u8; 32],
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Version {
    source: String,
    version: String,
    consistency: String,
}

#[derive(Serialize, Deserialize)]
struct Manifest {
    format: u32,
    generation: Uuid,
    model_revision: String,
    graph_options: GraphOptions,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    clock_labels: Option<BTreeSet<String>>,
    sources: Vec<Version>,
    #[serde(default)]
    built_at_ms: Option<u64>,
    #[serde(default)]
    local_generations: BTreeMap<String, String>,
    schemas: BTreeMap<String, String>,
    edge_types: Vec<String>,
    partitions: Vec<Partition>,
    edge_keys: Vec<Block>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    edge_identities: BTreeMap<String, Vec<Block>>,
    #[serde(default)]
    edge_schemas: BTreeMap<String, String>,
    #[serde(default)]
    tables: BTreeMap<String, Vec<Block>>,
    #[serde(default)]
    table_schemas: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    filter_columns: BTreeMap<String, Vec<String>>,
    vertices: u64,
    edges: u64,
    bytes: u64,
    build_ms: u64,
    reads: serde_json::Value,
}

pub(super) async fn verify_freshness(
    cluster: &super::Coordinator,
    model: &str,
    revision: &str,
    generation: Uuid,
    policy: &crate::FreshnessPolicy,
    inputs: &[crate::admin::freshness::Observation],
) -> ApiResult<()> {
    use crate::admin::freshness;
    if policy.require_current_csr
        && cluster
            .shared
            .metadata
            .graph(model, revision)
            .await
            .map_err(storage)?
            .map(|(head, _)| head)
            != Some(generation)
    {
        return Err(freshness::conflict("CSR 已发布更新代次"));
    }
    let cached = manifest(&cluster.shared.objects, model, revision, generation)
        .await
        .map_err(storage)?;
    if policy.require_consistent_snapshot && !cached.graph_options.require_consistent_snapshot {
        return Err(freshness::conflict("CSR 构建没有使用可证明的一致快照"));
    }
    if !inputs.is_empty() && !self::freshness::is_current(&cached, inputs) {
        return Err(freshness::conflict("CSR 依赖的本地副本代次已落后或未知"));
    }
    Ok(())
}

struct Generation<'a> {
    id: Uuid,
    revision: &'a str,
}

#[derive(Default, Serialize, Deserialize)]
struct Partition {
    vertices: Vec<Block>,
    arcs: Vec<Block>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::admin) struct Refresh {
    model_revision: String,
    #[serde(default)]
    graph_options: GraphOptions,
    timeout_ms: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum Operation {
    Reachable {
        start: Endpoint,
        max_hops: u32,
    },
    Shortest {
        start: Endpoint,
        target: Endpoint,
        max_hops: u32,
    },
    AllShortest {
        start: Endpoint,
        target: Endpoint,
        max_hops: u32,
        max_paths: usize,
    },
    Pagerank {
        damping: f64,
        tolerance: f64,
        max_iterations: usize,
    },
    Wcc {
        max_iterations: usize,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Endpoint {
    label: String,
    key: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::admin) struct Request {
    pub(super) generation: Uuid,
    #[serde(default = "outgoing")]
    direction: Direction,
    #[serde(default)]
    edge_types: Vec<String>,
    operation: Operation,
}

fn outgoing() -> Direction {
    Direction::Outgoing
}

pub(in crate::admin) async fn current(
    State(app): State<Arc<App>>,
    Path(model): Path<String>,
    Extension(principal): Extension<Principal>,
) -> ApiResult<Json<serde_json::Value>> {
    let cluster = app.cluster.as_ref().ok_or_else(Error::not_found)?;
    crate::admin::access::check_query(&app, &principal, &model)?;
    let revision = app.published_model_revision(&model).await?;
    let (generation, _) = cluster
        .shared
        .metadata
        .graph(&model, &revision)
        .await
        .map_err(storage)?
        .ok_or_else(Error::not_found)?;
    let manifest = manifest(&cluster.shared.objects, &model, &revision, generation)
        .await
        .map_err(storage)?;
    Ok(Json(manifest.summary()))
}

pub(in crate::admin) async fn refresh(
    State(app): State<Arc<App>>,
    Path(model): Path<String>,
    Extension(principal): Extension<Principal>,
    Json(request): Json<Refresh>,
) -> ApiResult<Json<serde_json::Value>> {
    Ok(Json(
        freshness::refresh_model(&app, &model, &principal, request)
            .await?
            .summary(),
    ))
}

async fn build_and_publish(
    app: &Arc<App>,
    model: &str,
    principal: &Principal,
    request: Refresh,
) -> ApiResult<Manifest> {
    let cluster = app
        .cluster
        .as_ref()
        .filter(|c| c.graph_partitions > 0)
        .ok_or_else(Error::not_found)?;
    let _slot = app.metadata_work.try_acquire().map_err(|_| Error::busy())?;
    let (_, _, mut engine) = app
        .query_model(model, &request.model_revision, principal)
        .await?;
    let (_, prior) = cluster
        .shared
        .metadata
        .graph(model, &request.model_revision)
        .await
        .map_err(storage)?
        .unwrap_or((Uuid::nil(), 0));
    let timeout = request.timeout_ms.unwrap_or(60_000);
    if !(1..=600_000).contains(&timeout) {
        return Err(Error::invalid(
            "invalid_timeout",
            "构建超时必须为 1 至 600000 毫秒",
        ));
    }
    let _selection =
        crate::admin::local_tables::managed::resolve(app, &mut engine, model, timeout / 1000 + 120)
            .await?;
    let scope = engine
        .scope(crate::QueryOptions {
            timeout: Duration::from_millis(timeout),
            cancellation: app.shutdown.child_token(),
            memory_bytes: cluster.memory_bytes,
            graph: request.graph_options,
            ..Default::default()
        })
        .await
        .map_err(query::query_error)?;
    let generation = Uuid::new_v4();
    let prefix = cluster
        .shared
        .objects
        .graph(model, &request.model_revision, generation);
    let built = scope
        .run(build::build(
            &engine,
            &scope,
            &cluster.shared.objects,
            &prefix,
            Generation {
                id: generation,
                revision: &request.model_revision,
            },
            cluster.graph_partitions,
        ))
        .await;
    let built = match built {
        Ok(built) => built,
        Err(error) => {
            cluster.shared.objects.clean_prefix(&prefix).await;
            return Err(query::query_error(error));
        }
    };
    let active = app.models.lock().unwrap().active_revision(model)?;
    if active.as_deref() != Some(request.model_revision.as_str()) {
        cluster.shared.objects.clean_prefix(&prefix).await;
        return Err(Error::new(
            StatusCode::CONFLICT,
            "active_revision_conflict",
            "生效模型版本已变化",
        ));
    }
    if !cluster
        .shared
        .metadata
        .publish_graph(
            model,
            &request.model_revision,
            prior,
            generation,
            app.ha.as_ref().map(|ha| ha.leader.as_ref()),
        )
        .await
        .map_err(storage)?
    {
        cluster.shared.objects.clean_prefix(&prefix).await;
        return Err(Error::new(
            StatusCode::CONFLICT,
            "csr_refresh_conflict",
            "CSR 已被另一轮刷新替换",
        ));
    }
    Ok(built)
}

impl Manifest {
    fn summary(&self) -> serde_json::Value {
        serde_json::json!({
            "generation": self.generation, "model_revision": self.model_revision, "format": self.format,
            "graph_options": self.graph_options, "sources": self.sources,
            "partitions": self.partitions.len(), "vertices": self.vertices, "edges": self.edges,
            "bytes": self.bytes, "build_ms": self.build_ms, "built_at_ms": self.built_at_ms, "source_reads": self.reads
        })
    }

    fn source_versions(&self) -> Vec<SourceVersion> {
        self.sources
            .iter()
            .map(|v| SourceVersion {
                source: v.source.clone(),
                version: v.version.clone(),
                consistency: match v.consistency.as_str() {
                    "pinned object" => "pinned object",
                    "PostgreSQL repeatable read" => "PostgreSQL repeatable read",
                    "MySQL InnoDB repeatable read" => "MySQL InnoDB repeatable read",
                    "SQLite read transaction" => "SQLite read transaction",
                    "DuckDB read transaction" => "DuckDB read transaction",
                    "Oracle read-only transaction" => "Oracle read-only transaction",
                    "SQL Server statement" => "SQL Server statement",
                    "MongoDB cursor" => "MongoDB cursor",
                    "Databricks statement" => "Databricks statement",
                    "MySQL-compatible statement" => "MySQL-compatible statement",
                    "Iceberg snapshot" => "Iceberg snapshot",
                    "Delta version" => "Delta version",
                    "Spanner read-only transaction" => "Spanner read-only transaction",
                    _ => "published CSR",
                },
            })
            .collect()
    }
}

async fn manifest(
    objects: &Objects,
    model: &str,
    revision: &str,
    generation: Uuid,
) -> Result<Manifest> {
    let prefix = objects.graph(model, revision, generation);
    let manifest: Manifest = objects
        .get_json(&prefix.join("manifest.json"), MANIFEST_LIMIT)
        .await?;
    ensure!(
        matches!(manifest.format, 2..=5)
            && manifest.generation == generation
            && manifest.model_revision == revision
            && (2..=8).contains(&manifest.partitions.len()),
        "invalid CSR manifest"
    );
    Ok(manifest)
}

fn encode_schema(schema: &Schema) -> Result<String> {
    Ok(STANDARD.encode(StreamWriter::try_new(Vec::new(), schema)?.into_inner()?))
}

fn decode_schema(encoded: &str) -> Result<Arc<Schema>> {
    Ok(StreamReader::try_new(std::io::Cursor::new(STANDARD.decode(encoded)?), None)?.schema())
}

fn vertex_id(label: &str, encoded: &[u8]) -> Vertex {
    let mut hash = Sha256::new();
    hash.update((label.len() as u64).to_le_bytes());
    hash.update(label.as_bytes());
    hash.update(encoded);
    u64::from_le_bytes(hash.finalize()[..8].try_into().expect("digest width"))
}

fn partition(vertex: Vertex, count: usize) -> usize {
    (vertex % count as u64) as usize
}

async fn write_block(
    objects: &Objects,
    key: &Key,
    first: u64,
    last: u64,
    bytes: Vec<u8>,
) -> Result<Block> {
    write_block_data(objects, key, first, last, bytes, false).await
}

async fn write_block_data(
    objects: &Objects,
    key: &Key,
    first: u64,
    last: u64,
    bytes: Vec<u8>,
    compress: bool,
) -> Result<Block> {
    let sha256 = Sha256::digest(&bytes).into();
    let original_size = bytes.len();
    let stored = if compress {
        let encoded = zstd::bulk::compress(&bytes, 1)?;
        if encoded.len() < bytes.len() {
            encoded
        } else {
            bytes
        }
    } else {
        bytes
    };
    let block = Block {
        first,
        last,
        ids: None,
        numbers: BTreeMap::new(),
        columns: Vec::new(),
        bytes: stored.len() as u64,
        decoded_bytes: (stored.len() != original_size).then_some(original_size as u64),
        sha256,
    };
    objects.put_bytes(key, stored).await?;
    Ok(block)
}

async fn read_block(
    objects: &Objects,
    key: &Key,
    block: &Block,
    scope: &QueryScope,
) -> Result<bytes::Bytes> {
    scope.check()?;
    let result = objects
        .get(key)
        .await
        .context("reading published CSR block")?;
    ensure!(
        result.meta.size == block.bytes
            && block.bytes <= 2 * BLOCK_BYTES as u64
            && block
                .decoded_bytes
                .is_none_or(|size| size > block.bytes && size <= BLOCK_BYTES as u64),
        "invalid CSR block size"
    );
    let _encoded = if block.decoded_bytes.is_some() {
        use datafusion::execution::memory_pool::MemoryConsumer;
        let memory = MemoryConsumer::new("CSR compressed identity block")
            .register(&scope.runtime.memory_pool);
        memory.try_grow(block.bytes as usize)?;
        Some(memory)
    } else {
        None
    };
    let bytes = result.bytes().await?;
    ensure!(bytes.len() as u64 == block.bytes, "invalid CSR block size");
    let data = match block.decoded_bytes {
        Some(size) => bytes::Bytes::from(zstd::bulk::decompress(&bytes, size as usize)?),
        None => bytes,
    };
    ensure!(
        data.len() == block.decoded_size() && Sha256::digest(&data).as_slice() == block.sha256,
        "CSR block checksum mismatch"
    );
    scope.reads.object_bytes(block.bytes as usize);
    scope.reads.request();
    Ok(data)
}

fn compact_view(array: datafusion::arrow::array::ArrayRef) -> datafusion::arrow::array::ArrayRef {
    use datafusion::arrow::array::{BinaryViewArray, StringViewArray};
    if let Some(array) = array.as_any().downcast_ref::<StringViewArray>() {
        Arc::new(array.gc())
    } else if let Some(array) = array.as_any().downcast_ref::<BinaryViewArray>() {
        Arc::new(array.gc())
    } else {
        array
    }
}

pub(crate) use execute::CypherCsr;
pub(in crate::admin) use execute::Stats;
pub(super) use execute::{ArcBlocks, query_csr, run_task};
