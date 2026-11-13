use std::{
    collections::{BTreeSet, HashMap},
    fs::{File, OpenOptions, TryLockError},
    io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    mem::size_of,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, bail, ensure};
use datafusion::{
    arrow::{
        array::{Array, RecordBatch},
        compute::concat_batches,
        ipc::{reader::StreamReader, writer::StreamWriter},
    },
    execution::memory_pool::{MemoryConsumer, MemoryReservation},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{Engine, QueryOptions, ReadMetrics, SourceVersion, engine::QueryScope};

use super::{COOPERATIVE_INTERVAL, Direction, Neighbor, Topology, row_converter, scan_plans};

const MAGIC: &[u8; 8] = b"GLCSR001";
const MAX_MANIFEST_BYTES: usize = 4 * 1024 * 1024;
const IPC_ROWS: usize = 8192;

#[derive(Clone)]
pub(crate) struct Cache {
    dir: PathBuf,
    binding: String,
}

#[derive(Debug)]
pub(super) struct Loaded {
    pub(super) topology: Arc<Topology>,
    pub(super) versions: Vec<SourceVersion>,
    pub(super) generation: String,
    _budget: MemoryReservation,
}

#[derive(Debug, Serialize)]
pub struct RefreshReport {
    pub generation: String,
    pub vertices: usize,
    pub edges: usize,
    pub versions: Vec<SourceVersion>,
    pub elapsed_ms: u64,
    pub peak_reserved_bytes: usize,
    pub reads: ReadMetrics,
}

impl Engine {
    /// Open a local topology cache. Use a distinct permission scope for each access policy.
    pub fn with_cache(
        mut self,
        directory: impl AsRef<Path>,
        permission_scope: &str,
    ) -> Result<Self> {
        ensure!(
            !permission_scope.trim().is_empty(),
            "CSR cache requires a permission scope"
        );
        let dir = std::path::absolute(directory.as_ref())?;
        std::fs::create_dir_all(&dir)?;
        let binding = hex(&Sha256::digest(serde_json::to_vec(&(
            self.catalog.as_ref(),
            permission_scope,
        ))?));
        self.cache = Some(Cache { dir, binding });
        Ok(self)
    }

    /// Build both directions from fresh source snapshots and atomically publish a generation.
    pub async fn refresh(&self, edge: &str, options: QueryOptions) -> Result<RefreshReport> {
        let cache = self
            .cache
            .as_ref()
            .context("refresh requires a CSR cache directory and permission scope")?;
        let mapping = self
            .catalog
            .edges
            .get(edge)
            .with_context(|| format!("unknown edge type {edge}"))?;
        ensure!(
            mapping.from.node == mapping.to.node,
            "CSR refresh requires one endpoint node type"
        );
        let scope = self.scope(options).await?;
        scope
            .run(async {
                let binding = cache.binding(&scope.options.graph)?;
                let path = cache.path(edge, &binding);
                let _lock = lock(&path.with_extension("lock")).await?;
                let prefix = format!(
                    ".{}-stage-",
                    path.file_stem().unwrap_or_default().to_string_lossy()
                );
                clean_staging(&cache.dir, &prefix)?;
                let context = scope.session();
                let labels = BTreeSet::from([edge.to_owned(), mapping.from.node.clone()]);
                let prepared = self
                    .prepare_full_topology(&scope, &context, labels, None)
                    .await?;
                let inputs =
                    scan_plans(&context, &self.catalog, edge, Direction::Outgoing, &[]).await?;
                let budget = MemoryConsumer::new("GraphLake CSR refresh")
                    .register(&scope.runtime.memory_pool);
                let topology = inputs.load(context.task_ctx(), &budget).await?;
                let reverse = reverse(&topology, &budget).await?;
                let manifest = Manifest::new(&binding, edge, &topology, &prepared.versions);
                let vertices = manifest.vertices;
                let edges = manifest.edges;
                let scope_for_io = Arc::clone(&scope);
                let directory = cache.dir.clone();
                let (file, generation) = tokio::task::spawn_blocking(move || {
                    let _budget = budget;
                    stage(
                        &directory,
                        &prefix,
                        &manifest,
                        &topology,
                        &reverse,
                        &scope_for_io,
                    )
                })
                .await??;
                scope.check()?;
                file.persist(&path)
                    .with_context(|| format!("publishing CSR generation to {}", path.display()))?;
                File::open(&cache.dir)?
                    .sync_all()
                    .context("synchronizing CSR cache directory")?;
                Ok(RefreshReport {
                    generation,
                    vertices,
                    edges,
                    versions: prepared.versions,
                    elapsed_ms: scope.elapsed_ms(),
                    peak_reserved_bytes: scope.peak_reserved_bytes(),
                    reads: scope.reads.metrics(),
                })
            })
            .await
    }
}

impl Cache {
    fn binding(&self, options: &crate::GraphOptions) -> Result<String> {
        let time = options
            .snapshot_time
            .as_deref()
            .filter(|time| !time.eq_ignore_ascii_case("current"));
        if options.logical_partition.is_empty()
            && time.is_none()
            && !options.require_consistent_snapshot
        {
            return Ok(self.binding.clone());
        }
        let mut predicates = options
            .logical_partition
            .iter()
            .map(serde_json::to_string)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        predicates.sort_unstable();
        let mut scope =
            serde_json::to_vec(&(self.binding.as_str(), predicates, &options.snapshot_time))?;
        if options.require_consistent_snapshot {
            scope.extend_from_slice(b"/consistent-snapshot");
        }
        Ok(hex(&Sha256::digest(scope)))
    }

    fn path(&self, edge: &str, binding: &str) -> PathBuf {
        let digest = Sha256::digest([binding.as_bytes(), edge.as_bytes()].concat());
        self.dir.join(format!("{}.csr", hex(&digest)))
    }

    pub(super) async fn load(
        &self,
        edge: &str,
        direction: Direction,
        scope: Arc<QueryScope>,
    ) -> Result<Option<Arc<Loaded>>> {
        let binding = self.binding(&scope.options.graph)?;
        let path = self.path(edge, &binding);
        let edge = edge.to_owned();
        tokio::task::spawn_blocking(move || {
            let file = match File::open(&path) {
                Ok(file) => file,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                Err(error) => return Err(error.into()),
            };
            read(file, &binding, &edge, direction, &scope)
                .map(Arc::new)
                .map(Some)
                .with_context(|| format!("reading CSR generation {}", path.display()))
        })
        .await?
    }
}

#[derive(Serialize, Deserialize)]
struct Manifest {
    binding: String,
    edge: String,
    versions: Vec<(String, String, String)>,
    vertices: usize,
    edges: usize,
}

impl Manifest {
    fn new(binding: &str, edge: &str, topology: &Topology, versions: &[SourceVersion]) -> Self {
        Self {
            binding: binding.into(),
            edge: edge.into(),
            versions: versions
                .iter()
                .map(|v| (v.source.clone(), v.version.clone(), v.consistency.into()))
                .collect(),
            vertices: topology.keys.num_rows(),
            edges: topology.neighbors.len(),
        }
    }

    fn source_versions(self) -> Result<Vec<SourceVersion>> {
        self.versions
            .into_iter()
            .map(|(source, version, consistency)| {
                let consistency = match consistency.as_str() {
                    "PostgreSQL repeatable read" => "PostgreSQL repeatable read",
                    "pinned object" => "pinned object",
                    "Iceberg snapshot" => "Iceberg snapshot",
                    "Delta version" => "Delta version",
                    "Snowflake time travel" => "Snowflake time travel",
                    "BigQuery time travel" => "BigQuery time travel",
                    "Trino statement" => "Trino statement",
                    "ClickHouse statement" => "ClickHouse statement",
                    "MySQL InnoDB repeatable read" => "MySQL InnoDB repeatable read",
                    _ => bail!("unknown CSR source consistency"),
                };
                Ok(SourceVersion {
                    source,
                    version,
                    consistency,
                })
            })
            .collect()
    }
}

async fn lock(path: &Path) -> Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)?;
    loop {
        match file.try_lock() {
            Ok(()) => return Ok(file),
            Err(TryLockError::WouldBlock) => tokio::time::sleep(Duration::from_millis(20)).await,
            Err(TryLockError::Error(error)) => return Err(error.into()),
        }
    }
}

fn clean_staging(directory: &Path, prefix: &str) -> Result<()> {
    for entry in std::fs::read_dir(directory)? {
        let entry = entry?;
        if entry.file_name().to_string_lossy().starts_with(prefix)
            && let Err(error) = std::fs::remove_file(entry.path())
            && error.kind() != std::io::ErrorKind::NotFound
        {
            return Err(error.into());
        }
    }
    Ok(())
}

type Adjacency = (Vec<usize>, Vec<Neighbor>);

async fn reverse(topology: &Topology, budget: &MemoryReservation) -> Result<Adjacency> {
    let n = topology.keys.num_rows();
    let m = topology.neighbors.len();
    budget.try_grow(
        (n + 1)
            .saturating_mul(2 * size_of::<usize>())
            .saturating_add(m.saturating_mul(size_of::<Neighbor>())),
    )?;
    let mut offsets = vec![0; n + 1];
    for chunk in topology.neighbors.chunks(COOPERATIVE_INTERVAL) {
        for neighbor in chunk {
            offsets[neighbor.vertex + 1] += 1;
        }
        tokio::task::yield_now().await;
    }
    for i in 1..=n {
        offsets[i] += offsets[i - 1];
        if i % COOPERATIVE_INTERVAL == 0 {
            tokio::task::yield_now().await;
        }
    }
    let mut positions = offsets[..n].to_vec();
    let mut neighbors = vec![Neighbor { vertex: 0, edge: 0 }; m];
    let mut work = 0;
    for (from, range) in topology.offsets.windows(2).enumerate() {
        for edge in &topology.neighbors[range[0]..range[1]] {
            let position = &mut positions[edge.vertex];
            neighbors[*position] = Neighbor {
                vertex: from,
                edge: edge.edge,
            };
            *position += 1;
            work += 1;
            if work == COOPERATIVE_INTERVAL {
                work = 0;
                tokio::task::yield_now().await;
            }
        }
        if from % COOPERATIVE_INTERVAL == 0 {
            tokio::task::yield_now().await;
        }
    }
    Ok((offsets, neighbors))
}

fn stage(
    directory: &Path,
    prefix: &str,
    manifest: &Manifest,
    topology: &Topology,
    reverse: &Adjacency,
    scope: &QueryScope,
) -> Result<(tempfile::NamedTempFile, String)> {
    let mut file = tempfile::Builder::new()
        .prefix(prefix)
        .tempfile_in(directory)?;
    let manifest = serde_json::to_vec(manifest)?;
    ensure!(
        manifest.len() <= MAX_MANIFEST_BYTES,
        "CSR manifest exceeds size limit"
    );
    {
        let mut writer = BufWriter::new(file.as_file_mut());
        writer.write_all(MAGIC)?;
        write_word(&mut writer, manifest.len())?;
        writer.write_all(&manifest)?;
        write_arrow(&mut writer, &topology.keys, scope)?;
        write_arrow(&mut writer, &topology.identities, scope)?;
        write_csr(&mut writer, &topology.offsets, &topology.neighbors, scope)?;
        write_csr(&mut writer, &reverse.0, &reverse.1, scope)?;
        writer.flush()?;
    }
    let body = file.as_file().metadata()?.len();
    let hash = hash_prefix(file.as_file_mut(), body, scope)?;
    file.write_all(&hash)?;
    file.as_file().sync_all()?;
    Ok((file, hex(&hash)))
}

fn read(
    mut file: File,
    binding: &str,
    edge: &str,
    direction: Direction,
    scope: &QueryScope,
) -> Result<Loaded> {
    let size = file.metadata()?.len();
    let body = size.checked_sub(32).context("truncated CSR generation")?;
    let budget =
        MemoryConsumer::new("GraphLake persisted CSR").register(&scope.runtime.memory_pool);
    budget.try_grow(usize::try_from(size)?)?;
    let hash = hash_prefix(&mut file, body, scope)?;
    let mut expected = [0_u8; 32];
    file.read_exact(&mut expected)?;
    ensure!(hash == expected, "CSR generation checksum mismatch");
    file.rewind()?;
    let mut reader = BufReader::new(file);
    let mut magic = [0_u8; 8];
    reader.read_exact(&mut magic)?;
    ensure!(&magic == MAGIC, "unsupported CSR file format");
    let len = read_word(&mut reader)?;
    ensure!(len <= MAX_MANIFEST_BYTES, "CSR manifest exceeds size limit");
    let mut metadata = vec![0_u8; len];
    reader.read_exact(&mut metadata)?;
    let manifest: Manifest = serde_json::from_slice(&metadata)?;
    ensure!(
        manifest.binding == binding && manifest.edge == edge,
        "CSR generation belongs to a different graph or permission scope"
    );
    let keys = read_arrow(&mut reader, scope, &budget)?;
    let identities = read_arrow(&mut reader, scope, &budget)?;
    ensure!(
        keys.num_rows() == manifest.vertices,
        "CSR vertex count mismatch"
    );
    let csr_bytes = manifest
        .vertices
        .checked_add(1)
        .and_then(|n| n.checked_add(manifest.edges.checked_mul(2)?))
        .and_then(|n| n.checked_mul(8))
        .context("invalid CSR array size")?;
    let arrays = reader.stream_position()?;
    ensure!(
        arrays.checked_add(
            (csr_bytes as u64)
                .checked_mul(2)
                .context("invalid CSR array size")?
        ) == Some(body),
        "CSR array lengths do not match file size"
    );
    if matches!(direction, Direction::Incoming) {
        reader.seek(SeekFrom::Start(arrays + csr_bytes as u64))?;
    }
    let (mut offsets, mut neighbors) = read_csr(
        &mut reader,
        manifest.vertices,
        manifest.edges,
        identities.num_rows(),
        scope,
    )?;
    if matches!(direction, Direction::Undirected) {
        let (reverse_offsets, reverse_neighbors) = read_csr(
            &mut reader,
            manifest.vertices,
            manifest.edges,
            identities.num_rows(),
            scope,
        )?;
        budget.try_grow(
            manifest
                .edges
                .saturating_mul(2 * size_of::<Neighbor>())
                .saturating_add(manifest.vertices.saturating_add(1) * size_of::<usize>()),
        )?;
        let mut combined_offsets = Vec::with_capacity(offsets.len());
        let mut combined = Vec::with_capacity(neighbors.len().saturating_mul(2));
        combined_offsets.push(0);
        for vertex in 0..manifest.vertices {
            if vertex % COOPERATIVE_INTERVAL == 0 {
                scope.check()?;
            }
            combined.extend_from_slice(&neighbors[offsets[vertex]..offsets[vertex + 1]]);
            combined.extend(
                reverse_neighbors[reverse_offsets[vertex]..reverse_offsets[vertex + 1]]
                    .iter()
                    .filter(|neighbor| neighbor.vertex != vertex)
                    .copied(),
            );
            combined_offsets.push(combined.len());
        }
        offsets = combined_offsets;
        neighbors = combined;
    }
    let converter = row_converter(&keys.schema())?;
    let mut index = HashMap::new();
    for offset in (0..keys.num_rows()).step_by(IPC_ROWS) {
        scope.check()?;
        let batch = keys.slice(offset, IPC_ROWS.min(keys.num_rows() - offset));
        ensure!(
            batch
                .columns()
                .iter()
                .all(|column| column.null_count() == 0),
            "null node key in CSR generation"
        );
        for row in converter.convert_columns(batch.columns())?.iter() {
            budget.try_grow(row.data().len().saturating_add(160))?;
            let id = index.len();
            ensure!(
                index.insert(row.data().into(), id).is_none(),
                "duplicate node key in CSR generation"
            );
        }
    }
    let generation = hex(&hash);
    Ok(Loaded {
        topology: Arc::new(Topology {
            keys,
            identities,
            offsets,
            neighbors,
            index,
        }),
        versions: manifest.source_versions()?,
        generation,
        _budget: budget,
    })
}

fn write_arrow(writer: &mut impl Write, batch: &RecordBatch, scope: &QueryScope) -> Result<()> {
    let mut stream = StreamWriter::try_new(writer, &batch.schema())?;
    for offset in (0..batch.num_rows()).step_by(IPC_ROWS) {
        scope.check()?;
        stream.write(&batch.slice(offset, IPC_ROWS.min(batch.num_rows() - offset)))?;
    }
    stream.finish()?;
    Ok(())
}

fn read_arrow(
    reader: &mut impl Read,
    scope: &QueryScope,
    budget: &MemoryReservation,
) -> Result<RecordBatch> {
    let stream = StreamReader::try_new(reader, None)?;
    let schema = stream.schema();
    let mut batches = Vec::new();
    for batch in stream {
        scope.check()?;
        let batch = batch?;
        budget.try_grow(batch.get_array_memory_size().saturating_mul(2))?;
        batches.push(batch);
    }
    Ok(concat_batches(&schema, &batches)?)
}

fn write_csr(
    writer: &mut impl Write,
    offsets: &[usize],
    neighbors: &[Neighbor],
    scope: &QueryScope,
) -> Result<()> {
    for chunk in offsets.chunks(COOPERATIVE_INTERVAL) {
        scope.check()?;
        for &offset in chunk {
            write_word(writer, offset)?;
        }
    }
    for chunk in neighbors.chunks(COOPERATIVE_INTERVAL) {
        scope.check()?;
        for neighbor in chunk {
            write_word(writer, neighbor.vertex)?;
            write_word(writer, neighbor.edge)?;
        }
    }
    Ok(())
}

fn read_csr(
    reader: &mut impl Read,
    n: usize,
    m: usize,
    identities: usize,
    scope: &QueryScope,
) -> Result<Adjacency> {
    let mut offsets = Vec::with_capacity(n + 1);
    for i in 0..=n {
        if i % COOPERATIVE_INTERVAL == 0 {
            scope.check()?;
        }
        let offset = read_word(reader)?;
        ensure!(
            offset <= m && offsets.last().is_none_or(|previous| *previous <= offset),
            "invalid CSR offset"
        );
        offsets.push(offset);
    }
    ensure!(offsets[0] == 0 && offsets[n] == m, "invalid CSR boundaries");
    let mut neighbors = Vec::with_capacity(m);
    for i in 0..m {
        if i % COOPERATIVE_INTERVAL == 0 {
            scope.check()?;
        }
        let vertex = read_word(reader)?;
        let edge = read_word(reader)?;
        ensure!(vertex < n && edge < identities, "invalid CSR neighbor");
        neighbors.push(Neighbor { vertex, edge });
    }
    Ok((offsets, neighbors))
}

fn read_word(reader: &mut impl Read) -> Result<usize> {
    let mut bytes = [0_u8; 8];
    reader.read_exact(&mut bytes)?;
    Ok(usize::try_from(u64::from_le_bytes(bytes))?)
}

fn write_word(writer: &mut impl Write, value: usize) -> Result<()> {
    Ok(writer.write_all(&(value as u64).to_le_bytes())?)
}

fn hash_prefix(file: &mut File, mut size: u64, scope: &QueryScope) -> Result<[u8; 32]> {
    file.rewind()?;
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    while size > 0 {
        scope.check()?;
        let chunk = size.min(buffer.len() as u64) as usize;
        file.read_exact(&mut buffer[..chunk])?;
        digest.update(&buffer[..chunk]);
        size -= chunk as u64;
    }
    Ok(digest.finalize().into())
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(output, "{byte:02x}").expect("writing to a string");
    }
    output
}
