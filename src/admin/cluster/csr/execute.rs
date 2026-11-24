mod algorithms;
mod cypher;
mod iterative;
mod properties;
pub(crate) use cypher::CypherCsr;
pub(super) use cypher::register_tables;

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::{Arc, OnceLock},
};

use anyhow::{Context, Result, ensure};
use axum::{
    Json,
    extract::State,
    http::{HeaderMap, StatusCode},
};
use datafusion::{
    arrow::{
        array::{ArrayRef, Float64Array, RecordBatch, StringArray, UInt64Array},
        datatypes::{DataType, Field, Schema},
    },
    execution::memory_pool::{MemoryConsumer, MemoryReservation},
};
use futures::{StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{
    super::{Job, JobState, Objects, WORK_BUDGET_HEADER, storage, work_deadline, worker::Worker},
    *,
};
use crate::{
    Engine, QueryOptions, QueryResult, ScalarValue,
    engine::Prepared,
    source::snapshot::Buffer,
    traversal::{graph_encode_key, graph_row_converter},
};

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum Step {
    Frontier {
        frontier: BTreeSet<Vertex>,
        path_limit: usize,
        target: Option<Vertex>,
    },
    Arcs {
        frontier: BTreeSet<Vertex>,
        start_block: usize,
        end_block: usize,
        #[serde(default)]
        targets: Option<BTreeSet<Vertex>>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        filters: Vec<(u32, Vec<String>)>,
    },
    Iteration(iterative::Step),
}

#[derive(Serialize, Deserialize)]
pub(in crate::admin::cluster) struct Task {
    id: Uuid,
    attempt: Uuid,
    generation: Uuid,
    shard: usize,
    direction: Direction,
    types: BTreeSet<u32>,
    input: Uuid,
    output: Uuid,
}

#[derive(Default, Deserialize, Serialize)]
struct Output {
    worker: Option<Uuid>,
    read_bytes: u64,
    #[serde(default)]
    shuffled_bytes: u64,
    #[serde(default)]
    peak_memory: usize,
    #[serde(default)]
    filtered_arcs: u64,
    #[serde(default, skip_serializing_if = "is_zero")]
    skipped_property_blocks: u64,
    #[serde(default, skip_serializing_if = "is_zero")]
    filter_column_pages: u64,
    links: Vec<(Vertex, Vertex, u64)>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    iteration: Option<iterative::Report>,
}

fn is_zero(value: &u64) -> bool {
    *value == 0
}

#[derive(Default)]
pub(in crate::admin::cluster) struct ArcBlocks(std::sync::Mutex<ArcBlockState>);

#[derive(Default)]
struct ArcBlockState {
    generations: VecDeque<(Uuid, GenerationBlocks)>,
}

#[derive(Default)]
struct GenerationBlocks {
    used: usize,
    blocks: BTreeMap<Key, ([u8; 32], bytes::Bytes)>,
}

impl GenerationBlocks {
    fn trim(&mut self, limit: usize) {
        while self.used > limit {
            let (key, (_, data)) = self.blocks.pop_last().expect("cached block");
            self.used -= data.len() + key.as_ref().len() + 160;
        }
    }
}

impl ArcBlocks {
    async fn read(&self, graph: &Graph<'_>, key: &Key, block: &Block) -> Result<bytes::Bytes> {
        let generation = graph.manifest.generation;
        tokio::task::consume_budget().await;
        graph.scope.check()?;
        {
            let mut state = self.0.lock().unwrap();
            if let Some(index) = state
                .generations
                .iter()
                .position(|(id, _)| *id == generation)
            {
                let entry = state.generations.remove(index).unwrap();
                let cached = entry
                    .1
                    .blocks
                    .get(key)
                    .filter(|(sha, _)| *sha == block.sha256)
                    .map(|(_, data)| data.clone());
                state.generations.push_back(entry);
                if let Some(data) = cached {
                    return Ok(data);
                }
            }
        }
        let data = read_block(graph.objects, key, block, graph.scope).await?;
        let capacity = (graph.scope.options.memory_bytes / 8).min(8 * 1024 * 1024);
        let mut state = self.0.lock().unwrap();
        if !state.generations.iter().any(|(id, _)| *id == generation) {
            if state.generations.len() == 2 {
                state.generations.pop_front();
            }
            state
                .generations
                .push_back((generation, GenerationBlocks::default()));
        }
        let limit = capacity / state.generations.len();
        for (_, blocks) in &mut state.generations {
            blocks.trim(limit);
        }
        let (_, blocks) = state
            .generations
            .iter_mut()
            .find(|(id, _)| *id == generation)
            .unwrap();
        // A stable subset is useful on each iteration even when a partition
        // is larger than the cache. Evicting on sequential misses would thrash.
        let bytes = data.len() + key.as_ref().len() + 160;
        if blocks.used + bytes <= limit && !blocks.blocks.contains_key(key) {
            blocks
                .blocks
                .insert(key.clone(), (block.sha256, data.clone()));
            blocks.used += bytes;
        }
        Ok(data)
    }
}

// All keys include the published generation. Keep a small cache per query so
// the path stream, separate hydration expressions and graph preview can reuse it.
#[derive(Default)]
struct BlockCache {
    hot: std::sync::Mutex<VecDeque<(Key, Arc<CachedBlock>)>>,
    spill: std::sync::Mutex<Option<BlockSpill>>,
}

struct BlockSpill {
    lookup: BTreeMap<Key, (u64, Option<usize>)>,
    size: u64,
    reader: std::fs::File,
    writer: Box<dyn datafusion::execution::spill_file::SpillWriter>,
    _file: Arc<dyn datafusion::execution::spill_file::SpillFile>,
    index_memory: MemoryReservation,
}

impl BlockCache {
    fn spill_entry(&self, key: Key, data: Arc<CachedBlock>, scope: &QueryScope) {
        let len = data.bytes.len();
        let limit = (scope.options.spill_bytes / 8).min(64 * 1024 * 1024);
        if len as u64 > limit {
            return;
        }
        // Check if already spilled before compressing (lock-free compression).
        {
            let guard = self.spill.lock().unwrap();
            if guard
                .as_ref()
                .is_some_and(|spill| spill.lookup.contains_key(&key))
            {
                return;
            }
        }
        crate::blocking(|| {
            let compression_memory = data._memory.new_empty();
            let compressed =
                if data.compress_spill && compression_memory.try_grow(len + 1024).is_ok() {
                    zstd::bulk::compress(&data.bytes, 1)
                        .ok()
                        .filter(|bytes| bytes.len() < len)
                } else {
                    None
                };
            let stored = compressed.as_deref().unwrap_or(&data.bytes);
            let mut guard = self.spill.lock().unwrap();
            let spilled = (|| -> Result<()> {
                if guard.is_none() {
                    let file = scope
                        .runtime
                        .disk_manager
                        .create_tmp_file("CSR lookup cache")?;
                    *guard = Some(BlockSpill {
                        lookup: BTreeMap::new(),
                        size: 0,
                        reader: std::fs::File::open(file.path().context("CSR lookup spill path")?)?,
                        writer: file.open_writer()?,
                        _file: file,
                        index_memory: MemoryConsumer::new("CSR lookup spill index")
                            .register(&scope.runtime.memory_pool),
                    });
                }
                let spill = guard.as_mut().unwrap();
                if !spill.lookup.contains_key(&key) {
                    if spill.size + stored.len() as u64 > limit {
                        return Ok(());
                    }
                    if spill
                        .index_memory
                        .try_grow(key.as_ref().len() + 96)
                        .is_err()
                    {
                        return Ok(());
                    }
                    std::io::Write::write_all(&mut spill.writer, stored)?;
                    spill
                        .lookup
                        .insert(key, (spill.size, compressed.as_ref().map(Vec::len)));
                    spill.size += stored.len() as u64;
                    scope.reads.snapshot_spill(stored.len() as u64);
                }
                Ok(())
            })();
            if spilled.is_err() {
                *guard = None;
            }
        });
    }

    fn read_spill(
        &self,
        key: &Key,
        block: &Block,
        memory: &MemoryReservation,
    ) -> Option<bytes::Bytes> {
        use std::io::{Read, Seek, SeekFrom};
        let compressed = memory.new_empty();
        let (encoded, raw_disk) = crate::blocking(|| {
            let mut guard = self.spill.lock().unwrap();
            let spill = guard.as_mut()?;
            let (offset, encoded) = *spill.lookup.get(key)?;
            compressed.try_grow(encoded.unwrap_or(0)).ok()?;
            let mut bytes = vec![0; encoded.unwrap_or(block.decoded_size())];
            if spill
                .reader
                .seek(SeekFrom::Start(offset))
                .and_then(|_| spill.reader.read_exact(&mut bytes))
                .is_err()
            {
                *guard = None;
                return None;
            }
            Some((encoded, bytes))
        })?;
        // Decompress and verify SHA-256 outside any mutex lock.
        let decoded = crate::blocking(|| {
            let raw = match encoded {
                Some(_) => zstd::bulk::decompress(&raw_disk, block.decoded_size()).ok(),
                None => Some(raw_disk),
            };
            if let Some(raw) = raw
                && raw.len() == block.decoded_size()
                && Sha256::digest(&raw).as_slice() == block.sha256
            {
                Some(bytes::Bytes::from(raw))
            } else {
                None
            }
        });
        if decoded.is_none() {
            *self.spill.lock().unwrap() = None;
        }
        decoded
    }
}

#[cfg(test)]
#[tokio::test]
async fn block_cache_reclaims_only_the_memory_needed_for_another_block() -> Result<()> {
    let admission = Arc::new(crate::engine::Admission::new(
        crate::EngineLimits::default(),
    )?);
    let scope = QueryScope::new(
        QueryOptions {
            memory_bytes: 1024 * 1024,
            ..Default::default()
        },
        &admission,
    )
    .await?;
    let cache = BlockCache::default();
    let bytes = bytes::Bytes::from(vec![7; 16 * 1024]);
    let block = Block {
        bytes: bytes.len() as u64,
        decoded_bytes: None,
        sha256: Sha256::digest(&bytes).into(),
        first: 0,
        last: 0,
        ids: None,
        numbers: Default::default(),
        columns: vec![],
    };
    for name in ["a", "b", "c"] {
        cache
            .read_with(Key::from(name), &block, &scope, async { Ok(bytes.clone()) })
            .await?;
    }
    let pressure =
        MemoryConsumer::new("test concurrent query state").register(&scope.runtime.memory_pool);
    pressure.try_grow(scope.options.memory_bytes - 3 * bytes.len() - 4096)?;
    let read = cache
        .read_with(Key::from("d"), &block, &scope, async { Ok(bytes.clone()) })
        .await?;
    assert_eq!(read.bytes, bytes);
    let hot = cache.hot.lock().unwrap();
    assert_eq!(
        hot.iter().map(|(key, _)| key.as_ref()).collect::<Vec<_>>(),
        vec!["b", "c", "d"]
    );
    Ok(())
}

struct CachedBlock {
    bytes: bytes::Bytes,
    record_offsets: OnceLock<Vec<(u64, usize)>>,
    _memory: MemoryReservation,
    compress_spill: bool,
}

impl CachedBlock {
    fn record(&self, id: u64) -> Option<&[u8]> {
        // WCC representatives can be interleaved across the whole block.
        // Index large identity blocks on demand, keeping offsets with the cached bytes.
        const STRIDE: usize = 4096;
        let offsets = self.record_offsets.get_or_init(|| {
            let capacity = self.bytes.len() / STRIDE;
            if capacity == 0
                || self
                    ._memory
                    .try_grow(capacity * size_of::<(u64, usize)>())
                    .is_err()
            {
                return Vec::new();
            }
            let mut index = Vec::with_capacity(capacity);
            let mut offset = 0;
            let mut last = 0;
            for (key, value) in records(&self.bytes) {
                if offset - last >= STRIDE {
                    index.push((key, offset));
                    last = offset;
                }
                offset += 12 + value.len();
            }
            index
        });
        let end = offsets.partition_point(|(key, _)| *key <= id);
        let start = offsets[..end].last().map_or(0, |(_, offset)| *offset);
        records(&self.bytes[start..])
            .find(|(key, _)| *key >= id)
            .filter(|(key, _)| *key == id)
            .map(|(_, value)| value)
    }
}

impl BlockCache {
    fn capacity(&self, scope: &QueryScope) -> usize {
        (scope.options.memory_bytes / 12).min(8 * 1024 * 1024)
    }

    async fn read(
        &self,
        objects: &Objects,
        key: Key,
        block: &Block,
        scope: &QueryScope,
    ) -> Result<Arc<CachedBlock>> {
        let fetch = read_block(objects, &key, block, scope);
        self.read_with(key.clone(), block, scope, fetch).await
    }

    async fn read_with(
        &self,
        key: Key,
        block: &Block,
        scope: &QueryScope,
        fetch: impl std::future::Future<Output = Result<bytes::Bytes>>,
    ) -> Result<Arc<CachedBlock>> {
        // Cache hits can form a long synchronous run while decoding many paths.
        // Keep the job heartbeat, cancellation and socket tasks runnable.
        tokio::task::consume_budget().await;
        scope.check()?;
        let memory = MemoryConsumer::new("CSR property and identity cache")
            .register(&scope.runtime.memory_pool);
        let limit = self.capacity(scope);
        let size = block.decoded_size();
        let keep = loop {
            scope.check()?;
            let evicted = {
                let mut hot = self.hot.lock().unwrap();
                if let Some(pos) = hot.iter().position(|(candidate, _)| *candidate == key) {
                    let entry = hot.remove(pos).unwrap();
                    let value = entry.1.clone();
                    hot.push_back(entry);
                    return Ok(value);
                }
                // Leave room for decoding and ordinary execution.
                if size > limit {
                    break false;
                }
                let used = hot
                    .iter()
                    .map(|(_, value)| value.bytes.len())
                    .sum::<usize>();
                if used.saturating_add(size) <= limit && memory.try_grow(size).is_ok() {
                    break true;
                }
                hot.pop_front()
            };
            let Some((evicted_key, evicted_data)) = evicted else {
                break false;
            };
            // Release this block's reservation before retrying the allocation.
            self.spill_entry(evicted_key, evicted_data, scope);
        };
        if !keep {
            memory.try_grow(block.decoded_size())?;
        }
        let cached = self.read_spill(&key, block, &memory);
        let bytes = match cached {
            Some(bytes) => bytes,
            None => fetch.await?,
        };
        let value = Arc::new(CachedBlock {
            bytes,
            record_offsets: OnceLock::new(),
            _memory: memory,
            compress_spill: block.decoded_bytes.is_some() || block.ids.is_some(),
        });
        if keep {
            let mut post_evicted = Vec::new();
            {
                let mut hot = self.hot.lock().unwrap();
                let limit = self.capacity(scope);
                let mut used = hot
                    .iter()
                    .map(|(_, block)| block.bytes.len())
                    .sum::<usize>();
                // Other expressions may have filled the cache while the request was in flight.
                while used + value.bytes.len() > limit {
                    let Some(entry) = hot.pop_front() else {
                        break;
                    };
                    used -= entry.1.bytes.len();
                    post_evicted.push(entry);
                }
                hot.retain(|(candidate, _)| *candidate != key);
                hot.push_back((key, value.clone()));
            }
            for (evicted_key, evicted_data) in post_evicted {
                self.spill_entry(evicted_key, evicted_data, scope);
            }
        }
        Ok(value)
    }
}

fn identity_block(blocks: &[Block], id: u64) -> Option<usize> {
    let index = blocks.partition_point(|block| block.last < id);
    blocks.get(index).filter(|block| block.first <= id)?;
    Some(index)
}

struct Graph<'a> {
    objects: &'a Objects,
    manifest: Arc<Manifest>,
    prefix: Key,
    scope: &'a QueryScope,
    budget: MemoryReservation,
    types: BTreeSet<u32>,
    direction: Direction,
    arc_cache: &'a ArcBlocks,
    identity_cache: BlockCache,
}

impl Graph<'_> {
    fn route(&self, endpoint: &Endpoint) -> Result<Vertex> {
        let schema = decode_schema(
            self.manifest
                .schemas
                .get(&endpoint.label)
                .context("unknown CSR node label")?,
        )?;
        let key = endpoint
            .key
            .iter()
            .map(|value| ScalarValue::Utf8(Some(value.clone())))
            .collect::<Vec<_>>();
        let key = graph_encode_key(&key, &schema, &graph_row_converter(&schema)?)?;
        Ok(vertex_id(&endpoint.label, &key))
    }

    async fn vertex(&self, id: Vertex) -> Result<Identity> {
        let shard = partition(id, self.manifest.partitions.len());
        let bytes = self
            .record(
                &self.manifest.partitions[shard].vertices,
                &format!("v-{shard}"),
                id,
            )
            .await?;
        Ok(serde_json::from_slice(vertex_record(&bytes)?.0)?)
    }

    async fn edge(&self, id: u64) -> Result<serde_json::Value> {
        let bytes = self.record(&self.manifest.edge_keys, "edges", id).await?;
        Ok(serde_json::from_slice(
            bytes.split(|&b| b == b'\n').next().unwrap_or_default(),
        )?)
    }

    async fn record(&self, blocks: &[Block], path: &str, id: u64) -> Result<Vec<u8>> {
        if let Some(index) = identity_block(blocks, id) {
            let key = self.prefix.clone().join(path).join(index.to_string());
            let block = self
                .identity_cache
                .read(self.objects, key, &blocks[index], self.scope)
                .await?;
            if let Some(value) = block.record(id) {
                return Ok(value.to_vec());
            }
        }
        anyhow::bail!("vertex or edge not found in the CSR generation")
    }

    async fn vertices(
        &self,
        shard: usize,
        indices: Option<&[usize]>,
        mut visit: impl FnMut(Vertex, &[u8]) -> Result<()>,
    ) -> Result<()> {
        let prefix = self.prefix.clone().join(format!("v-{shard}"));
        let blocks = &self.manifest.partitions[shard].vertices;
        for offset in 0..indices.map_or(blocks.len(), <[usize]>::len) {
            let index = indices.map_or(offset, |selected| selected[offset]);
            let block = &blocks[index];
            let key = prefix.clone().join(index.to_string());
            let memory = self.budget.new_empty();
            let cached;
            let data;
            let bytes = if indices.is_some() {
                cached = self
                    .identity_cache
                    .read(self.objects, key, block, self.scope)
                    .await?;
                &cached.bytes
            } else {
                memory.try_grow(block.decoded_size())?;
                data = read_block(self.objects, &key, block, self.scope).await?;
                &data
            };
            for (id, bytes) in records(bytes) {
                visit(id, bytes)?;
            }
        }
        Ok(())
    }

    async fn arcs(
        &self,
        shard: usize,
        frontier: Option<&BTreeSet<Vertex>>,
        visit: impl FnMut(ArcEntry) -> Result<()>,
    ) -> Result<()> {
        self.arc_blocks(
            shard,
            frontier,
            0..self.manifest.partitions[shard].arcs.len(),
            visit,
        )
        .await
    }

    async fn arc_blocks(
        &self,
        shard: usize,
        frontier: Option<&BTreeSet<Vertex>>,
        range: std::ops::Range<usize>,
        mut visit: impl FnMut(ArcEntry) -> Result<()>,
    ) -> Result<()> {
        let prefix = self.prefix.clone().join(format!("a-{shard}"));
        for (index, block) in self.manifest.partitions[shard]
            .arcs
            .iter()
            .enumerate()
            .skip(range.start)
            .take(range.len())
        {
            if frontier.is_some_and(|ids| ids.range(block.first..=block.last).next().is_none()) {
                continue;
            }
            let memory = self.budget.new_empty();
            memory.try_grow(block.decoded_size())?;
            let key = prefix.clone().join(index.to_string());
            let data = self.arc_cache.read(self, &key, block).await?;
            ensure!(data.len() >= 4, "invalid CSR block");
            let rows = u32_at(&data, 0) as usize;
            let adjacency = 4 + rows * 12;
            ensure!(
                adjacency <= data.len() && (data.len() - adjacency).is_multiple_of(24),
                "invalid CSR offsets"
            );
            let count = (data.len() - adjacency) / 24;
            for i in 0..rows {
                let base = 4 + i * 12;
                let from = u64_at(&data, base);
                if frontier.is_some_and(|ids| !ids.contains(&from)) {
                    continue;
                }
                let start = u32_at(&data, base + 8) as usize;
                let end = if i + 1 == rows {
                    count
                } else {
                    u32_at(&data, base + 20) as usize
                };
                ensure!(start <= end && end <= count, "invalid CSR row offsets");
                for j in start..end {
                    let offset = adjacency + j * 24;
                    let to = u64_at(&data, offset);
                    let kind = u32_at(&data, offset + 16);
                    let reverse = u32_at(&data, offset + 20) != 0;
                    if (!self.types.is_empty() && !self.types.contains(&kind))
                        || match self.direction {
                            Direction::Outgoing => reverse,
                            Direction::Incoming => !reverse,
                            Direction::Undirected => reverse && from == to,
                        }
                    {
                        continue;
                    }
                    visit(ArcEntry {
                        from,
                        to,
                        kind,
                        reverse,
                        edge: u64_at(&data, offset + 8),
                    })?;
                }
            }
        }
        Ok(())
    }
}

fn u64_at(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(
        bytes[offset..offset + 8]
            .try_into()
            .expect("checked CSR width"),
    )
}

fn u32_at(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(
        bytes[offset..offset + 4]
            .try_into()
            .expect("checked CSR width"),
    )
}

fn records(mut bytes: &[u8]) -> impl Iterator<Item = (u64, &[u8])> {
    std::iter::from_fn(move || {
        if bytes.len() < 12 {
            return None;
        }
        let id = u64_at(bytes, 0);
        let len = u32_at(bytes, 8) as usize;
        let value = bytes.get(12..12 + len)?;
        bytes = &bytes[12 + len..];
        Some((id, value))
    })
}

fn vertex_record(bytes: &[u8]) -> Result<(&[u8], &[u8])> {
    let index = bytes
        .iter()
        .position(|b| *b == b'\n')
        .context("invalid CSR vertex record")?;
    Ok((&bytes[..index], &bytes[index + 1..]))
}

pub(in crate::admin::cluster) async fn run_task(
    State(worker): State<Arc<Worker>>,
    headers: HeaderMap,
    Json(task): Json<Task>,
) -> ApiResult<StatusCode> {
    let started = tokio::time::Instant::now();
    let saved = worker
        .shared
        .metadata
        .get(task.id)
        .await
        .map_err(storage)?
        .filter(|saved| {
            saved.job.attempt == task.attempt
                && saved.job.csr_generation == Some(task.generation)
                && matches!(saved.job.state, JobState::Running)
        })
        .ok_or_else(super::super::cancelled)?;
    let job = saved.job;
    if job.worker == Some(worker.id) {
        return Err(Error::new(
            StatusCode::CONFLICT,
            "stage_leader",
            "分区交给其他节点",
        ));
    }
    let _slot = worker
        .stage_slots
        .clone()
        .try_acquire_owned()
        .map_err(|_| Error::busy())?;
    let deadline = work_deadline(&headers, started, job.deadline_ms)?;
    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
    if remaining.is_zero() {
        return Err(super::super::timeout());
    }
    let scope = Arc::new(
        QueryScope::new(
            QueryOptions {
                timeout: remaining,
                memory_bytes: worker.memory_bytes,
                cancellation: worker.shutdown.child_token(),
                ..Default::default()
            },
            &worker.admission,
        )
        .await
        .map_err(query::query_error)?,
    );
    let objects = &worker.shared.objects;
    let operation = async {
        let manifest = manifest(objects, &job.model, &job.revision, task.generation).await?;
        let graph = Graph {
            prefix: objects.graph(&job.model, &job.revision, task.generation),
            objects,
            manifest: manifest,
            budget: MemoryConsumer::new("GraphLake CSR worker")
                .register(&scope.runtime.memory_pool),
            scope: &scope,
            direction: task.direction,
            types: task.types,
            arc_cache: &worker.arc_cache,
            identity_cache: BlockCache::default(),
        };
        ensure!(
            task.shard < graph.manifest.partitions.len(),
            "invalid CSR partition"
        );
        let input = objects.stage(task.id, task.attempt, task.input, "csr-input");
        let step: Step = objects
            .get_json(&input, (worker.memory_bytes / 6) as u64)
            .await?;
        let output = partition_step(
            &graph,
            task.shard,
            &step,
            task.id,
            task.attempt,
            task.output,
        )
        .await?;
        let output = Output {
            worker: Some(worker.id),
            read_bytes: scope.reads.metrics().object_bytes,
            peak_memory: scope.peak_reserved_bytes(),
            ..output
        };
        let bytes = serde_json::to_vec(&output)?;
        ensure!(
            bytes.len() <= worker.memory_bytes / 6,
            "CSR partition output exceeds the query budget"
        );
        ensure!(
            worker
                .shared
                .metadata
                .get(task.id)
                .await?
                .is_some_and(|saved| saved.job.attempt == task.attempt
                    && matches!(saved.job.state, JobState::Running)),
            "query cancelled"
        );
        objects
            .put_bytes(
                &objects.stage(task.id, task.attempt, task.output, "csr-output"),
                bytes,
            )
            .await?;
        Ok(())
    };
    let monitor = async {
        let mut errors = 0u32;
        loop {
            tokio::time::sleep(super::super::WAIT).await;
            match worker.shared.metadata.get(task.id).await {
                Ok(Some(saved))
                    if saved.job.attempt == task.attempt
                        && matches!(saved.job.state, JobState::Running) =>
                {
                    errors = 0;
                }
                Ok(_) => anyhow::bail!("query cancelled"),
                Err(error) => {
                    errors += 1;
                    if errors >= 6 {
                        return Err(error);
                    }
                }
            }
        }
    };
    tokio::select! {
        result = scope.run(operation) => result,
        result = monitor => result,
    }
    .map_err(query::query_error)?;
    Ok(StatusCode::NO_CONTENT)
}

struct Execution<'a> {
    worker: &'a Worker,
    job: &'a Job,
    id: Uuid,
    graph: Graph<'a>,
    stats: Arc<std::sync::Mutex<Stats>>,
    local: tokio::sync::Semaphore,
    unavailable: std::sync::Mutex<BTreeSet<usize>>,
}

struct Round {
    outputs: Vec<Output>,
    _budget: MemoryReservation,
}

#[derive(Default, Clone, Serialize)]
pub(in crate::admin) struct Stats {
    rounds: usize,
    remote_partitions: usize,
    workers: BTreeSet<Uuid>,
    object_bytes: u64,
    exchange_bytes: u64,
    retries: usize,
    peak_worker_bytes: usize,
    peak_leader_bytes: usize,
    state_exchange_bytes: u64,
    filtered_arcs: u64,
    #[serde(skip_serializing_if = "is_zero")]
    skipped_property_blocks: u64,
    #[serde(skip_serializing_if = "is_zero")]
    filter_column_pages: u64,
    #[serde(skip_serializing_if = "is_zero")]
    lookup_spilled_bytes: u64,
    #[serde(skip_serializing_if = "is_zero")]
    sort_column_bytes: u64,
    #[serde(skip_serializing_if = "is_zero")]
    hydration_column_bytes: u64,
}

impl<'a> Execution<'a> {
    fn new(
        worker: &'a Worker,
        job: &'a Job,
        id: Uuid,
        manifest: Arc<Manifest>,
        scope: &'a QueryScope,
        types: BTreeSet<u32>,
        direction: Direction,
        stats: Arc<std::sync::Mutex<Stats>>,
    ) -> Self {
        let objects = &worker.shared.objects;
        let prefix = objects.graph(&job.model, &job.revision, manifest.generation);
        Self {
            worker,
            job,
            id,
            graph: Graph {
                objects,
                manifest,
                prefix,
                scope,
                types,
                direction,
                arc_cache: &worker.arc_cache,
                identity_cache: BlockCache::default(),
                budget: MemoryConsumer::new("GraphLake CSR execution")
                    .register(&scope.runtime.memory_pool),
            },
            stats,
            local: tokio::sync::Semaphore::new(1),
            unavailable: Default::default(),
        }
    }
    async fn round(&self, inputs: Vec<(usize, Step)>) -> Result<Round> {
        self.run_round(inputs, false).await
    }

    async fn run_round(&self, inputs: Vec<(usize, Step)>, local: bool) -> Result<Round> {
        let local = local
            || self.worker.workers.len() == 1
            || self
                .graph
                .manifest
                .vertices
                .saturating_add(self.graph.manifest.edges)
                < self.worker.distributed_min_rows as u64;
        let mut tasks = futures::stream::iter(inputs.into_iter().enumerate().map(
            |(seq, (shard, input))| async move {
                self.graph.scope.check()?;
                let task_id = Uuid::new_v4();
                if local {
                    let _slot = self.local.acquire().await?;
                    let output = partition_step(
                        &self.graph,
                        shard,
                        &input,
                        self.id,
                        self.job.attempt,
                        task_id,
                    )
                    .await?;
                    return Ok((seq, output));
                }
                let objects = self.graph.objects;
                let key = objects.stage(self.id, self.job.attempt, task_id, "csr-input");
                let bytes = serde_json::to_vec(&input)?;
                ensure!(
                    bytes.len() <= self.worker.memory_bytes / 6,
                    "CSR frontier or algorithm state exceeds the query budget"
                );
                self.stats.lock().unwrap().exchange_bytes += bytes.len() as u64;
                objects.put_bytes(&key, bytes).await?;
                let route = match &input {
                    Step::Arcs { start_block, .. } => shard.wrapping_add(*start_block / 2),
                    _ => shard,
                };
                let output = self.send(shard, route, task_id, &input).await;
                objects.delete(&key).await;
                Ok::<_, anyhow::Error>((seq, output?))
            },
        ))
        .buffer_unordered(self.worker.workers.len().saturating_sub(1).max(1));
        let pending = self.graph.budget.new_empty();
        let mut output = Vec::new();
        while let Some(item) = tasks.try_next().await? {
            pending.try_grow(item.1.links.len().saturating_mul(48))?;
            let mut stats = self.stats.lock().unwrap();
            stats.state_exchange_bytes += item.1.shuffled_bytes;
            stats.filtered_arcs += item.1.filtered_arcs;
            stats.skipped_property_blocks += item.1.skipped_property_blocks;
            stats.filter_column_pages += item.1.filter_column_pages;
            ensure!(
                stats.state_exchange_bytes <= self.graph.scope.options.spill_bytes,
                "distributed shuffle exceeds query spill budget"
            );
            if let Some(worker) = item.1.worker {
                stats.workers.insert(worker);
                stats.remote_partitions += 1;
                stats.object_bytes += item.1.read_bytes;
                stats.peak_worker_bytes = stats.peak_worker_bytes.max(item.1.peak_memory);
            }
            output.push(item);
        }
        self.stats.lock().unwrap().rounds += 1;
        output.sort_unstable_by_key(|(seq, _)| *seq);
        Ok(Round {
            outputs: output.into_iter().map(|(_, output)| output).collect(),
            _budget: pending,
        })
    }

    async fn send(&self, shard: usize, route: usize, input: Uuid, step: &Step) -> Result<Output> {
        let peers = &self.worker.workers;
        for offset in 0..peers.len() {
            let index = (route + offset) % peers.len();
            let remaining_available = {
                let unavailable = self.unavailable.lock().unwrap();
                if unavailable.contains(&index) {
                    continue;
                }
                (offset..peers.len())
                    .filter(|step| !unavailable.contains(&((route + step) % peers.len())))
                    .count()
            };
            let endpoint = &peers[index];
            let output = Uuid::new_v4();
            // Divide remaining budget only across remaining *available* peers
            // (plus local fallback), capped at 3 so skewed shards keep realistic time.
            let remaining = self.graph.scope.remaining()?;
            let attempts = u32::try_from((remaining_available + 1).min(3)).unwrap_or(3);
            let attempt = remaining / attempts.max(1);
            let millis = attempt.as_millis();
            if millis == 0 {
                break;
            }
            let response = self
                .worker
                .shared
                .client
                .post(endpoint.join("/internal/v1/csr")?)
                .headers(crate::admin::telemetry::trace_headers())
                .bearer_auth(&self.worker.shared.token)
                .timeout(attempt)
                .header(WORK_BUDGET_HEADER, millis.to_string())
                .json(&Task {
                    id: self.id,
                    attempt: self.job.attempt,
                    generation: self.graph.manifest.generation,
                    shard,
                    direction: self.graph.direction,
                    types: self.graph.types.clone(),
                    input,
                    output,
                })
                .send()
                .await;
            let key = self
                .graph
                .objects
                .stage(self.id, self.job.attempt, output, "csr-output");
            if response.as_ref().is_ok_and(|r| r.status().is_success()) {
                let data = self
                    .graph
                    .objects
                    .get_json_with_size(&key, (self.worker.memory_bytes / 6) as u64)
                    .await;
                self.graph.objects.delete(&key).await;
                if let Ok((data, bytes_len)) = data {
                    self.stats.lock().unwrap().exchange_bytes += bytes_len;
                    return Ok(data);
                }
            } else if response.as_ref().is_ok_and(|r| {
                matches!(
                    r.status(),
                    StatusCode::CONFLICT | StatusCode::TOO_MANY_REQUESTS
                )
            }) {
                let response = response.unwrap();
                let status = response.status();
                if status == StatusCode::CONFLICT {
                    let body = response
                        .json::<serde_json::Value>()
                        .await
                        .unwrap_or_default();
                    if body["code"].as_str() == Some("cancelled") {
                        anyhow::bail!("query cancelled");
                    }
                    self.unavailable.lock().unwrap().insert(index);
                }
                continue;
            } else if response.as_ref().is_err_and(|error| error.is_connect())
                || response
                    .as_ref()
                    .is_ok_and(|r| r.status() == StatusCode::BAD_GATEWAY)
            {
                self.unavailable.lock().unwrap().insert(index);
            } else if let Ok(response) = response
                && matches!(
                    response.status(),
                    StatusCode::UNPROCESSABLE_ENTITY | StatusCode::INSUFFICIENT_STORAGE
                )
            {
                let status = response.status();
                let body = response
                    .json::<serde_json::Value>()
                    .await
                    .unwrap_or_default();
                anyhow::bail!(
                    "CSR partition failed ({status}): {}",
                    body["message"].as_str().unwrap_or("execution failed")
                );
            }
            if matches!(step, Step::Iteration(_)) {
                self.graph
                    .objects
                    .clean_stage(self.id, self.job.attempt, output)
                    .await;
            }
            self.stats.lock().unwrap().retries += 1;
        }
        // The original generation is in shared storage and can be retried locally.
        let _local = self.local.acquire().await?;
        partition_step(
            &self.graph,
            shard,
            step,
            self.id,
            self.job.attempt,
            Uuid::new_v4(),
        )
        .await
    }
}

async fn partition_step(
    graph: &Graph<'_>,
    shard: usize,
    step: &Step,
    id: Uuid,
    attempt: Uuid,
    output: Uuid,
) -> Result<Output> {
    if let Step::Iteration(step) = step {
        let report = iterative::partition_step(graph, shard, id, attempt, step, output).await?;
        return Ok(Output {
            shuffled_bytes: report.bytes,
            iteration: Some(report),
            ..Default::default()
        });
    }
    algorithms::partition_step(graph, shard, step).await
}

pub(in crate::admin::cluster) async fn query_csr(
    worker: &Worker,
    engine: &Engine,
    job: &Job,
    id: Uuid,
    request: &Request,
    mut options: QueryOptions,
) -> ApiResult<(QueryResult, Stats)> {
    let objects = &worker.shared.objects;
    let manifest = manifest(objects, &job.model, &job.revision, request.generation)
        .await
        .map_err(storage)?;
    options.graph = manifest.graph_options.clone();
    let scope = engine.scope(options).await.map_err(query::query_error)?;
    let mut types = BTreeSet::new();
    for name in &request.edge_types {
        let index = manifest
            .edge_types
            .binary_search(name)
            .map_err(|_| Error::invalid("invalid_csr_scope", "CSR 代次中没有此边类型"))?;
        types.insert(index as u32);
    }
    let result = scope
        .run(async {
            let direction = if matches!(request.operation, Operation::Wcc { .. }) {
                Direction::Undirected
            } else {
                request.direction
            };
            let execution = Execution::new(
                worker,
                job,
                id,
                manifest,
                &scope,
                types,
                direction,
                Default::default(),
            );
            let versions = execution.graph.manifest.source_versions();
            let prepared = Prepared {
                aggregate: None,
                versions,
                reservations: Vec::new(),
                postgres: BTreeMap::new(),
            };
            *scope.csr_generation.lock().unwrap() = Some(request.generation.to_string());
            let context = scope.session();
            let (table, reservation) = algorithms::run(&execution, &request.operation).await?;
            let mut prepared = prepared;
            prepared.reservations.push(reservation);
            let frame = context.read_table(table)?;
            let result = scope
                .result(&engine.catalog.graph, &context, frame, prepared, None)
                .await?;
            let mut stats = execution.stats.lock().unwrap().clone();
            stats.workers.insert(worker.id);
            stats.object_bytes += scope.reads.metrics().object_bytes;
            stats.peak_leader_bytes = scope.peak_reserved_bytes();
            Ok((result, stats))
        })
        .await;
    result.map_err(query::query_error)
}
