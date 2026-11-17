use super::*;
use datafusion::execution::spill_file::{SpillFile, SpillWriter};
use std::io::Write;

#[derive(Clone, Copy, Serialize, Deserialize)]
pub(super) enum Algorithm {
    Rank,
    Components,
}

#[derive(Clone, Serialize, Deserialize)]
pub(super) enum Step {
    Initialize {
        algorithm: Algorithm,
    },
    Scatter {
        algorithm: Algorithm,
        state: Uuid,
    },
    Apply {
        algorithm: Algorithm,
        state: Uuid,
        messages: Vec<Uuid>,
        base: f64,
        damping: f64,
    },
    Finish {
        algorithm: Algorithm,
        state: Uuid,
    },
}

#[derive(Default, Deserialize, Serialize)]
pub(super) struct Report {
    output: Uuid,
    dangling: f64,
    delta: f64,
    changed: bool,
    files: Vec<usize>,
    pub(super) bytes: u64,
}

#[derive(Clone, Copy)]
struct Value {
    number: u64,
    aux: u64,
}
type State = BTreeMap<Vertex, Value>;

fn object(graph: &Graph<'_>, id: Uuid, attempt: Uuid, output: Uuid, name: &str) -> Key {
    graph.objects.stage(id, attempt, output, name)
}

async fn visit_records(
    graph: &Graph<'_>,
    key: &Key,
    mut visit: impl FnMut(Vertex, Value) -> Result<()>,
) -> Result<()> {
    let result = graph.objects.get(key).await?;
    let size = result.meta.size;
    ensure!(size.is_multiple_of(24), "invalid CSR iteration object");
    let mut stream = result.into_stream();
    let budget = graph.budget.new_empty();
    let mut pending = Vec::new();
    let mut received = 0_u64;
    while let Some(bytes) = stream.try_next().await? {
        graph.scope.check()?;
        received += bytes.len() as u64;
        budget.try_resize(bytes.len().saturating_mul(2))?;
        let mut bytes = bytes.as_ref();
        if !pending.is_empty() {
            let len = bytes.len().min(24 - pending.len());
            pending.extend_from_slice(&bytes[..len]);
            bytes = &bytes[len..];
            if pending.len() == 24 {
                visit(
                    u64_at(&pending, 0),
                    Value {
                        number: u64_at(&pending, 8),
                        aux: u64_at(&pending, 16),
                    },
                )?;
                pending.clear();
            }
        }
        let mut chunks = bytes.chunks_exact(24);
        for data in &mut chunks {
            visit(
                u64_at(data, 0),
                Value {
                    number: u64_at(data, 8),
                    aux: u64_at(data, 16),
                },
            )?;
        }
        pending.extend_from_slice(chunks.remainder());
    }
    ensure!(
        pending.is_empty() && size == received,
        "incomplete CSR iteration object"
    );
    graph.scope.reads.object_bytes(received as usize);
    graph.scope.reads.request();
    Ok(())
}

fn append(writer: &mut impl Write, id: Vertex, value: Value) -> Result<()> {
    let mut bytes = [0; 24];
    bytes[0..8].copy_from_slice(&id.to_le_bytes());
    bytes[8..16].copy_from_slice(&value.number.to_le_bytes());
    bytes[16..24].copy_from_slice(&value.aux.to_le_bytes());
    Ok(writer.write_all(&bytes)?)
}

struct Spool {
    file: Arc<dyn SpillFile>,
    writer: Box<dyn SpillWriter>,
}

impl Spool {
    fn new(graph: &Graph<'_>) -> Result<Self> {
        let file = graph
            .scope
            .runtime
            .disk_manager
            .create_tmp_file("CSR algorithm exchange")?;
        Ok(Self {
            writer: file.open_writer()?,
            file,
        })
    }

    async fn upload(mut self, graph: &Graph<'_>, key: &Key) -> Result<u64> {
        self.writer.finish()?;
        let size = self.file.size().context("CSR exchange file size")?;
        let budget = graph.budget.new_empty();
        budget.try_grow(size.min(5 * 1024 * 1024) as usize)?;
        graph
            .objects
            .put_file(
                key,
                self.file.path().context("CSR exchange spill path")?,
                size,
            )
            .await?;
        Ok(size)
    }
}

async fn load(graph: &Graph<'_>, key: &Key, budget: &MemoryReservation) -> Result<State> {
    let mut state = State::new();
    visit_records(graph, key, |id, value| {
        budget.try_grow(64)?;
        ensure!(
            state.insert(id, value).is_none(),
            "duplicate CSR vertex state"
        );
        Ok(())
    })
    .await?;
    Ok(state)
}

pub(super) async fn partition_step(
    graph: &Graph<'_>,
    shard: usize,
    id: Uuid,
    attempt: Uuid,
    step: &Step,
    output: Uuid,
) -> Result<Report> {
    let mut report = Report {
        output,
        ..Default::default()
    };
    let key = |object_id, name: &str| object(graph, id, attempt, object_id, name);
    let budget = graph.budget.new_empty();
    match step {
        Step::Initialize { algorithm } => {
            let mut state = State::new();
            graph
                .vertices(shard, None, |id, bytes| {
                    budget.try_grow(64)?;
                    let value = match algorithm {
                        Algorithm::Rank => Value {
                            number: (1.0 / graph.manifest.vertices as f64).to_bits(),
                            aux: 0,
                        },
                        Algorithm::Components => {
                            let (_, raw) = vertex_record(bytes)?;
                            let offset = raw
                                .len()
                                .checked_sub(8)
                                .context("missing CSR vertex order")?;
                            Value {
                                number: u64_at(raw, offset),
                                aux: id,
                            }
                        }
                    };
                    state.insert(id, value);
                    Ok(())
                })
                .await?;
            if matches!(algorithm, Algorithm::Rank) {
                graph
                    .arcs(shard, None, |arc| {
                        state
                            .get_mut(&arc.from)
                            .context("missing CSR source vertex")?
                            .aux += 1;
                        Ok(())
                    })
                    .await?;
            }
            report.bytes += save(graph, &key(output, "state"), state).await?;
        }
        Step::Scatter { algorithm, state } => {
            let state = load(graph, &key(*state, "state"), &budget).await?;
            if matches!(algorithm, Algorithm::Rank) {
                report.dangling = state
                    .values()
                    .filter(|value| value.aux == 0)
                    .map(|value| f64::from_bits(value.number))
                    .sum();
            }
            let count = graph.manifest.partitions.len();
            let mut messages = State::new();
            let mut files = (0..count).map(|_| None::<Spool>).collect::<Vec<_>>();
            let memory = graph.budget.new_empty();
            graph
                .arcs(shard, None, |arc| {
                    let from = state.get(&arc.from).context("missing CSR source state")?;
                    let value = match algorithm {
                        Algorithm::Rank => Value {
                            number: (f64::from_bits(from.number) / from.aux as f64).to_bits(),
                            aux: 0,
                        },
                        Algorithm::Components => *from,
                    };
                    match messages.entry(arc.to) {
                        std::collections::btree_map::Entry::Vacant(entry) => {
                            memory.try_grow(64)?;
                            entry.insert(value);
                        }
                        std::collections::btree_map::Entry::Occupied(mut entry) => {
                            merge(entry.get_mut(), value, *algorithm)
                        }
                    }
                    if messages.len() >= 4096 {
                        flush(graph, &mut files, std::mem::take(&mut messages))?;
                        memory.free();
                    }
                    Ok(())
                })
                .await?;
            flush(graph, &mut files, messages)?;
            for (shard, spool) in files.into_iter().enumerate() {
                if let Some(spool) = spool {
                    report.bytes += spool
                        .upload(graph, &key(output, &format!("to-{shard}")))
                        .await?;
                    report.files.push(shard);
                }
            }
        }
        Step::Apply {
            algorithm,
            state,
            messages,
            base,
            damping,
        } => {
            let old = load(graph, &key(*state, "state"), &budget).await?;
            let mut next = State::new();
            budget.try_grow(old.len().saturating_mul(64))?;
            for (&id, &value) in &old {
                next.insert(
                    id,
                    match algorithm {
                        Algorithm::Rank => Value {
                            number: base.to_bits(),
                            aux: value.aux,
                        },
                        Algorithm::Components => value,
                    },
                );
            }
            for message in messages {
                visit_records(
                    graph,
                    &key(*message, &format!("to-{shard}")),
                    |id, mut score| {
                        if matches!(algorithm, Algorithm::Rank) {
                            score.number = (f64::from_bits(score.number) * damping).to_bits();
                        }
                        merge(
                            next.get_mut(&id).context("missing CSR destination state")?,
                            score,
                            *algorithm,
                        );
                        Ok(())
                    },
                )
                .await?;
            }
            for (id, current) in &next {
                let previous = old[id];
                report.changed |= previous.number != current.number;
                if matches!(algorithm, Algorithm::Rank) {
                    report.delta +=
                        (f64::from_bits(previous.number) - f64::from_bits(current.number)).abs();
                }
            }
            drop(old);
            report.bytes += save(graph, &key(output, "state"), next).await?;
        }
        Step::Finish { algorithm, state } => {
            let state = load(graph, &key(*state, "state"), &budget).await?;
            let schema = schema(*algorithm);
            let mut spool = Spool::new(graph)?;
            {
                let mut writer = datafusion::arrow::ipc::writer::StreamWriter::try_new(
                    &mut spool.writer,
                    &schema,
                )?;
                let mut rows = Vec::new();
                let identities = graph.budget.new_empty();
                let mut roots = BTreeMap::new();
                let mut blocks = (0..graph.manifest.partitions.len())
                    .map(|_| None)
                    .collect::<Vec<_>>();
                for (vertex, score) in state {
                    let identity = vertex_text(graph, &mut blocks, vertex).await?;
                    let component = if matches!(algorithm, Algorithm::Components) {
                        if let Some(value) = roots.get(&score.aux) {
                            Some(Arc::<str>::clone(value))
                        } else {
                            let root: Arc<str> =
                                Arc::from(vertex_text(graph, &mut blocks, score.aux).await?);
                            if roots.len() >= 256 {
                                roots.clear();
                            }
                            roots.insert(score.aux, root.clone());
                            Some(root)
                        }
                    } else {
                        None
                    };
                    identities.try_grow(
                        identity.len() + component.as_ref().map_or(0, |s| s.len()) + 96,
                    )?;
                    rows.push((identity, score, component));
                    if rows.len() == 128 {
                        writer.write(&batch(schema.clone(), *algorithm, &rows)?)?;
                        rows.clear();
                        identities.free();
                    }
                }
                if !rows.is_empty() {
                    writer.write(&batch(schema.clone(), *algorithm, &rows)?)?;
                }
                writer.finish()?;
            }
            report.bytes += spool.upload(graph, &key(output, "result")).await?;
        }
    }
    Ok(report)
}

fn flush(graph: &Graph<'_>, files: &mut [Option<Spool>], messages: State) -> Result<()> {
    for (id, value) in messages {
        let target = partition(id, files.len());
        if files[target].is_none() {
            files[target] = Some(Spool::new(graph)?);
        }
        append(
            &mut files[target].as_mut().expect("message spool").writer,
            id,
            value,
        )?;
    }
    Ok(())
}

fn merge(value: &mut Value, candidate: Value, algorithm: Algorithm) {
    match algorithm {
        Algorithm::Rank => {
            value.number =
                (f64::from_bits(value.number) + f64::from_bits(candidate.number)).to_bits()
        }
        Algorithm::Components if candidate.number < value.number => *value = candidate,
        Algorithm::Components => {}
    }
}

async fn save(graph: &Graph<'_>, key: &Key, state: State) -> Result<u64> {
    let mut spool = Spool::new(graph)?;
    for (id, value) in state {
        append(&mut spool.writer, id, value)?;
    }
    spool.upload(graph, key).await
}

fn schema(algorithm: Algorithm) -> Arc<Schema> {
    let (name, ty) = match algorithm {
        Algorithm::Rank => ("rank", DataType::Float64),
        Algorithm::Components => ("component", DataType::Utf8),
    };
    Arc::new(Schema::new(vec![
        Field::new("vertex", DataType::Utf8, false),
        Field::new(name, ty, false),
    ]))
}

fn batch(
    schema: Arc<Schema>,
    algorithm: Algorithm,
    rows: &[(String, Value, Option<Arc<str>>)],
) -> Result<RecordBatch> {
    let vertices = Arc::new(StringArray::from_iter_values(
        rows.iter().map(|(vertex, _, _)| vertex),
    ));
    let values: ArrayRef = match algorithm {
        Algorithm::Rank => Arc::new(Float64Array::from_iter_values(
            rows.iter()
                .map(|(_, score, _)| f64::from_bits(score.number)),
        )),
        Algorithm::Components => Arc::new(StringArray::from_iter_values(
            rows.iter()
                .map(|(_, _, root)| root.as_deref().expect("component identity")),
        )),
    };
    Ok(RecordBatch::try_new(schema, vec![vertices, values])?)
}

async fn vertex_text(
    graph: &Graph<'_>,
    cache: &mut [Option<(usize, CachedBlock)>],
    id: Vertex,
) -> Result<String> {
    let shard = partition(id, cache.len());
    let blocks = &graph.manifest.partitions[shard].vertices;
    let index = blocks.partition_point(|b| b.last < id);
    let block = blocks
        .get(index)
        .filter(|b| b.first <= id)
        .context("missing CSR identity block")?;
    if cache[shard].as_ref().is_none_or(|(i, _)| *i != index) {
        cache[shard] = None;
        let path = graph
            .prefix
            .clone()
            .join(format!("v-{shard}"))
            .join(index.to_string());
        let memory = graph.budget.new_empty();
        memory.try_grow(block.decoded_size())?;
        let bytes = read_block(graph.objects, &path, block, graph.scope).await?;
        cache[shard] = Some((
            index,
            CachedBlock {
                bytes,
                _memory: memory,
                compress_spill: false,
            },
        ));
    }
    let data = records(&cache[shard].as_ref().unwrap().1.bytes)
        .find(|(key, _)| *key == id)
        .map(|(_, value)| value)
        .context("missing CSR identity")?;
    Ok(std::str::from_utf8(vertex_record(data)?.0)?.to_owned())
}

pub(super) async fn run(
    execution: &Execution<'_>,
    algorithm: Algorithm,
    max_iterations: usize,
    damping: f64,
    tolerance: f64,
) -> Result<(
    Arc<dyn datafusion::catalog::TableProvider>,
    MemoryReservation,
)> {
    let graph = &execution.graph;
    let n = graph.manifest.partitions.len();
    let mut states = round(
        execution,
        (0..n).map(|shard| (shard, Step::Initialize { algorithm })),
    )
    .await?;
    let mut converged = false;
    for _ in 0..max_iterations {
        let sent = round(
            execution,
            states.iter().enumerate().map(|(shard, state)| {
                (
                    shard,
                    Step::Scatter {
                        algorithm,
                        state: state.output,
                    },
                )
            }),
        )
        .await?;
        let dangling: f64 = sent.iter().map(|r| r.dangling).sum();
        let base = (1.0 - damping + damping * dangling) / graph.manifest.vertices.max(1) as f64;
        let next = round(
            execution,
            states.iter().enumerate().map(|(shard, state)| {
                (
                    shard,
                    Step::Apply {
                        algorithm,
                        state: state.output,
                        base,
                        damping,
                        messages: sent
                            .iter()
                            .filter(|r| r.files.contains(&shard))
                            .map(|r| r.output)
                            .collect(),
                    },
                )
            }),
        )
        .await?;
        for state in &states {
            graph
                .objects
                .delete(&object(
                    graph,
                    execution.id,
                    execution.job.attempt,
                    state.output,
                    "state",
                ))
                .await;
        }
        for msg in sent {
            for shard in msg.files {
                graph
                    .objects
                    .delete(&object(
                        graph,
                        execution.id,
                        execution.job.attempt,
                        msg.output,
                        &format!("to-{shard}"),
                    ))
                    .await;
            }
        }
        converged = match algorithm {
            Algorithm::Rank => next.iter().map(|r| r.delta).sum::<f64>() <= tolerance,
            Algorithm::Components => next.iter().all(|r| !r.changed),
        };
        states = next;
        if converged {
            break;
        }
    }
    ensure!(
        matches!(algorithm, Algorithm::Rank) || converged,
        "weak components did not converge within max_iterations"
    );
    let results = round(
        execution,
        states.iter().enumerate().map(|(shard, state)| {
            (
                shard,
                Step::Finish {
                    algorithm,
                    state: state.output,
                },
            )
        }),
    )
    .await?;
    for state in states {
        graph
            .objects
            .delete(&object(
                graph,
                execution.id,
                execution.job.attempt,
                state.output,
                "state",
            ))
            .await;
    }
    let schema = schema(algorithm);
    let mut buffer = Buffer::new(
        &graph.scope.runtime.memory_pool,
        Some(&graph.scope.runtime.disk_manager),
    );
    for result in results {
        let key = object(
            graph,
            execution.id,
            execution.job.attempt,
            result.output,
            "result",
        );
        let mut stream = graph.objects.get(&key).await?.into_stream();
        let mut decoder = datafusion::arrow::ipc::reader::StreamDecoder::new();
        while let Some(chunk) = stream.try_next().await? {
            graph.scope.check()?;
            graph.scope.reads.object_bytes(chunk.len());
            let mut chunk = datafusion::arrow::buffer::Buffer::from(chunk);
            while !chunk.is_empty() {
                if let Some(batch) = decoder.decode(&mut chunk)? {
                    buffer.push(batch)?;
                }
            }
        }
        decoder.finish()?;
        graph.objects.delete(&key).await;
    }
    buffer.finish(schema, &graph.scope.reads)
}

async fn round(
    execution: &Execution<'_>,
    steps: impl Iterator<Item = (usize, Step)>,
) -> Result<Vec<Report>> {
    let steps = steps
        .map(|(shard, step)| (shard, super::Step::Iteration(step)))
        .collect::<Vec<_>>();
    execution
        .round(steps)
        .await?
        .outputs
        .into_iter()
        .map(|o| o.iteration.context("missing CSR iteration report"))
        .collect()
}
