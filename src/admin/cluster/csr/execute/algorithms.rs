use super::*;

pub(super) async fn partition_step(graph: &Graph<'_>, shard: usize, step: &Step) -> Result<Output> {
    let mut result = Output::default();
    let budget = graph.budget.new_empty();
    match step {
        Step::Iteration(_) => unreachable!(),
        Step::Arcs {
            frontier,
            start_block,
            end_block,
            targets,
            filters,
        } => {
            let mut candidates = Vec::new();
            graph
                .arc_blocks(shard, Some(frontier), *start_block..*end_block, |arc| {
                    if targets.as_ref().is_some_and(|ids| !ids.contains(&arc.to)) {
                        return Ok(());
                    }
                    budget.try_grow(48)?;
                    if filters.is_empty() {
                        result.links.push((arc.to, arc.from, arc.edge));
                    } else {
                        candidates.push(arc);
                    }
                    Ok(())
                })
                .await?;
            if !candidates.is_empty() {
                let source = super::properties::Read {
                    objects: graph.objects,
                    prefix: graph.prefix.clone(),
                    manifest: &graph.manifest,
                    scope: graph.scope,
                    cache: None,
                };
                let filtered = source.filter_edges(&candidates, filters, &budget).await?;
                result.skipped_property_blocks = filtered.skipped;
                result.filter_column_pages = filtered.column_pages;
                result.filtered_arcs = candidates
                    .iter()
                    .filter(|arc| {
                        filters.iter().any(|(kind, _)| *kind == arc.kind)
                            && !filtered.accepted.contains(&arc.edge)
                    })
                    .count() as u64;
                for arc in candidates {
                    if !filters.iter().any(|(kind, _)| *kind == arc.kind)
                        || filtered.accepted.contains(&arc.edge)
                    {
                        result.links.push((arc.to, arc.from, arc.edge));
                    }
                }
            }
        }
        Step::Frontier {
            frontier,
            path_limit,
            target,
        } => {
            let mut seen = BTreeMap::<Vertex, usize>::new();
            graph
                .arcs(shard, Some(frontier), |arc| {
                    if !seen.contains_key(&arc.to) {
                        budget.try_grow(64)?;
                    }
                    let count = seen.entry(arc.to).or_default();
                    if *count <= *path_limit {
                        budget.try_grow(48)?;
                        result.links.push((arc.to, arc.from, arc.edge));
                        *count += 1;
                    }
                    Ok(())
                })
                .await?;
            if let Some(target) = target
                && seen.contains_key(target)
            {
                result.links.retain(|(to, _, _)| to == target);
            }
        }
    }
    Ok(result)
}

type Table = (
    Arc<dyn datafusion::catalog::TableProvider>,
    MemoryReservation,
);

pub(super) async fn run(execution: &Execution<'_>, operation: &Operation) -> Result<Table> {
    match operation {
        Operation::Reachable { start, max_hops } => {
            let start = endpoint(&execution.graph, start).await?;
            let (depth, _) = bfs(execution, start, None, *max_hops, 0).await?;
            node_results(
                execution,
                "hops",
                DataType::UInt64,
                |ids| {
                    let values = ids
                        .iter()
                        .map(|id| depth.get(id).map(|hops| u64::from(*hops)))
                        .collect::<UInt64Array>();
                    Ok(Arc::new(values))
                },
                Some(&depth),
            )
            .await
        }
        Operation::Shortest {
            start,
            target,
            max_hops,
        } => paths(execution, start, target, *max_hops, 1, false).await,
        Operation::AllShortest {
            start,
            target,
            max_hops,
            max_paths,
        } => {
            ensure!(
                (1..=10_000).contains(max_paths),
                "max_paths must be 1–10000"
            );
            paths(execution, start, target, *max_hops, *max_paths, true).await
        }
        Operation::Pagerank {
            damping,
            tolerance,
            max_iterations,
        } => pagerank(execution, *damping, *tolerance, *max_iterations).await,
        Operation::Wcc { max_iterations } => wcc(execution, *max_iterations).await,
    }
}

async fn endpoint(graph: &Graph<'_>, endpoint: &Endpoint) -> Result<Vertex> {
    let id = graph.route(endpoint)?;
    graph.vertex(id).await?;
    Ok(id)
}

type Depth = BTreeMap<Vertex, u32>;
type Predecessors = BTreeMap<Vertex, Vec<(Vertex, u64)>>;

async fn bfs(
    execution: &Execution<'_>,
    start: Vertex,
    target: Option<Vertex>,
    max_hops: u32,
    path_limit: usize,
) -> Result<(Depth, Predecessors)> {
    ensure!(max_hops <= MAX_HOPS, "max_hops must be at most {MAX_HOPS}");
    let graph = &execution.graph;
    let count = graph.manifest.partitions.len();
    let mut depth = BTreeMap::from([(start, 0)]);
    let mut predecessors = BTreeMap::<Vertex, Vec<(Vertex, u64)>>::new();
    let mut frontier = vec![BTreeSet::new(); count];
    frontier[partition(start, count)].insert(start);
    for hop in 1..=max_hops {
        if target.is_some_and(|id| depth.contains_key(&id)) {
            break;
        }
        let inputs = frontier
            .into_iter()
            .enumerate()
            .filter(|(_, ids)| !ids.is_empty())
            .map(|(shard, frontier)| {
                (
                    shard,
                    Step::Frontier {
                        frontier,
                        path_limit,
                        target,
                    },
                )
            })
            .collect::<Vec<_>>();
        if inputs.is_empty() {
            break;
        }
        frontier = vec![BTreeSet::new(); count];
        let round = execution.round(inputs).await?;
        let hit_target = target.is_some_and(|target| {
            round
                .outputs
                .iter()
                .any(|o| o.links.iter().any(|(to, _, _)| *to == target))
        });
        for output in round.outputs {
            for (to, from, edge) in output.links {
                if hit_target && Some(to) != target {
                    continue;
                }
                let distance = match depth.entry(to) {
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        graph.budget.try_grow(176)?;
                        frontier[partition(to, count)].insert(to);
                        *entry.insert(hop)
                    }
                    std::collections::btree_map::Entry::Occupied(entry) => *entry.get(),
                };
                if target.is_some() && distance == hop {
                    let entries = predecessors.entry(to).or_default();
                    if entries.len() <= path_limit {
                        graph.budget.try_grow(48)?;
                        entries.push((from, edge));
                    }
                }
            }
        }
    }
    Ok((depth, predecessors))
}

async fn paths(
    execution: &Execution<'_>,
    start: &Endpoint,
    target: &Endpoint,
    max_hops: u32,
    max_paths: usize,
    all: bool,
) -> Result<Table> {
    let graph = &execution.graph;
    let start = endpoint(graph, start).await?;
    let target = endpoint(graph, target).await?;
    let (depth, mut predecessors) = bfs(
        execution,
        start,
        Some(target),
        max_hops,
        if all { max_paths } else { 0 },
    )
    .await?;
    for choices in predecessors.values_mut() {
        choices.sort_unstable();
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("hops", DataType::UInt64, false),
        Field::new("vertices", DataType::Utf8, false),
        Field::new("edges", DataType::Utf8, false),
    ]));
    let mut buffer = Buffer::new(
        &graph.scope.runtime.memory_pool,
        Some(&graph.scope.runtime.disk_manager),
    );
    if let Some(&hops) = depth.get(&target) {
        let mut visited = vec![(target, 0_usize)];
        let mut edge_path = Vec::new();
        let mut emitted = 0;
        while let Some(&(vertex, choice)) = visited.last() {
            graph.scope.check()?;
            if vertex == start {
                ensure!(emitted < max_paths, "shortest path count exceeds max_paths");
                let mut vertices = Vec::with_capacity(visited.len());
                for (vertex, _) in visited.iter().rev() {
                    vertices.push(graph.vertex(*vertex).await?);
                }
                let mut edges = Vec::with_capacity(edge_path.len());
                for edge in edge_path.iter().rev() {
                    edges.push(graph.edge(*edge).await?);
                }
                buffer.push(RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(UInt64Array::from(vec![u64::from(hops)])),
                        Arc::new(StringArray::from(vec![serde_json::to_string(&vertices)?])),
                        Arc::new(StringArray::from(vec![serde_json::to_string(&edges)?])),
                    ],
                )?)?;
                emitted += 1;
                if !all {
                    break;
                }
            } else if let Some(&(from, edge)) = predecessors
                .get(&vertex)
                .and_then(|items| items.get(choice))
            {
                visited.last_mut().expect("path stack").1 += 1;
                visited.push((from, 0));
                edge_path.push(edge);
                continue;
            }
            visited.pop();
            edge_path.pop();
        }
    }
    buffer.finish(schema, &graph.scope.reads)
}

async fn pagerank(
    execution: &Execution<'_>,
    damping: f64,
    tolerance: f64,
    max_iterations: usize,
) -> Result<Table> {
    crate::GraphAlgorithm::PageRank {
        damping,
        tolerance,
        max_iterations,
    }
    .validate()?;
    ensure!(
        max_iterations <= 1000,
        "max_iterations must be at most 1000"
    );
    iterative::run(
        execution,
        iterative::Algorithm::Rank,
        max_iterations,
        damping,
        tolerance,
    )
    .await
}
async fn wcc(execution: &Execution<'_>, max_iterations: usize) -> Result<Table> {
    ensure!(
        (1..=100_000).contains(&max_iterations),
        "max_iterations must be 1–100000"
    );
    iterative::run(
        execution,
        iterative::Algorithm::Components,
        max_iterations,
        0.0,
        0.0,
    )
    .await
}

async fn node_results(
    execution: &Execution<'_>,
    name: &str,
    ty: DataType,
    score: impl Fn(&[Vertex]) -> Result<ArrayRef>,
    subset: Option<&Depth>,
) -> Result<Table> {
    let graph = &execution.graph;
    let schema = Arc::new(Schema::new(vec![
        Field::new("vertex", DataType::Utf8, false),
        Field::new(name, ty, false),
    ]));
    let mut buffer = Buffer::new(
        &graph.scope.runtime.memory_pool,
        Some(&graph.scope.runtime.disk_manager),
    );
    let selected_memory = graph.budget.new_empty();
    let selected = if let Some(ids) = subset {
        let mut blocks = BTreeSet::new();
        for &id in ids.keys() {
            graph.scope.check()?;
            let shard = partition(id, graph.manifest.partitions.len());
            let index = identity_block(&graph.manifest.partitions[shard].vertices, id)
                .context("missing CSR vertex identity block")?;
            if blocks.insert((shard, index)) {
                selected_memory.try_grow(64)?;
            }
        }
        Some(blocks)
    } else {
        None
    };
    for shard in 0..graph.manifest.partitions.len() {
        let mut ids = Vec::new();
        let mut identities = Vec::new();
        let mut flush = |ids: &mut Vec<Vertex>, identities: &mut Vec<String>| -> Result<()> {
            if !ids.is_empty() {
                let values = score(ids)?;
                buffer.push(RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(StringArray::from(std::mem::take(identities))),
                        values,
                    ],
                )?)?;
                ids.clear();
            }
            Ok(())
        };
        let indices = selected.as_ref().map(|blocks| {
            blocks
                .range((shard, 0)..=(shard, usize::MAX))
                .map(|&(_, index)| index)
                .collect::<Vec<_>>()
        });
        graph
            .vertices(shard, indices.as_deref(), |id, identity| {
                if subset.is_some_and(|vertices| !vertices.contains_key(&id)) {
                    return Ok(());
                }
                ids.push(id);
                identities.push(std::str::from_utf8(vertex_record(identity)?.0)?.to_owned());
                if ids.len() >= graph.scope.options.batch_size {
                    flush(&mut ids, &mut identities)?;
                }
                Ok(())
            })
            .await?;
        flush(&mut ids, &mut identities)?;
    }
    buffer.finish(schema, &graph.scope.reads)
}
