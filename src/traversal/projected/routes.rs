use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashSet},
    mem::size_of,
};

use anyhow::Result;
use bigdecimal::{BigDecimal, Zero};
use datafusion::execution::memory_pool::MemoryReservation;

use super::{cooperate, graph::Graph};
use crate::engine::QueryScope;

pub(super) struct Path {
    pub(super) vertices: Vec<usize>,
    pub(super) edges: Vec<usize>,
    pub(super) cost: BigDecimal,
}

struct Distance {
    cost: BigDecimal,
    vertex: usize,
}

impl PartialEq for Distance {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl Eq for Distance {}
impl PartialOrd for Distance {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Distance {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .cost
            .cmp(&self.cost)
            .then_with(|| other.vertex.cmp(&self.vertex))
    }
}

struct Candidate(Path);

impl PartialEq for Candidate {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl Eq for Candidate {}
impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .0
            .cost
            .cmp(&self.0.cost)
            .then_with(|| other.0.edges.cmp(&self.0.edges))
    }
}

pub(super) async fn k_shortest(
    graph: &Graph,
    start: usize,
    target: usize,
    k: usize,
    scope: &QueryScope,
    budget: &MemoryReservation,
) -> Result<Vec<Path>> {
    let mut accepted = Vec::new();
    let mut queue = BinaryHeap::<Candidate>::new();
    let mut seen = HashSet::new();
    let search = budget.new_empty();
    let Some(first) = shortest(
        graph,
        start,
        target,
        (&BigDecimal::zero(), &HashSet::new(), &HashSet::new(), None),
        scope,
        &search,
    )
    .await?
    else {
        return Ok(accepted);
    };
    budget.try_grow(path_bytes(&first, graph))?;
    seen.insert(first.edges.clone());
    accepted.push(first);
    while accepted.len() < k {
        let previous = accepted.last().expect("first path");
        let mut banned_vertices = HashSet::new();
        let mut root_cost = BigDecimal::zero();
        for (index, edge) in previous.edges.iter().copied().enumerate() {
            scope.check()?;
            let prefix = &previous.edges[..index];
            let banned_edges = accepted
                .iter()
                .filter(|path| path.edges.starts_with(prefix))
                .filter_map(|path| path.edges.get(index).copied())
                .collect();
            let spur = previous.vertices[index];
            if let Some(mut suffix) = shortest(
                graph,
                spur,
                target,
                (&root_cost, &banned_vertices, &banned_edges, None),
                scope,
                &search,
            )
            .await?
            {
                let edges = prefix
                    .iter()
                    .copied()
                    .chain(suffix.edges)
                    .collect::<Vec<_>>();
                if !seen.contains(&edges) {
                    let mut vertices = previous.vertices[..index].to_vec();
                    vertices.append(&mut suffix.vertices);
                    let path = Path {
                        vertices,
                        edges,
                        cost: suffix.cost,
                    };
                    budget.try_grow(path_bytes(&path, graph))?;
                    seen.insert(path.edges.clone());
                    queue.push(Candidate(path));
                }
            }
            banned_vertices.insert(spur);
            root_cost += &graph.route_weights[edge];
        }
        let Some(Candidate(next)) = queue.pop() else {
            break;
        };
        accepted.push(next);
    }
    Ok(accepted)
}

fn path_bytes(path: &Path, graph: &Graph) -> usize {
    path.vertices
        .len()
        .saturating_add(path.edges.len().saturating_mul(2))
        .saturating_mul(size_of::<usize>())
        .saturating_add(128)
        .saturating_add(graph.route_cost_bytes)
}

pub(super) async fn shortest(
    graph: &Graph,
    start: usize,
    target: usize,
    (initial_cost, banned_vertices, banned_edges, potentials): (
        &BigDecimal,
        &HashSet<usize>,
        &HashSet<usize>,
        Option<&[BigDecimal]>,
    ),
    scope: &QueryScope,
    reservation: &MemoryReservation,
) -> Result<Option<Path>> {
    let budget = reservation.new_empty();
    let n = graph.vertices.len();
    let cost_bytes = graph.route_cost_bytes;
    budget.try_grow(
        n.saturating_mul(3 * size_of::<usize>() + size_of::<Option<BigDecimal>>() + cost_bytes),
    )?;
    let mut distances = vec![None; n];
    let mut parent = vec![None; n];
    let mut heap = BinaryHeap::new();
    distances[start] = Some(initial_cost.clone());
    budget.try_grow(4 * (size_of::<Distance>() + cost_bytes))?;
    heap.push(Distance {
        vertex: start,
        cost: initial_cost.clone(),
    });
    let mut work = 0;
    while let Some(Distance { vertex, cost }) = heap.pop() {
        work += 1;
        cooperate(scope, work).await?;
        if distances[vertex]
            .as_ref()
            .is_some_and(|known| &cost > known)
        {
            continue;
        }
        if vertex == target {
            let cost = match potentials {
                Some(potentials) => cost - &potentials[start] + &potentials[target],
                None => cost,
            };
            return path_to(target, cost, &parent, scope).await.map(Some);
        }
        for neighbor in &graph.adjacency[vertex] {
            work += 1;
            cooperate(scope, work).await?;
            if banned_vertices.contains(&neighbor.vertex) || banned_edges.contains(&neighbor.edge) {
                continue;
            }
            let mut distance = &cost + &graph.route_weights[neighbor.edge];
            if let Some(potentials) = potentials {
                distance += &potentials[vertex];
                distance -= &potentials[neighbor.vertex];
            }
            if distances[neighbor.vertex]
                .as_ref()
                .is_none_or(|known| &distance < known)
            {
                if heap.len() == heap.capacity() {
                    budget.try_grow(
                        heap.capacity()
                            .max(4)
                            .saturating_mul(size_of::<Distance>() + cost_bytes),
                    )?;
                }
                distances[neighbor.vertex] = Some(distance.clone());
                parent[neighbor.vertex] = Some((vertex, neighbor.edge));
                heap.push(Distance {
                    vertex: neighbor.vertex,
                    cost: distance,
                });
            }
        }
    }
    Ok(None)
}

pub(super) async fn path_to(
    target: usize,
    cost: BigDecimal,
    parent: &[Option<(usize, usize)>],
    scope: &QueryScope,
) -> Result<Path> {
    let mut vertices = vec![target];
    let mut edges = Vec::new();
    let mut cursor = target;
    while let Some((from, edge)) = parent[cursor] {
        anyhow::ensure!(edges.len() < parent.len(), "cycle in computed path");
        cooperate(scope, edges.len()).await?;
        edges.push(edge);
        vertices.push(from);
        cursor = from;
    }
    vertices.reverse();
    edges.reverse();
    Ok(Path {
        vertices,
        edges,
        cost,
    })
}
