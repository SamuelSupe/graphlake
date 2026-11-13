use std::{
    collections::{BTreeMap, BTreeSet, HashSet, VecDeque},
    mem::size_of,
};

use anyhow::{Result, bail};
use bigdecimal::{BigDecimal, Zero};
use datafusion::execution::memory_pool::MemoryReservation;

use crate::engine::QueryScope;

use super::{
    RouteStrategy, cooperate,
    graph::Graph,
    routes::{self, Path},
};

pub(super) async fn find(
    graph: &Graph,
    start: usize,
    target: usize,
    k: usize,
    strategy: RouteStrategy,
    scope: &QueryScope,
    budget: &MemoryReservation,
) -> Result<(Vec<Path>, &'static str)> {
    if matches!(strategy, RouteStrategy::Auto) {
        return Ok((
            routes::k_shortest(graph, start, target, k, scope, budget).await?,
            if k == 1 { "dijkstra" } else { "yen" },
        ));
    }
    let n = graph.vertices.len();
    budget.try_grow(n.saturating_mul(graph.route_cost_bytes.saturating_mul(2) + 128))?;
    let mut distance = vec![None; n];
    let mut parent = vec![None; n];
    distance[start] = Some(BigDecimal::zero());
    let name = match strategy {
        RouteStrategy::Auto => unreachable!(),
        RouteStrategy::BellmanFord => {
            bellman_ford(graph, &mut distance, &mut parent, scope).await?;
            "bellman_ford"
        }
        RouteStrategy::Johnson => {
            distance.fill(Some(BigDecimal::zero()));
            bellman_ford(graph, &mut distance, &mut parent, scope).await?;
            let potentials = distance
                .into_iter()
                .map(|cost| cost.expect("all potentials were initialized"))
                .collect::<Vec<_>>();
            let path = routes::shortest(
                graph,
                start,
                target,
                (
                    &BigDecimal::zero(),
                    &HashSet::new(),
                    &HashSet::new(),
                    Some(&potentials),
                ),
                scope,
                budget,
            )
            .await?;
            return Ok((path.into_iter().collect(), "johnson"));
        }
        RouteStrategy::DagLongest => {
            dag(graph, &mut distance, &mut parent, scope).await?;
            "dag_longest"
        }
        RouteStrategy::DeltaStepping => {
            delta_stepping(
                graph,
                start,
                target,
                &mut distance,
                &mut parent,
                scope,
                budget,
            )
            .await?;
            "delta_stepping"
        }
    };
    let path = match distance[target].take() {
        Some(cost) => vec![routes::path_to(target, cost, &parent, scope).await?],
        None => Vec::new(),
    };
    Ok((path, name))
}

async fn bellman_ford(
    graph: &Graph,
    distance: &mut [Option<BigDecimal>],
    parent: &mut [Option<(usize, usize)>],
    scope: &QueryScope,
) -> Result<()> {
    let mut work = 0;
    for _ in 0..distance.len() {
        let mut changed = false;
        for (from, neighbors) in graph.adjacency.iter().enumerate() {
            work += 1;
            cooperate(scope, work).await?;
            let Some(cost) = distance[from].clone() else {
                continue;
            };
            for neighbor in neighbors {
                work += 1;
                cooperate(scope, work).await?;
                let next = &cost + &graph.route_weights[neighbor.edge];
                if distance[neighbor.vertex]
                    .as_ref()
                    .is_none_or(|old| &next < old)
                {
                    distance[neighbor.vertex] = Some(next);
                    parent[neighbor.vertex] = Some((from, neighbor.edge));
                    changed = true;
                }
            }
        }
        if !changed {
            return Ok(());
        }
    }
    bail!("negative-weight cycle in the route graph")
}

async fn dag(
    graph: &Graph,
    distance: &mut [Option<BigDecimal>],
    parent: &mut [Option<(usize, usize)>],
    scope: &QueryScope,
) -> Result<()> {
    let mut indegree = vec![0_usize; distance.len()];
    let mut work = 0;
    for neighbors in &graph.adjacency {
        for neighbor in neighbors {
            work += 1;
            cooperate(scope, work).await?;
            indegree[neighbor.vertex] += 1;
        }
    }
    let mut ready: VecDeque<_> = indegree
        .iter()
        .enumerate()
        .filter_map(|(vertex, &degree)| (degree == 0).then_some(vertex))
        .collect();
    let mut visited = 0;
    while let Some(from) = ready.pop_front() {
        visited += 1;
        cooperate(scope, visited).await?;
        let cost = distance[from].clone();
        for neighbor in &graph.adjacency[from] {
            work += 1;
            cooperate(scope, work).await?;
            let to = neighbor.vertex;
            if let Some(cost) = &cost {
                let next = cost + &graph.route_weights[neighbor.edge];
                if distance[to].as_ref().is_none_or(|old| &next > old) {
                    distance[to] = Some(next);
                    parent[to] = Some((from, neighbor.edge));
                }
            }
            indegree[to] -= 1;
            if indegree[to] == 0 {
                ready.push_back(to);
            }
        }
    }
    anyhow::ensure!(
        visited == distance.len(),
        "DAG longest path requires an acyclic projection"
    );
    Ok(())
}

async fn delta_stepping(
    graph: &Graph,
    start: usize,
    target: usize,
    distance: &mut [Option<BigDecimal>],
    parent: &mut [Option<(usize, usize)>],
    scope: &QueryScope,
    budget: &MemoryReservation,
) -> Result<()> {
    // Unit-width buckets preserve the exact decimal costs and keep bucket arithmetic simple.
    let delta = BigDecimal::from(1);
    let entry_bytes = graph.route_cost_bytes + size_of::<[usize; 8]>();
    let buckets_budget = budget.new_empty();
    buckets_budget.try_grow(entry_bytes)?;
    let mut buckets = BTreeMap::from([(BigDecimal::zero(), vec![start])]);
    let mut work = 0;
    while let Some((bucket, initial)) = buckets.pop_first() {
        let mut settled = BTreeSet::new();
        let mut pending = initial;
        for light in [true, false] {
            if !light {
                pending.extend(settled.iter().copied());
            }
            loop {
                for from in pending.drain(..) {
                    if light {
                        buckets_budget.shrink(entry_bytes);
                    }
                    work += 1;
                    cooperate(scope, work).await?;
                    let cost = distance[from].as_ref().expect("queued distance").clone();
                    if cost.with_scale(0) != bucket {
                        continue;
                    }
                    if light {
                        settled.insert(from);
                    }
                    for neighbor in &graph.adjacency[from] {
                        work += 1;
                        cooperate(scope, work).await?;
                        let weight = &graph.route_weights[neighbor.edge];
                        if (weight <= &delta) != light {
                            continue;
                        }
                        let next = &cost + weight;
                        if distance[neighbor.vertex]
                            .as_ref()
                            .is_none_or(|old| &next < old)
                        {
                            buckets_budget.try_grow(entry_bytes)?;
                            buckets
                                .entry(next.with_scale(0))
                                .or_default()
                                .push(neighbor.vertex);
                            distance[neighbor.vertex] = Some(next);
                            parent[neighbor.vertex] = Some((from, neighbor.edge));
                        }
                    }
                }
                if !light {
                    break;
                }
                let Some(next) = buckets.remove(&bucket) else {
                    break;
                };
                pending = next;
            }
        }
        if settled.contains(&target) {
            break;
        }
    }
    Ok(())
}
