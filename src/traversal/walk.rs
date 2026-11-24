use std::collections::{HashSet, VecDeque};

use super::{Neighbor, Topology, TraversalKind};
use crate::cypher::paths::Shortest;

pub(super) struct Hit {
    pub(super) vertex: usize,
    pub(super) depth: usize,
    pub(super) edges: Vec<usize>,
    pub(super) vertices: Vec<usize>,
}

pub(super) enum Step {
    Continue,
    Visit { depth: usize, hit: Option<Hit> },
    Done,
}

pub(super) struct Walk(WalkState);

enum WalkState {
    BreadthFirst(BreadthFirst),
    Trails(Trails),
}

impl Walk {
    pub(super) fn new(
        graph: &Topology,
        start: usize,
        target: Target,
        kind: TraversalKind,
        shortest: Option<(Shortest, usize)>,
        track_vertices: bool,
        undirected: bool,
    ) -> Self {
        if let Some(shortest) = shortest {
            return Self(WalkState::BreadthFirst(BreadthFirst::new(
                graph,
                start,
                target,
                Some(shortest),
                track_vertices,
                undirected,
            )));
        }
        Self(match kind {
            TraversalKind::Paths { min_hops } => {
                WalkState::Trails(Trails::new(graph, start, min_hops, track_vertices))
            }
            TraversalKind::Reachable | TraversalKind::Shortest { .. } => WalkState::BreadthFirst(
                BreadthFirst::new(graph, start, target, None, false, undirected),
            ),
        })
    }

    pub(super) fn restart(&mut self, graph: &Topology, start: usize, target: Target) {
        match &mut self.0 {
            WalkState::BreadthFirst(bfs) => bfs.restart(start, target),
            WalkState::Trails(trails) => {
                trails.stack.push(Trails::frame(graph, start));
                debug_assert!(trails.path.is_empty() && trails.used.is_empty());
            }
        }
    }

    pub(super) fn step(&mut self, graph: &Topology, max_hops: usize) -> Step {
        match &mut self.0 {
            WalkState::BreadthFirst(bfs) => bfs.step(graph, max_hops),
            WalkState::Trails(trails) => trails.step(graph, max_hops),
        }
    }
}

#[derive(Clone, Copy)]
pub(super) enum Target {
    All,
    Vertex(Option<usize>),
}

struct BreadthFirst {
    start: usize,
    target: Target,
    queue: VecDeque<(usize, usize)>,
    predecessor: Vec<Option<Neighbor>>,
    touched: Vec<usize>,
    expansion: Option<(usize, usize, std::ops::Range<usize>)>,
    shortest: Option<(Shortest, usize)>,
    ties: Option<Ties>,
    emit: Option<EmitPaths>,
    track_vertices: bool,
    undirected: bool,
    undirected_cycle: Option<UndirectedCycle>,
}

struct UndirectedCycle {
    depths: Vec<usize>,
    root_edge: Vec<usize>,
    best_len: Option<usize>,
    witnesses: Vec<CycleWitness>,
    pending_hits: VecDeque<Hit>,
    emitted: bool,
}

#[derive(Clone, Copy)]
enum CycleWitness {
    SelfLoop {
        edge: usize,
    },
    Odd {
        u: usize,
        w: usize,
        edge: usize,
        depth: usize,
    },
    Even {
        meet: usize,
        u: usize,
        edge: usize,
        depth: usize,
    },
}

impl UndirectedCycle {
    fn new(size: usize, start: usize) -> Self {
        let mut depths = vec![usize::MAX; size];
        let root_edge = vec![usize::MAX; size];
        depths[start] = 0;
        Self {
            depths,
            root_edge,
            best_len: None,
            witnesses: Vec::new(),
            pending_hits: VecDeque::new(),
            emitted: false,
        }
    }

    fn reset(&mut self, touched: &[usize], start: usize) {
        for &vertex in touched {
            self.depths[vertex] = usize::MAX;
            self.root_edge[vertex] = usize::MAX;
        }
        self.depths[start] = 0;
        self.root_edge[start] = usize::MAX;
        self.best_len = None;
        self.witnesses.clear();
        self.pending_hits.clear();
        self.emitted = false;
    }

    fn record(&mut self, len: usize, witness: CycleWitness, keep_all: bool) {
        match self.best_len {
            None => {
                self.best_len = Some(len);
                self.witnesses.push(witness);
            }
            Some(best) if len < best => {
                self.best_len = Some(len);
                self.witnesses.clear();
                self.witnesses.push(witness);
            }
            Some(best) if len == best && keep_all => {
                self.witnesses.push(witness);
            }
            _ => {}
        }
    }
}

impl BreadthFirst {
    fn allow_directed_return(
        undirected: bool,
        shortest: Option<(Shortest, usize)>,
        target: Target,
        start: usize,
    ) -> bool {
        !undirected
            && matches!(shortest, Some((_, min)) if min >= 1)
            && !matches!(target, Target::Vertex(Some(t)) if t != start)
    }

    fn track_undirected_cycle(
        undirected: bool,
        shortest: Option<(Shortest, usize)>,
        target: Target,
        start: usize,
    ) -> bool {
        undirected
            && matches!(shortest, Some((_, min)) if min >= 1)
            && !matches!(target, Target::Vertex(Some(t)) if t != start)
    }

    fn new(
        graph: &Topology,
        start: usize,
        target: Target,
        shortest: Option<(Shortest, usize)>,
        track_vertices: bool,
        undirected: bool,
    ) -> Self {
        let size = graph.keys.num_rows();
        let allow_return = Self::allow_directed_return(undirected, shortest, target, start);
        let mut predecessor = vec![None; size];
        let mut touched = Vec::new();
        if !allow_return {
            predecessor[start] = Some(Neighbor {
                vertex: start,
                edge: 0,
            });
            touched.push(start);
        }
        let queue = if matches!(target, Target::Vertex(None)) {
            VecDeque::new()
        } else {
            VecDeque::from([(start, 0)])
        };
        let track_cycle = Self::track_undirected_cycle(undirected, shortest, target, start);
        Self {
            start,
            target,
            queue,
            predecessor,
            touched,
            expansion: None,
            shortest,
            ties: matches!(shortest, Some((Shortest::All, _))).then(|| {
                let mut depths = vec![usize::MAX; size];
                if !allow_return {
                    depths[start] = 0;
                }
                Ties {
                    depths,
                    parents: vec![Vec::new(); size],
                }
            }),
            emit: None,
            track_vertices,
            undirected,
            undirected_cycle: track_cycle.then(|| UndirectedCycle::new(size, start)),
        }
    }

    fn restart(&mut self, start: usize, target: Target) {
        let allow_return =
            Self::allow_directed_return(self.undirected, self.shortest, target, start);
        if let Some(cycle) = &mut self.undirected_cycle {
            cycle.reset(&self.touched, start);
        }
        for vertex in self.touched.drain(..) {
            self.predecessor[vertex] = None;
            if let Some(ties) = &mut self.ties {
                ties.depths[vertex] = usize::MAX;
                // Do not retain a different start's predecessor allocations.
                ties.parents[vertex] = Vec::new();
            }
        }
        self.start = start;
        self.target = target;
        self.expansion = None;
        self.emit = None;
        self.queue.clear();
        if !matches!(target, Target::Vertex(None)) {
            self.queue.push_back((start, 0));
        }
        if !allow_return {
            self.predecessor[start] = Some(Neighbor {
                vertex: start,
                edge: 0,
            });
            self.touched.push(start);
            if let Some(ties) = &mut self.ties {
                ties.depths[start] = 0;
            }
        }
        if Self::track_undirected_cycle(self.undirected, self.shortest, target, start) {
            if self.undirected_cycle.is_none() {
                self.undirected_cycle = Some(UndirectedCycle::new(self.predecessor.len(), start));
            }
        } else {
            self.undirected_cycle = None;
        }
    }

    fn reconstruct_single(&self, target: usize, depth: usize) -> (Vec<usize>, Vec<usize>) {
        let mut edges = Vec::with_capacity(depth);
        let mut vertices = if self.track_vertices {
            let mut v = Vec::with_capacity(depth + 1);
            v.push(target);
            v
        } else {
            Vec::new()
        };
        let mut cursor = target;
        let mut remaining = depth;
        while remaining > 0 {
            let parent = self.predecessor[cursor].expect("BFS predecessor");
            edges.push(parent.edge);
            cursor = parent.vertex;
            if self.track_vertices {
                vertices.push(cursor);
            }
            remaining -= 1;
        }
        edges.reverse();
        vertices.reverse();
        (edges, vertices)
    }

    fn collect_all_paths(&self, target: usize, depth: usize) -> Vec<(Vec<usize>, Vec<usize>)> {
        let Some(ties) = &self.ties else {
            return vec![self.reconstruct_single(target, depth)];
        };
        let mut results = Vec::new();
        let mut stack = vec![(target, 0usize)];
        let mut edges = Vec::with_capacity(depth);
        while let Some((vertex, index)) = stack.last_mut() {
            if *vertex == self.start && edges.len() == depth {
                let path_edges = edges.iter().rev().copied().collect();
                let path_vertices = if self.track_vertices {
                    stack.iter().rev().map(|(v, _)| *v).collect()
                } else {
                    Vec::new()
                };
                results.push((path_edges, path_vertices));
                stack.pop();
                edges.pop();
                continue;
            }
            let next = if *index == 0 {
                self.predecessor[*vertex]
            } else {
                ties.parents[*vertex].get(*index - 1).copied()
            };
            if let Some(parent) = next {
                *index += 1;
                stack.push((parent.vertex, 0));
                edges.push(parent.edge);
            } else {
                stack.pop();
                edges.pop();
            }
        }
        results
    }

    fn materialize_undirected_cycles(&mut self) {
        let Some(cycle) = &mut self.undirected_cycle else {
            return;
        };
        if cycle.emitted {
            return;
        }
        cycle.emitted = true;
        let Some(best_len) = cycle.best_len else {
            return;
        };
        let witnesses = std::mem::take(&mut cycle.witnesses);
        let all = self.ties.is_some();
        let mut hits = VecDeque::new();
        let mut seen_edges = HashSet::<Vec<usize>>::new();
        let mut seen_meets = HashSet::<usize>::new();

        for witness in witnesses {
            match witness {
                CycleWitness::SelfLoop { edge } => {
                    let edges = vec![edge];
                    if !all || seen_edges.insert(edges.clone()) {
                        let vertices = if self.track_vertices {
                            vec![self.start, self.start]
                        } else {
                            Vec::new()
                        };
                        hits.push_back(Hit {
                            vertex: self.start,
                            depth: best_len,
                            edges,
                            vertices,
                        });
                        if !all {
                            break;
                        }
                    }
                }
                CycleWitness::Odd { u, w, edge, depth } => {
                    let left_paths = if all {
                        self.collect_all_paths(u, depth)
                    } else {
                        vec![self.reconstruct_single(u, depth)]
                    };
                    let right_paths = if all {
                        self.collect_all_paths(w, depth)
                    } else {
                        vec![self.reconstruct_single(w, depth)]
                    };
                    for (le, lv) in &left_paths {
                        for (re, rv) in &right_paths {
                            if le.first() == re.first() || le.iter().any(|e| re.contains(e)) {
                                continue;
                            }
                            let mut edges = Vec::with_capacity(best_len);
                            edges.extend_from_slice(le);
                            edges.push(edge);
                            edges.extend(re.iter().rev().copied());
                            if !all || seen_edges.insert(edges.clone()) {
                                let vertices = if self.track_vertices {
                                    let mut v = Vec::with_capacity(best_len + 1);
                                    v.extend_from_slice(lv);
                                    v.extend(rv.iter().rev().copied());
                                    v
                                } else {
                                    Vec::new()
                                };
                                hits.push_back(Hit {
                                    vertex: self.start,
                                    depth: best_len,
                                    edges,
                                    vertices,
                                });
                                if !all {
                                    break;
                                }
                            }
                        }
                        if !all && !hits.is_empty() {
                            break;
                        }
                    }
                    if !all && !hits.is_empty() {
                        break;
                    }
                }
                CycleWitness::Even {
                    meet,
                    u,
                    edge,
                    depth,
                } => {
                    if !all {
                        let (mut edges, mut vertices) = self.reconstruct_single(meet, depth);
                        let (re, rv) = self.reconstruct_single(u, depth - 1);
                        edges.push(edge);
                        edges.extend(re.iter().rev().copied());
                        if self.track_vertices {
                            vertices.extend(rv.iter().rev().copied());
                        }
                        hits.push_back(Hit {
                            vertex: self.start,
                            depth: best_len,
                            edges,
                            vertices,
                        });
                        break;
                    }
                    if !seen_meets.insert(meet) {
                        continue;
                    }
                    let paths = self.collect_all_paths(meet, depth);
                    for (i, (le, lv)) in paths.iter().enumerate() {
                        for (j, (re, rv)) in paths.iter().enumerate() {
                            if i == j
                                || le.first() == re.first()
                                || le.iter().any(|e| re.contains(e))
                            {
                                continue;
                            }
                            let mut edges = Vec::with_capacity(best_len);
                            edges.extend_from_slice(le);
                            edges.extend(re.iter().rev().copied());
                            if seen_edges.insert(edges.clone()) {
                                let vertices = if self.track_vertices {
                                    let mut v = Vec::with_capacity(best_len + 1);
                                    v.extend_from_slice(lv);
                                    // `rv` ends with `meet`, which is already the last item of `lv`.
                                    v.extend(rv.iter().rev().skip(1).copied());
                                    v
                                } else {
                                    Vec::new()
                                };
                                hits.push_back(Hit {
                                    vertex: self.start,
                                    depth: best_len,
                                    edges,
                                    vertices,
                                });
                            }
                        }
                    }
                }
            }
        }
        if let Some(cycle) = &mut self.undirected_cycle {
            cycle.pending_hits = hits;
        }
    }

    fn step(&mut self, graph: &Topology, max_hops: usize) -> Step {
        if let Some(cycle) = &mut self.undirected_cycle
            && let Some(hit) = cycle.pending_hits.pop_front()
        {
            return Step::Visit {
                depth: hit.depth,
                hit: Some(hit),
            };
        }
        if let Some(emit) = &mut self.emit {
            match emit.step(
                self.start,
                &self.predecessor,
                &self.ties.as_ref().expect("ties").parents,
            ) {
                Some(step) => return step,
                None => self.emit = None,
            }
        }
        if let Some((vertex, depth, range)) = &mut self.expansion {
            if let Some(index) = range.next() {
                let edge = graph.neighbors[index];
                let min_hops = self.shortest.map_or(0, |(_, min)| min);
                let keep_all = self.ties.is_some();
                if let Some(cycle) = &mut self.undirected_cycle {
                    if *depth == 0 && edge.vertex == self.start {
                        if 1 >= min_hops && 1 <= max_hops {
                            cycle.record(1, CycleWitness::SelfLoop { edge: edge.edge }, keep_all);
                        }
                        return Step::Continue;
                    }
                    if edge.vertex != self.start {
                        let parent = Neighbor {
                            vertex: *vertex,
                            edge: edge.edge,
                        };
                        let my_root = if *depth == 0 {
                            edge.edge
                        } else {
                            cycle.root_edge[*vertex]
                        };
                        if self.predecessor[edge.vertex].is_none() {
                            self.predecessor[edge.vertex] = Some(parent);
                            self.touched.push(edge.vertex);
                            cycle.depths[edge.vertex] = *depth + 1;
                            cycle.root_edge[edge.vertex] = my_root;
                            if let Some(ties) = &mut self.ties {
                                ties.depths[edge.vertex] = *depth + 1;
                            }
                            self.queue.push_back((edge.vertex, *depth + 1));
                        } else {
                            let w_depth = cycle.depths[edge.vertex];
                            if w_depth == *depth + 1 {
                                if let Some(ties) = &mut self.ties {
                                    ties.parents[edge.vertex].push(parent);
                                }
                                let len = 2 * (*depth + 1);
                                if len >= min_hops
                                    && len <= max_hops
                                    && my_root != cycle.root_edge[edge.vertex]
                                {
                                    cycle.record(
                                        len,
                                        CycleWitness::Even {
                                            meet: edge.vertex,
                                            u: *vertex,
                                            edge: edge.edge,
                                            depth: *depth + 1,
                                        },
                                        keep_all,
                                    );
                                }
                            } else if *depth >= 1 && w_depth == *depth {
                                let len = 2 * (*depth) + 1;
                                if len >= min_hops
                                    && len <= max_hops
                                    && my_root != cycle.root_edge[edge.vertex]
                                {
                                    cycle.record(
                                        len,
                                        CycleWitness::Odd {
                                            u: *vertex,
                                            w: edge.vertex,
                                            edge: edge.edge,
                                            depth: *depth,
                                        },
                                        keep_all,
                                    );
                                }
                            }
                        }
                    }
                    return Step::Continue;
                }
                if edge.vertex == self.start && *depth + 1 < min_hops {
                    return Step::Continue;
                }
                let parent = Neighbor {
                    vertex: *vertex,
                    edge: edge.edge,
                };
                if self.predecessor[edge.vertex].is_none() {
                    self.predecessor[edge.vertex] = Some(parent);
                    self.touched.push(edge.vertex);
                    if let Some(ties) = &mut self.ties {
                        ties.depths[edge.vertex] = *depth + 1;
                    }
                    self.queue.push_back((edge.vertex, *depth + 1));
                } else if let Some(ties) = &mut self.ties
                    && ties.depths[edge.vertex] == *depth + 1
                {
                    ties.parents[edge.vertex].push(parent);
                }
                return Step::Continue;
            }
            self.expansion = None;
        }
        let Some((vertex, depth)) = self.queue.pop_front() else {
            if self
                .undirected_cycle
                .as_ref()
                .is_some_and(|c| !c.emitted && c.best_len.is_some())
            {
                self.materialize_undirected_cycles();
                if let Some(cycle) = &mut self.undirected_cycle
                    && let Some(hit) = cycle.pending_hits.pop_front()
                {
                    return Step::Visit {
                        depth: hit.depth,
                        hit: Some(hit),
                    };
                }
            }
            return Step::Done;
        };
        if matches!(self.target, Target::Vertex(Some(target)) if target == self.start)
            && let Some(cycle) = &self.undirected_cycle
            && (2 * depth + 1 > max_hops
                || cycle.best_len.is_some_and(|best| 2 * depth + 1 > best))
        {
            self.queue.clear();
            self.materialize_undirected_cycles();
            if let Some(cycle) = &mut self.undirected_cycle
                && let Some(hit) = cycle.pending_hits.pop_front()
            {
                return Step::Visit {
                    depth: hit.depth,
                    hit: Some(hit),
                };
            }
            return Step::Done;
        }
        let meets_min = self.shortest.is_none_or(|(_, min)| depth >= min);
        let found =
            meets_min && matches!(self.target, Target::Vertex(Some(target)) if target == vertex);
        if found {
            self.queue.clear();
        } else if depth < max_hops && !(depth > 0 && vertex == self.start) {
            self.expansion = Some((
                vertex,
                depth,
                graph.offsets[vertex]..graph.offsets[vertex + 1],
            ));
        }
        let eligible = found || matches!(self.target, Target::All) && meets_min;
        if !eligible {
            return Step::Visit { depth, hit: None };
        }
        if self.ties.is_some() {
            self.emit = Some(EmitPaths {
                vertex,
                depth,
                stack: vec![(vertex, 0)],
                edges: Vec::new(),
                track_vertices: self.track_vertices,
            });
            return Step::Continue;
        }
        let (edges, vertices) = if found || self.shortest.is_some() {
            self.reconstruct_single(vertex, depth)
        } else {
            (
                Vec::new(),
                if self.track_vertices {
                    vec![vertex]
                } else {
                    Vec::new()
                },
            )
        };
        Step::Visit {
            depth,
            hit: Some(Hit {
                vertex,
                depth,
                edges,
                vertices,
            }),
        }
    }
}

struct Ties {
    depths: Vec<usize>,
    parents: Vec<Vec<Neighbor>>,
}

struct EmitPaths {
    vertex: usize,
    depth: usize,
    stack: Vec<(usize, usize)>,
    edges: Vec<usize>,
    track_vertices: bool,
}

impl EmitPaths {
    fn step(
        &mut self,
        start: usize,
        first: &[Option<Neighbor>],
        extra: &[Vec<Neighbor>],
    ) -> Option<Step> {
        let (vertex, index) = self.stack.last_mut()?;
        if *vertex == start && self.edges.len() == self.depth {
            let hit = Hit {
                vertex: self.vertex,
                depth: self.depth,
                edges: self.edges.iter().rev().copied().collect(),
                vertices: if self.track_vertices {
                    self.stack.iter().rev().map(|(vertex, _)| *vertex).collect()
                } else {
                    Vec::new()
                },
            };
            self.stack.pop();
            self.edges.pop();
            return Some(Step::Visit {
                depth: self.depth,
                hit: Some(hit),
            });
        }
        let next = if *index == 0 {
            first[*vertex]
        } else {
            extra[*vertex].get(*index - 1).copied()
        };
        if let Some(parent) = next {
            *index += 1;
            self.stack.push((parent.vertex, 0));
            self.edges.push(parent.edge);
        } else {
            self.stack.pop();
            self.edges.pop();
        }
        Some(Step::Continue)
    }
}

struct Trails {
    stack: Vec<TrailFrame>,
    path: Vec<usize>,
    used: HashSet<usize>,
    min_hops: usize,
    track_vertices: bool,
}

struct TrailFrame {
    vertex: usize,
    range: std::ops::Range<usize>,
    visited: bool,
}

impl Trails {
    fn new(graph: &Topology, start: usize, min_hops: usize, track_vertices: bool) -> Self {
        Self {
            stack: vec![Self::frame(graph, start)],
            path: Vec::new(),
            used: HashSet::new(),
            min_hops,
            track_vertices,
        }
    }

    fn frame(graph: &Topology, vertex: usize) -> TrailFrame {
        TrailFrame {
            vertex,
            range: graph.offsets[vertex]..graph.offsets[vertex + 1],
            visited: false,
        }
    }

    fn step(&mut self, graph: &Topology, max_hops: usize) -> Step {
        let Some(frame) = self.stack.last_mut() else {
            return Step::Done;
        };
        let depth = self.path.len();
        if !frame.visited {
            frame.visited = true;
            let vertex = frame.vertex;
            let hit = (depth >= self.min_hops).then(|| Hit {
                vertex,
                depth,
                edges: self.path.clone(),
                vertices: if self.track_vertices {
                    self.stack.iter().map(|frame| frame.vertex).collect()
                } else {
                    Vec::new()
                },
            });
            return Step::Visit { depth, hit };
        }
        if depth < max_hops
            && let Some(index) = frame.range.next()
        {
            let edge = graph.neighbors[index];
            if self.used.insert(edge.edge) {
                self.path.push(edge.edge);
                self.stack.push(Self::frame(graph, edge.vertex));
            }
            return Step::Continue;
        }
        self.stack.pop();
        if let Some(edge) = self.path.pop() {
            self.used.remove(&edge);
        }
        Step::Continue
    }
}
