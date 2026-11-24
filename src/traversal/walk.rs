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
    ) -> Self {
        if let Some(shortest) = shortest {
            return Self(WalkState::BreadthFirst(BreadthFirst::new(
                graph,
                start,
                target,
                Some(shortest),
                track_vertices,
            )));
        }
        Self(match kind {
            TraversalKind::Paths { min_hops } => {
                WalkState::Trails(Trails::new(graph, start, min_hops, track_vertices))
            }
            TraversalKind::Reachable | TraversalKind::Shortest { .. } => {
                WalkState::BreadthFirst(BreadthFirst::new(graph, start, target, None, false))
            }
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
}

impl BreadthFirst {
    fn new(
        graph: &Topology,
        start: usize,
        target: Target,
        shortest: Option<(Shortest, usize)>,
        track_vertices: bool,
    ) -> Self {
        let allow_return = matches!(shortest, Some((_, min)) if min >= 1);
        let mut predecessor = vec![None; graph.keys.num_rows()];
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
        Self {
            start,
            target,
            queue,
            predecessor,
            touched,
            expansion: None,
            shortest,
            ties: matches!(shortest, Some((Shortest::All, _))).then(|| {
                let size = graph.keys.num_rows();
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
        }
    }

    fn restart(&mut self, start: usize, target: Target) {
        let allow_return = matches!(self.shortest, Some((_, min)) if min >= 1);
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
    }

    fn step(&mut self, graph: &Topology, max_hops: usize) -> Step {
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
            return Step::Done;
        };
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
        let mut edges = Vec::new();
        let mut vertices = if self.track_vertices {
            vec![vertex]
        } else {
            Vec::new()
        };
        if found || self.shortest.is_some() {
            let mut cursor = vertex;
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
        }
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
