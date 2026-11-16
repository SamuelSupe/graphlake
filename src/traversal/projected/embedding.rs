use anyhow::{Result, ensure};
use rand::{Rng, SeedableRng, rngs::SmallRng};

use super::{Node2VecOptions, cooperate, graph::Graph};
use crate::engine::QueryScope;

impl Node2VecOptions {
    pub(super) fn validate(&self) -> Result<()> {
        ensure!(
            self.dimensions > 0
                && self.walk_length > 1
                && self.walks_per_node > 0
                && self.window > 0
                && self.epochs > 0
                && self.negative_samples > 0,
            "Node2Vec needs positive dimensions, walk counts, window, epochs and negative samples"
        );
        ensure!(
            [self.return_bias, self.explore_bias, self.learning_rate]
                .into_iter()
                .all(|value| value.is_finite() && value > 0.0),
            "Node2Vec biases and learning rate must be finite and positive"
        );
        ensure!(
            self.learning_rate <= 1.0,
            "Node2Vec learning rate must be at most one"
        );
        Ok(())
    }
}

pub(super) async fn train(
    graph: &Graph,
    options: Node2VecOptions,
    scope: &QueryScope,
) -> Result<Vec<f32>> {
    let vertices = graph.vertices.len();
    let size = vertices
        .checked_mul(options.dimensions)
        .ok_or_else(|| anyhow::anyhow!("Node2Vec embedding size overflow"))?;
    let mut rng = SmallRng::seed_from_u64(options.seed);
    let initial = 0.5 / options.dimensions as f32;
    let mut input = Vec::with_capacity(size);
    for index in 0..size {
        input.push(rng.random_range(-initial..initial));
        cooperate(scope, index).await?;
    }
    let mut output = vec![0.0_f32; size];
    if vertices == 0 {
        return Ok(input);
    }

    // Negative samples follow the usual degree^0.75 distribution.
    let mut negative = Vec::with_capacity(vertices);
    let mut total = 0.0;
    for (index, row) in graph.adjacency.iter().enumerate() {
        total += (row.len() as f64).powf(0.75);
        negative.push(total);
        cooperate(scope, index).await?;
    }
    let mut walk = Vec::with_capacity(options.walk_length);
    let mut steps = 0_usize;
    for epoch in 0..options.epochs {
        let learning_rate =
            (options.learning_rate * (1.0 - epoch as f64 / options.epochs as f64)) as f32;
        for start in 0..vertices {
            scope.check()?;
            for _ in 0..options.walks_per_node {
                walk.clear();
                walk.push(start);
                while walk.len() < options.walk_length {
                    cooperate(scope, walk.len()).await?;
                    let previous = walk.len().checked_sub(2).map(|index| walk[index]);
                    let current = *walk.last().expect("walk start");
                    let Some(next) =
                        step(graph, previous, current, &options, &mut rng, scope).await?
                    else {
                        break;
                    };
                    walk.push(next);
                }
                for (position, &center) in walk.iter().enumerate() {
                    let lower = position.saturating_sub(options.window);
                    let upper = position
                        .saturating_add(options.window)
                        .saturating_add(1)
                        .min(walk.len());
                    for (other, &context) in walk[lower..upper].iter().enumerate() {
                        if lower + other == position {
                            continue;
                        }
                        update(
                            &mut input,
                            &mut output,
                            (center, context),
                            options.dimensions,
                            1.0,
                            learning_rate,
                        );
                        for _ in 0..options.negative_samples {
                            let target = if total == 0.0 {
                                rng.random_range(0..vertices)
                            } else {
                                let sample = rng.random::<f64>() * total;
                                negative
                                    .partition_point(|weight| *weight <= sample)
                                    .min(vertices - 1)
                            };
                            if target != context {
                                update(
                                    &mut input,
                                    &mut output,
                                    (center, target),
                                    options.dimensions,
                                    0.0,
                                    learning_rate,
                                );
                            }
                            steps += 1;
                            cooperate(scope, steps).await?;
                        }
                    }
                }
                tokio::task::yield_now().await;
            }
        }
    }
    ensure!(
        input.iter().all(|value| value.is_finite()),
        "Node2Vec produced non-finite embeddings"
    );
    Ok(input)
}

pub(super) fn workspace(vertices: usize, options: Node2VecOptions) -> usize {
    vertices
        .saturating_mul(options.dimensions)
        .saturating_mul(2 * std::mem::size_of::<f32>())
        .saturating_add(vertices.saturating_mul(2 * std::mem::size_of::<f64>()))
        .saturating_add(
            options
                .walk_length
                .saturating_mul(std::mem::size_of::<usize>()),
        )
}

async fn step(
    graph: &Graph,
    previous: Option<usize>,
    current: usize,
    options: &Node2VecOptions,
    rng: &mut SmallRng,
    scope: &QueryScope,
) -> Result<Option<usize>> {
    // Weighted reservoir sampling needs one pass and no per-node alias tables.
    let mut total = 0.0;
    let mut selected = None;
    for (i, neighbor) in graph.adjacency[current].iter().enumerate() {
        let bias = match previous {
            Some(previous) if previous == neighbor.vertex => options.return_bias,
            Some(previous)
                if !graph.adjacency[previous]
                    .binary_search_by_key(&neighbor.vertex, |arc| arc.vertex)
                    .is_ok() =>
            {
                options.explore_bias
            }
            _ => 1.0,
        };
        let weight = graph.links[neighbor.edge].weight / bias;
        total += weight;
        ensure!(
            total.is_finite(),
            "Node2Vec transition weight exceeds Float64"
        );
        if weight > 0.0 && rng.random::<f64>() * total < weight {
            selected = Some(neighbor.vertex);
        }
        cooperate(scope, i + 1).await?;
    }
    Ok(selected)
}

fn update(
    input: &mut [f32],
    output: &mut [f32],
    vertices: (usize, usize),
    dimensions: usize,
    expected: f32,
    rate: f32,
) {
    let (a, b) = (vertices.0 * dimensions, vertices.1 * dimensions);
    let left = &mut input[a..a + dimensions];
    let right = &mut output[b..b + dimensions];
    let dot = left
        .iter()
        .zip(right.iter())
        .map(|(x, y)| x * y)
        .sum::<f32>()
        .clamp(-16.0, 16.0);
    let gradient = (expected - 1.0 / (1.0 + (-dot).exp())) * rate;
    for (x, y) in left.iter_mut().zip(right) {
        let before = *x;
        *x += gradient * *y;
        *y += gradient * before;
    }
}
