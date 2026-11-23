use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    mem::size_of,
    sync::Arc,
};

use anyhow::{Context, Result, ensure};
use bigdecimal::{BigDecimal, Signed};
use datafusion::{
    arrow::{
        array::{Array, Float64Array},
        datatypes::{DataType, SchemaRef},
        row::RowConverter,
    },
    common::TableReference,
    execution::memory_pool::MemoryReservation,
    prelude::SessionContext,
};
use futures::TryStreamExt;

use crate::traversal::{
    COOPERATIVE_INTERVAL, Direction,
    topology::{Neighbor, coerce_key, encode_key, row_converter},
};
use crate::{
    Catalog, ScalarValue,
    catalog::{ident, qualified},
    engine::QueryScope,
};

use super::{GraphProjection, VertexKey};

pub(super) struct Identity {
    pub(super) label: Arc<str>,
    pub(super) key: Vec<String>,
}

pub(super) struct Link {
    pub(super) from: usize,
    pub(super) to: usize,
    pub(super) weight: f64,
}

#[derive(Default)]
pub(super) struct Graph {
    pub(super) vertices: Vec<Identity>,
    pub(super) links: Vec<Link>,
    pub(super) adjacency: Vec<Vec<Neighbor>>,
    pub(super) route_edges: Vec<Identity>,
    pub(super) route_weights: Vec<BigDecimal>,
    pub(super) route_cost_bytes: usize,
    indexes: BTreeMap<String, Index>,
}

pub(super) enum GraphRead {
    Edges,
    Adjacency,
    Routes { signed: bool },
}

struct Index {
    schema: SchemaRef,
    converter: RowConverter,
    ids: HashMap<Box<[u8]>, usize>,
}

pub(super) struct Labels {
    pub(super) nodes: BTreeSet<String>,
    pub(super) edges: BTreeSet<String>,
}

impl Labels {
    pub(super) fn new(catalog: &Catalog, projection: &GraphProjection) -> Result<Self> {
        let mut nodes: BTreeSet<_> = projection.nodes.iter().cloned().collect();
        let edges: BTreeSet<_> = projection.edges.iter().cloned().collect();
        ensure!(
            !edges.is_empty() || !nodes.is_empty(),
            "select at least one node or edge label"
        );
        for node in &nodes {
            ensure!(
                catalog.nodes.contains_key(node),
                "unknown projected node {node}"
            );
        }
        for name in &edges {
            let edge = catalog
                .edges
                .get(name)
                .with_context(|| format!("unknown projected edge {name}"))?;
            nodes.extend([edge.from.node.clone(), edge.to.node.clone()]);
            if let Some(weight) = &projection.weight {
                ensure!(
                    edge.properties.contains(weight) || edge.key.contains(weight),
                    "projected edge {name} has no mapped weight column {weight}"
                );
            }
        }
        Ok(Self { nodes, edges })
    }

    pub(super) fn all(&self) -> BTreeSet<String> {
        self.nodes.union(&self.edges).cloned().collect()
    }
}

impl Graph {
    pub(super) async fn read(
        catalog: &Catalog,
        projection: &GraphProjection,
        labels: &Labels,
        context: &SessionContext,
        scope: &QueryScope,
        budget: &MemoryReservation,
        read: GraphRead,
    ) -> Result<Self> {
        let routes = matches!(read, GraphRead::Routes { .. });
        let needs_adjacency = !matches!(read, GraphRead::Edges);
        let transient_ids = budget.new_empty();
        let mut vertices = Vec::new();
        let mut indexes = BTreeMap::new();
        for name in &labels.nodes {
            let label: Arc<str> = Arc::from(name.as_str());
            let columns = catalog.nodes[name]
                .key
                .iter()
                .map(|key| qualified("v", key))
                .collect::<Vec<_>>()
                .join(", ");
            let mut rows = context
                .sql(&format!(
                    "SELECT {columns} FROM {} AS v ORDER BY {columns}",
                    ident(name)
                ))
                .await?
                .execute_stream()
                .await?;
            let schema = rows.schema();
            let converter = row_converter(&schema)?;
            let mut ids = HashMap::new();
            while let Some(batch) = rows.try_next().await? {
                let encoded = converter.convert_columns(batch.columns())?;
                for (i, row) in encoded.iter().enumerate() {
                    let key = key_text(batch.columns(), i)?;
                    let index_bytes = row.data().len().saturating_add(64);
                    let identity = identity_bytes(&key).saturating_add(64);
                    if routes {
                        budget.try_grow(index_bytes)?;
                    } else {
                        transient_ids.try_grow(index_bytes)?;
                    }
                    budget.try_grow(identity)?;
                    ensure!(
                        ids.insert(row.data().into(), vertices.len()).is_none(),
                        "duplicate business key in {name}"
                    );
                    vertices.push(Identity {
                        label: Arc::clone(&label),
                        key,
                    });
                    if i % COOPERATIVE_INTERVAL == 0 {
                        scope.check()?;
                        tokio::task::yield_now().await;
                    }
                }
            }
            indexes.insert(
                name.clone(),
                Index {
                    schema,
                    converter,
                    ids,
                },
            );
        }
        let mut adjacency = Vec::new();
        let mut link_bytes = 2 * size_of::<Link>();
        if needs_adjacency {
            budget.try_grow(vertices.len().saturating_mul(size_of::<Vec<Neighbor>>()))?;
            adjacency.resize_with(vertices.len(), Vec::new);
            // An undirected edge can start two new adjacency lists, each with capacity four.
            link_bytes += 8 * size_of::<Neighbor>();
        }
        let mut links = Vec::new();
        let mut route_edges = Vec::new();
        let mut route_weights = Vec::new();
        let mut largest_integer_bits = 0_u64;
        let mut largest_scale = 0_i64;
        for name in &labels.edges {
            let label: Arc<str> = Arc::from(name.as_str());
            let edge = &catalog.edges[name];
            let from_index = &indexes[&edge.from.node];
            let to_index = &indexes[&edge.to.node];
            let from = edge.from.columns.iter().map(|key| qualified("e", key));
            let to = edge.to.columns.iter().map(|key| qualified("e", key));
            let identity = edge.identity();
            let keys = identity
                .iter()
                .map(|key| qualified("e", key))
                .collect::<Vec<_>>();
            let weight = if let Some(field) = &projection.weight {
                let table = context.table(TableReference::bare(name.as_str())).await?;
                let kind = table
                    .schema()
                    .field_with_unqualified_name(field)?
                    .data_type();
                let kind = match kind {
                    DataType::Dictionary(_, values) => values.as_ref(),
                    kind => kind,
                };
                ensure!(
                    kind.is_numeric(),
                    "edge {name} weight {field} must be numeric"
                );
                qualified("e", field)
            } else {
                "1".into()
            };
            let weight = if routes {
                weight
            } else {
                format!("CAST({weight} AS DOUBLE)")
            };
            let columns = from
                .chain(to)
                .chain(keys.iter().cloned())
                .enumerate()
                .map(|(i, col)| format!("{col} AS __graphlake_project_{i}"))
                .collect::<Vec<_>>();
            let order = keys.join(", ");
            let sql = format!(
                "SELECT {}, {weight} AS __graphlake_weight FROM {} AS e ORDER BY {order}",
                columns.join(", "),
                ident(name)
            );
            let mut rows = context.sql(&sql).await?.execute_stream().await?;
            let from_width = edge.from.columns.len();
            let to_width = from_width + edge.to.columns.len();
            let id_width = to_width + identity.len();
            let id_converter = row_converter(
                &rows
                    .schema()
                    .project(&(to_width..id_width).collect::<Vec<_>>())?,
            )?;
            let mut previous = Vec::new();
            while let Some(batch) = rows.try_next().await? {
                let columns = batch.columns();
                let from = from_index
                    .converter
                    .convert_columns(&columns[..from_width])?;
                let to = to_index
                    .converter
                    .convert_columns(&columns[from_width..to_width])?;
                let edge_ids = id_converter.convert_columns(&columns[to_width..id_width])?;
                for (i, ((from, to), id)) in
                    from.iter().zip(to.iter()).zip(edge_ids.iter()).enumerate()
                {
                    if i % COOPERATIVE_INTERVAL == 0 {
                        scope.check()?;
                        tokio::task::yield_now().await;
                    }
                    ensure!(
                        previous.as_slice() != id.data(),
                        "duplicate business key in edge {name}"
                    );
                    previous.clear();
                    previous.extend_from_slice(id.data());
                    let (Some(&from), Some(&to)) =
                        (from_index.ids.get(from.data()), to_index.ids.get(to.data()))
                    else {
                        continue;
                    };
                    let weight = if let GraphRead::Routes { signed } = read {
                        let value = route_weight(
                            ScalarValue::try_from_array(&columns[id_width], i)?,
                            signed,
                        )
                        .with_context(|| format!("invalid route weight on edge {name}"))?;
                        let (coefficient, scale) = value.as_bigint_and_scale();
                        let bits = coefficient.bits();
                        largest_integer_bits =
                            largest_integer_bits.max(bits + (-scale).max(0) as u64 * 4);
                        largest_scale = largest_scale.max(scale);
                        budget.try_grow(64 + bits.div_ceil(8) as usize)?;
                        route_weights.push(value);
                        let key = key_text(&columns[to_width..id_width], i)?;
                        budget.try_grow(
                            identity_bytes(&key).saturating_add(2 * size_of::<Identity>()),
                        )?;
                        route_edges.push(Identity {
                            label: Arc::clone(&label),
                            key,
                        });
                        0.0
                    } else {
                        let weights = columns[id_width]
                            .as_any()
                            .downcast_ref::<Float64Array>()
                            .context("invalid projected weight array")?;
                        let weight = weights.value(i);
                        ensure!(
                            !weights.is_null(i) && weight.is_finite() && weight >= 0.0,
                            "edge {name} weight must be finite, non-negative and non-NULL"
                        );
                        weight
                    };
                    budget.try_grow(link_bytes)?;
                    let id = links.len();
                    links.push(Link { from, to, weight });
                    if !needs_adjacency {
                        continue;
                    }
                    match projection.direction {
                        Direction::Outgoing => adjacency[from].push(Neighbor {
                            vertex: to,
                            edge: id,
                        }),
                        Direction::Incoming => adjacency[to].push(Neighbor {
                            vertex: from,
                            edge: id,
                        }),
                        Direction::Undirected => {
                            adjacency[from].push(Neighbor {
                                vertex: to,
                                edge: id,
                            });
                            if from != to {
                                adjacency[to].push(Neighbor {
                                    vertex: from,
                                    edge: id,
                                });
                            }
                        }
                    }
                }
            }
        }
        for (i, neighbors) in adjacency.iter_mut().enumerate() {
            neighbors.sort_unstable_by_key(|neighbor| (neighbor.vertex, neighbor.edge));
            if i.is_multiple_of(2048) {
                scope.check()?;
                tokio::task::yield_now().await;
            }
        }
        if !routes {
            indexes.clear();
        }
        Ok(Self {
            vertices,
            links,
            adjacency,
            route_edges,
            route_weights,
            route_cost_bytes: 64
                + (largest_integer_bits + largest_scale as u64 * 4 + 64).div_ceil(8) as usize,
            indexes,
        })
    }

    pub(super) fn vertex(&self, key: &VertexKey) -> Result<usize> {
        let index = self
            .indexes
            .get(&key.label)
            .with_context(|| format!("node label {} is outside the projection", key.label))?;
        let values = coerce_key(&key.key, &index.schema)?;
        let encoded = encode_key(&values, &index.schema, &index.converter)?;
        index
            .ids
            .get(encoded.as_slice())
            .copied()
            .with_context(|| format!("vertex {} does not exist in the projection", key.label))
    }
}

fn route_weight(value: ScalarValue, signed: bool) -> Result<BigDecimal> {
    let weight = crate::scalar::finite_decimal(&value)
        .context("weight must be a finite, non-NULL number")?;
    ensure!(
        signed || !weight.is_negative(),
        "weight must be non-negative"
    );
    Ok(weight)
}

fn key_text(columns: &[datafusion::arrow::array::ArrayRef], row: usize) -> Result<Vec<String>> {
    columns
        .iter()
        .map(|column| {
            let value = ScalarValue::try_from_array(column, row)?;
            ensure!(!value.is_null(), "projected business keys must be non-NULL");
            Ok(value.to_string())
        })
        .collect()
}

fn identity_bytes(key: &[String]) -> usize {
    key.iter().fold(0_usize, |bytes, value| {
        bytes.saturating_add(value.capacity() + std::mem::size_of::<String>())
    })
}
