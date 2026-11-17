use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    pin::Pin,
};

use futures::TryStreamExt;

use boltr::types::{
    BoltDict, BoltNode, BoltPath, BoltRelationship, BoltUnboundRelationship, BoltValue,
};
use sha2::{Digest, Sha256};

use super::super::query::{
    explore::exploration_key_match,
    graph::{Identity, Kind, identity, text_key},
    paths::list,
    query_error,
};
use super::{ApiResult, Error, result_limit, value_at};
use crate::{
    Catalog, QueryResult, ScalarValue,
    arrow::array::{Array, ArrayRef, RecordBatch, StructArray},
    catalog::ident,
    engine::query_graph::GraphColumn,
};

pub(super) type Hydrated = BTreeMap<Identity, (BoltValue, usize)>;

type Pending<'a, T> = Pin<Box<dyn Future<Output = ApiResult<T>> + Send + 'a>>;

fn invalid() -> Error {
    Error::invalid(
        "result_type",
        "Could not decode a graph element from the result",
    )
}

pub(super) fn value<'a>(
    catalog: &'a Catalog,
    result: &'a mut QueryResult,
    array: &'a ArrayRef,
    row: usize,
    shape: Option<&'a GraphColumn>,
    variant: bool,
    budget: &'a mut usize,
    prefetched: &'a Hydrated,
) -> Pending<'a, BoltValue> {
    Box::pin(async move {
        if array.is_null(row) {
            return Ok(BoltValue::Null);
        }
        if let Some(record) = array.as_any().downcast_ref::<StructArray>()
            && (variant || crate::engine::variant_fields(record.fields()))
        {
            let Some((field, column)) = record
                .fields()
                .iter()
                .zip(record.columns())
                .find(|(_, column)| column.is_valid(row))
            else {
                return Ok(BoltValue::Null);
            };
            let shape = match shape {
                Some(GraphColumn::Record(fields)) if variant => fields.get(field.name()),
                shape => shape,
            };
            return value(
                catalog, result, column, row, shape, false, budget, prefetched,
            )
            .await;
        }
        Ok(match shape {
            Some(GraphColumn::Entity { label, node }) => {
                let record = array
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .ok_or_else(invalid)?;
                if let Some(label) = label {
                    fields(
                        catalog,
                        result,
                        label,
                        *node,
                        row,
                        |name| record.column_by_name(name),
                        budget,
                    )
                    .await?
                } else {
                    let id = tagged(catalog, *node, record, row)?;
                    let properties = record
                        .column_by_name("properties")
                        .and_then(|value| value.as_any().downcast_ref::<StructArray>());
                    let values = property_values(
                        catalog,
                        &id,
                        row,
                        |name| properties.and_then(|record| record.column_by_name(name)),
                        budget,
                    )?;
                    materialize(catalog, result, id, values, None, budget).await?
                }
            }
            Some(GraphColumn::Path {
                labels: Some(labels),
            }) => {
                let record = array
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .ok_or_else(invalid)?;
                let nodes = list(
                    record.column_by_name("nodes").ok_or_else(invalid)?.as_ref(),
                    row,
                )
                .ok_or_else(invalid)?;
                let edges = list(
                    record
                        .column_by_name("relationships")
                        .ok_or_else(invalid)?
                        .as_ref(),
                    row,
                )
                .ok_or_else(invalid)?;
                path(
                    catalog,
                    result,
                    path_ids(catalog, &nodes, &edges, labels, *budget)?,
                    budget,
                    prefetched,
                )
                .await?
            }
            Some(GraphColumn::Path { labels: None }) => {
                // GraphSON Path permits vertices, edges, and scalar steps in the same list.
                let steps = list(array.as_ref(), row).ok_or_else(invalid)?;
                let record = steps
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .ok_or_else(invalid)?;
                let kinds = record.column_by_name("kind").ok_or_else(invalid)?;
                if steps.len() > *budget / std::mem::size_of::<BoltValue>() {
                    return Err(result_limit());
                }
                let steps = (0..record.len())
                    .map(|i| match text_key(kinds.as_ref(), i).as_deref() {
                        Some(kind @ ("vertex" | "edge")) => {
                            tagged(catalog, kind == "vertex", record, i).map(Some)
                        }
                        Some("value") => Ok(None),
                        _ => Err(invalid()),
                    })
                    .collect::<ApiResult<Vec<_>>>()?;
                let missing = steps
                    .iter()
                    .flatten()
                    .filter(|id| !prefetched.contains_key(*id))
                    .cloned()
                    .collect();
                let mut hydration_budget = *budget;
                let hydrated =
                    hydrate_many(catalog, result, missing, &mut hydration_budget).await?;
                let mut values = Vec::with_capacity(steps.len());
                for (i, step) in steps.iter().enumerate() {
                    match step {
                        Some(id) => {
                            let source = if prefetched.contains_key(id) {
                                prefetched
                            } else {
                                &hydrated
                            };
                            values.push(cached(source, id, budget)?.ok_or_else(invalid)?);
                        }
                        None => {
                            let keys = list(
                                record.column_by_name("key").ok_or_else(invalid)?.as_ref(),
                                i,
                            )
                            .ok_or_else(invalid)?;
                            values.push(value_at(&keys, 0, 0, false, budget)?);
                        }
                    }
                }
                // The Gremlin encoder recognizes this only when produced from a Path column.
                BoltValue::UnboundRelationship(BoltUnboundRelationship {
                    id: 0,
                    element_id: String::new(),
                    rel_type: String::new(),
                    properties: BoltDict::from([("objects".into(), BoltValue::List(values))]),
                })
            }
            Some(GraphColumn::List(item)) => {
                // A result list is not subject to the canvas path-length limit.
                let items = match ScalarValue::try_from_array(array, row).map_err(|_| invalid())? {
                    ScalarValue::List(items) => items.value(0),
                    ScalarValue::LargeList(items) => items.value(0),
                    ScalarValue::FixedSizeList(items) => items.value(0),
                    _ => return Err(invalid()),
                };
                if items.len() > *budget / std::mem::size_of::<BoltValue>() {
                    return Err(result_limit());
                }
                let mut values = Vec::new();
                for i in 0..items.len() {
                    values.push(
                        value(
                            catalog,
                            result,
                            &items,
                            i,
                            Some(item),
                            false,
                            budget,
                            prefetched,
                        )
                        .await?,
                    );
                }
                BoltValue::List(values)
            }
            Some(GraphColumn::Record(shapes)) => {
                let record = array
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .ok_or_else(invalid)?;
                let mut values = BoltDict::new();
                for (field, column) in record.fields().iter().zip(record.columns()) {
                    values.insert(
                        field.name().clone(),
                        value(
                            catalog,
                            result,
                            column,
                            row,
                            shapes.get(field.name()),
                            false,
                            budget,
                            prefetched,
                        )
                        .await?,
                    );
                }
                BoltValue::Dict(values)
            }
            None => value_at(array, row, 0, variant, budget)?,
        })
    })
}

fn property_values<'a>(
    catalog: &Catalog,
    id: &Identity,
    row: usize,
    get: impl Fn(&str) -> Option<&'a ArrayRef>,
    budget: &mut usize,
) -> ApiResult<BoltDict> {
    let keys = key_names(catalog, id.kind == Kind::Node, &id.label);
    let names = if id.kind == Kind::Node {
        &catalog.nodes[&id.label].properties
    } else {
        &catalog.edges[&id.label].properties
    };
    keys.iter()
        .chain(names)
        .filter_map(|name| Some((name, get(name)?)))
        .filter(|(_, array)| array.is_valid(row))
        .map(|(name, array)| Ok((name.clone(), value_at(array, row, 0, false, budget)?)))
        .collect()
}

fn key_names(catalog: &Catalog, node: bool, label: &str) -> Vec<String> {
    if node {
        catalog.nodes[label].key.clone()
    } else {
        catalog.edges[label].identity()
    }
}

fn from_fields<'a>(
    catalog: &Catalog,
    label: &str,
    node: bool,
    row: usize,
    names: &[String],
    get: impl Fn(&str) -> Option<&'a ArrayRef>,
) -> Option<Identity> {
    let keys = names
        .iter()
        .map(|name| text_key(get(name)?.as_ref(), row))
        .collect::<Option<_>>()?;
    identity(catalog, node, label.into(), keys)
}

fn tagged(catalog: &Catalog, node: bool, record: &StructArray, row: usize) -> ApiResult<Identity> {
    let label = record
        .column_by_name("label")
        .and_then(|array| text_key(array.as_ref(), row))
        .ok_or_else(invalid)?;
    let keys = list(
        record.column_by_name("key").ok_or_else(invalid)?.as_ref(),
        row,
    )
    .ok_or_else(invalid)?;
    let keys = (0..keys.len())
        .map(|i| text_key(keys.as_ref(), i))
        .collect::<Option<_>>()
        .ok_or_else(invalid)?;
    identity(catalog, node, label, keys).ok_or_else(invalid)
}

pub(super) async fn fields<'a>(
    catalog: &Catalog,
    result: &mut QueryResult,
    label: &str,
    node: bool,
    row: usize,
    get: impl Fn(&str) -> Option<&'a ArrayRef> + Send + Sync,
    budget: &mut usize,
) -> ApiResult<BoltValue> {
    let id = from_fields(
        catalog,
        label,
        node,
        row,
        &key_names(catalog, node, label),
        &get,
    )
    .ok_or_else(invalid)?;
    let values = property_values(catalog, &id, row, &get, budget)?;
    let ends = (!node)
        .then(|| endpoints(catalog, label, row, get))
        .flatten();
    materialize(catalog, result, id, values, ends, budget).await
}

fn endpoints<'a>(
    catalog: &Catalog,
    label: &str,
    row: usize,
    get: impl Fn(&str) -> Option<&'a ArrayRef>,
) -> Option<(Identity, Identity)> {
    let edge = &catalog.edges[label];
    Some((
        from_fields(
            catalog,
            &edge.from.node,
            true,
            row,
            &edge.from.columns,
            &get,
        )?,
        from_fields(catalog, &edge.to.node, true, row, &edge.to.columns, &get)?,
    ))
}

async fn materialize(
    catalog: &Catalog,
    result: &mut QueryResult,
    id: Identity,
    values: BoltDict,
    ends: Option<(Identity, Identity)>,
    budget: &mut usize,
) -> ApiResult<BoltValue> {
    if id.kind == Kind::Node {
        Ok(BoltValue::Node(node(&id, values, budget)?))
    } else if let Some(ends) = ends {
        Ok(BoltValue::Relationship(edge(&id, values, ends, budget)?))
    } else {
        hydrate(catalog, result, &id, budget).await
    }
}

async fn hydrate(
    catalog: &Catalog,
    result: &mut QueryResult,
    id: &Identity,
    budget: &mut usize,
) -> ApiResult<BoltValue> {
    hydrate_many(catalog, result, BTreeSet::from([id.clone()]), budget)
        .await?
        .remove(id)
        .map(|(value, _)| value)
        .ok_or_else(invalid)
}

async fn hydrate_many(
    catalog: &Catalog,
    result: &mut QueryResult,
    identities: BTreeSet<Identity>,
    budget: &mut usize,
) -> ApiResult<Hydrated> {
    if identities.is_empty() {
        return Ok(BTreeMap::new());
    }
    let context = result.graph_context.clone().ok_or_else(invalid)?;
    let mut groups: BTreeMap<_, Vec<_>> = BTreeMap::new();
    for id in identities {
        groups
            .entry((id.kind, id.label.clone()))
            .or_default()
            .push(id);
    }
    let mut hydrated = BTreeMap::new();
    for ((kind, label), ids) in groups {
        let node_type = kind == Kind::Node;
        let keys = key_names(catalog, node_type, &label);
        let provider = context
            .table_provider(datafusion::common::TableReference::bare(label.as_str()))
            .await
            .map_err(|error| query_error(error.into()))?;
        let schema = provider.schema();
        for group in ids.chunks((128 / keys.len()).max(1)) {
            let mut batches = result
                .graph_read(async {
                    let mut values = Vec::with_capacity(group.len() * keys.len());
                    let conditions = group
                        .iter()
                        .map(|id| exploration_key_match("e", &keys, &id.key, &schema, &mut values))
                        .collect::<anyhow::Result<Vec<_>>>()?
                        .join(" OR ");
                    let sql = format!("SELECT e.* FROM {} AS e WHERE {conditions}", ident(&label));
                    Ok(context
                        .sql(&sql)
                        .await?
                        .with_param_values(values)?
                        .execute_stream()
                        .await?)
                })
                .await
                .map_err(query_error)?;
            while let Some(batch) = result
                .graph_read(async { Ok(batches.try_next().await?) })
                .await
                .map_err(query_error)?
            {
                for row in 0..batch.num_rows() {
                    let get = |name: &str| batch.column_by_name(name);
                    let id = from_fields(catalog, &label, node_type, row, &keys, get)
                        .ok_or_else(invalid)?;
                    let before = *budget;
                    let properties = property_values(catalog, &id, row, get, budget)?;
                    let value = if node_type {
                        BoltValue::Node(node(&id, properties, budget)?)
                    } else {
                        let ends = endpoints(catalog, &label, row, get).ok_or_else(invalid)?;
                        BoltValue::Relationship(edge(&id, properties, ends, budget)?)
                    };
                    hydrated.insert(id, (value, before - *budget));
                }
            }
        }
    }
    Ok(hydrated)
}

fn encode_id(id: &Identity, budget: &mut usize) -> ApiResult<(i64, String)> {
    let mut text = String::new();
    crate::cypher::escape_id_component(&id.label, &mut text);
    text.push('[');
    for (i, part) in id.key.iter().enumerate() {
        if i != 0 {
            text.push(',');
        }
        crate::cypher::escape_id_component(part, &mut text);
    }
    text.push(']');
    *budget = budget
        .checked_sub(text.len() + std::mem::size_of::<BoltValue>())
        .ok_or_else(result_limit)?;
    let digest = Sha256::digest(text.as_bytes());
    let number = i64::from_be_bytes(digest[..8].try_into().unwrap()) & i64::MAX;
    Ok((number, text))
}

fn node(id: &Identity, properties: BoltDict, budget: &mut usize) -> ApiResult<BoltNode> {
    let (number, element_id) = encode_id(id, budget)?;
    Ok(BoltNode {
        id: number,
        element_id,
        labels: vec![id.label.clone()],
        properties,
    })
}

fn edge(
    id: &Identity,
    properties: BoltDict,
    (from, to): (Identity, Identity),
    budget: &mut usize,
) -> ApiResult<BoltRelationship> {
    let (number, element_id) = encode_id(id, budget)?;
    let (start_node_id, start_element_id) = encode_id(&from, budget)?;
    let (end_node_id, end_element_id) = encode_id(&to, budget)?;
    Ok(BoltRelationship {
        id: number,
        element_id,
        rel_type: id.label.clone(),
        properties,
        start_node_id,
        start_element_id,
        end_node_id,
        end_element_id,
    })
}

fn path_ids(
    catalog: &Catalog,
    nodes: &ArrayRef,
    edges: &ArrayRef,
    labels: &(Option<String>, Option<String>),
    budget: usize,
) -> ApiResult<(Vec<Identity>, Vec<Identity>)> {
    if nodes.len() != edges.len() + 1 {
        return Err(invalid());
    }
    let read = |array: &ArrayRef, label: &Option<String>, node, row| {
        let record = array
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(invalid)?;
        match label {
            Some(label) => from_fields(
                catalog,
                label,
                node,
                row,
                &key_names(catalog, node, label),
                |name| record.column_by_name(name),
            )
            .ok_or_else(invalid),
            None => tagged(catalog, node, record, row),
        }
    };
    if nodes
        .len()
        .saturating_add(edges.len())
        .saturating_mul(std::mem::size_of::<Identity>())
        > budget
    {
        return Err(result_limit());
    }
    let nodes = (0..nodes.len())
        .map(|row| read(nodes, &labels.0, true, row))
        .collect::<ApiResult<Vec<_>>>()?;
    let edges = (0..edges.len())
        .map(|row| read(edges, &labels.1, false, row))
        .collect::<ApiResult<Vec<_>>>()?;
    Ok((nodes, edges))
}

fn cached(values: &Hydrated, id: &Identity, budget: &mut usize) -> ApiResult<Option<BoltValue>> {
    let Some((value, bytes)) = values.get(id) else {
        return Ok(None);
    };
    *budget = budget.checked_sub(*bytes).ok_or_else(result_limit)?;
    Ok(Some(value.clone()))
}

pub(super) async fn prefetch_paths(
    catalog: &Catalog,
    result: &mut QueryResult,
    batch: &RecordBatch,
    rows: std::ops::Range<usize>,
) -> ApiResult<Hydrated> {
    let mut ids = BTreeSet::new();
    for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
        let Some(GraphColumn::Path { labels }) = result.graph_output.columns.get(field.name())
        else {
            continue;
        };
        for row in rows.clone() {
            if column.is_null(row) {
                continue;
            }
            if let Some(labels) = labels {
                let record = column
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .ok_or_else(invalid)?;
                let nodes = list(
                    record.column_by_name("nodes").ok_or_else(invalid)?.as_ref(),
                    row,
                )
                .ok_or_else(invalid)?;
                let edges = list(
                    record
                        .column_by_name("relationships")
                        .ok_or_else(invalid)?
                        .as_ref(),
                    row,
                )
                .ok_or_else(invalid)?;
                let (nodes, edges) = path_ids(catalog, &nodes, &edges, labels, super::ROW_BYTES)?;
                ids.extend(nodes);
                ids.extend(edges);
            } else {
                let steps = list(column.as_ref(), row).ok_or_else(invalid)?;
                let record = steps
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .ok_or_else(invalid)?;
                let kinds = record.column_by_name("kind").ok_or_else(invalid)?;
                for i in 0..steps.len() {
                    if let Some(kind @ ("vertex" | "edge")) = text_key(kinds.as_ref(), i).as_deref()
                    {
                        ids.insert(tagged(catalog, kind == "vertex", record, i)?);
                    }
                }
            }
            if ids.len() > 512 {
                return Ok(BTreeMap::new());
            }
        }
    }
    if ids.is_empty() {
        return Ok(BTreeMap::new());
    }
    hydrate_many(catalog, result, ids, &mut (2 * super::ROW_BYTES)).await
}

async fn path(
    catalog: &Catalog,
    result: &mut QueryResult,
    (nodes, edges): (Vec<Identity>, Vec<Identity>),
    budget: &mut usize,
    prefetched: &Hydrated,
) -> ApiResult<BoltValue> {
    let unique = nodes.iter().chain(&edges).cloned().collect::<BTreeSet<_>>();
    let missing = unique
        .iter()
        .filter(|id| !prefetched.contains_key(*id))
        .cloned()
        .collect();
    let mut hydrated = hydrate_many(catalog, result, missing, budget).await?;
    for id in unique {
        if let Some(value) = cached(prefetched, &id, budget)? {
            hydrated.insert(id, (value, 0));
        }
    }
    let mut path = BoltPath {
        nodes: Vec::new(),
        rels: Vec::new(),
        indices: Vec::new(),
    };
    let mut node_indices = BTreeMap::new();
    let mut edge_indices = BTreeMap::new();
    let mut visits = Vec::new();
    for id in nodes {
        let index = if let Some(&index) = node_indices.get(&id) {
            index
        } else {
            let Some((BoltValue::Node(node), _)) = hydrated.remove(&id) else {
                return Err(invalid());
            };
            let index = path.nodes.len();
            path.nodes.push(node);
            node_indices.insert(id, index);
            index
        };
        visits.push(index);
    }
    for (row, id) in edges.into_iter().enumerate() {
        let (index, start) = if let Some(&entry) = edge_indices.get(&id) {
            entry
        } else {
            let Some((BoltValue::Relationship(edge), _)) = hydrated.remove(&id) else {
                return Err(invalid());
            };
            let entry = (path.rels.len(), edge.start_node_id);
            path.rels.push(BoltUnboundRelationship {
                id: edge.id,
                element_id: edge.element_id,
                rel_type: edge.rel_type,
                properties: edge.properties,
            });
            edge_indices.insert(id, entry);
            entry
        };
        let sign = if path.nodes[visits[row]].id == start {
            1
        } else {
            -1
        };
        path.indices
            .extend([sign * (index as i64 + 1), visits[row + 1] as i64]);
    }
    Ok(BoltValue::Path(path))
}
