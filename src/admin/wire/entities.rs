use std::{collections::BTreeMap, future::Future, pin::Pin};

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
            return value(catalog, result, column, row, shape, false, budget).await;
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
                path(catalog, result, &nodes, &edges, labels, budget).await?
            }
            Some(GraphColumn::Path { labels: None }) => {
                // GraphSON Path permits vertices, edges, and scalar steps in the same list.
                let steps = list(array.as_ref(), row).ok_or_else(invalid)?;
                let record = steps
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .ok_or_else(invalid)?;
                let kinds = record.column_by_name("kind").ok_or_else(invalid)?;
                let mut values = Vec::new();
                for i in 0..steps.len() {
                    match text_key(kinds.as_ref(), i).as_deref() {
                        Some("vertex" | "edge") => {
                            let node = text_key(kinds.as_ref(), i).as_deref() == Some("vertex");
                            let id = tagged(catalog, node, record, i)?;
                            values.push(hydrate(catalog, result, &id, budget).await?);
                        }
                        Some("value") => {
                            let keys = list(
                                record.column_by_name("key").ok_or_else(invalid)?.as_ref(),
                                i,
                            )
                            .ok_or_else(invalid)?;
                            values.push(value_at(&keys, 0, 0, false, budget)?);
                        }
                        _ => return Err(invalid()),
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
                    values
                        .push(value(catalog, result, &items, i, Some(item), false, budget).await?);
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
    let node_type = id.kind == Kind::Node;
    let keys = key_names(catalog, node_type, &id.label);
    let context = result.graph_context.clone().ok_or_else(invalid)?;
    let batches: Vec<RecordBatch> = result
        .graph_read(async {
            let schema = context
                .table_provider(datafusion::common::TableReference::bare(id.label.as_str()))
                .await?
                .schema();
            let mut values = Vec::with_capacity(keys.len());
            let conditions = exploration_key_match("e", &keys, &id.key, &schema, &mut values)?;
            let sql = format!(
                "SELECT e.* FROM {} e WHERE {conditions} LIMIT 1",
                ident(&id.label)
            );
            Ok(context
                .sql(&sql)
                .await?
                .with_param_values(values)?
                .collect()
                .await?)
        })
        .await
        .map_err(query_error)?;
    let batch = batches
        .iter()
        .find(|batch| batch.num_rows() != 0)
        .ok_or_else(invalid)?;
    let values = property_values(catalog, id, 0, |name| batch.column_by_name(name), budget)?;
    Ok(if node_type {
        BoltValue::Node(node(id, values, budget)?)
    } else {
        let ends = endpoints(catalog, &id.label, 0, |name| batch.column_by_name(name))
            .ok_or_else(invalid)?;
        BoltValue::Relationship(edge(id, values, ends, budget)?)
    })
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

async fn path(
    catalog: &Catalog,
    result: &mut QueryResult,
    nodes: &ArrayRef,
    edges: &ArrayRef,
    labels: &(Option<String>, Option<String>),
    budget: &mut usize,
) -> ApiResult<BoltValue> {
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
    let mut path = BoltPath {
        nodes: Vec::new(),
        rels: Vec::new(),
        indices: Vec::new(),
    };
    let mut node_indices = BTreeMap::new();
    let mut edge_indices = BTreeMap::new();
    let mut visits = Vec::new();
    for row in 0..nodes.len() {
        let id = read(nodes, &labels.0, true, row)?;
        let index = if let Some(&index) = node_indices.get(&id) {
            index
        } else {
            let BoltValue::Node(node) = hydrate(catalog, result, &id, budget).await? else {
                return Err(invalid());
            };
            let index = path.nodes.len();
            path.nodes.push(node);
            node_indices.insert(id, index);
            index
        };
        visits.push(index);
    }
    for row in 0..edges.len() {
        let id = read(edges, &labels.1, false, row)?;
        let (index, start) = if let Some(&entry) = edge_indices.get(&id) {
            entry
        } else {
            let BoltValue::Relationship(edge) = hydrate(catalog, result, &id, budget).await? else {
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
