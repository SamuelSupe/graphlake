use crate::arrow::{
    array::{Array, ArrayRef, LargeListArray, ListArray, StructArray},
    util::display::{ArrayFormatter, FormatOptions},
};

use super::*;

#[derive(Serialize)]
pub(super) struct PathPreview {
    row: usize,
    column: String,
    items: Vec<Item>,
}

#[derive(Serialize)]
struct Item {
    kind: &'static str,
    label: String,
    key: String,
}

pub(in crate::admin) fn list(array: &dyn Array, row: usize) -> Option<ArrayRef> {
    if array.is_null(row) {
        return None;
    }
    if let Some(list) = array.as_any().downcast_ref::<ListArray>() {
        (list.value_length(row) <= 513).then(|| list.value(row))
    } else {
        let list = array.as_any().downcast_ref::<LargeListArray>()?;
        (list.value_length(row) <= 513).then(|| list.value(row))
    }
}

fn text(array: &dyn Array, row: usize) -> Option<String> {
    if array.is_null(row) {
        return None;
    }
    let format = FormatOptions::default();
    let formatter = ArrayFormatter::try_new(array, &format).ok()?;
    let mut output = CellText {
        value: String::new(),
        limit: 256,
        exceeded: false,
    };
    formatter.value(row).write(&mut output).ok()?;
    Some(output.value)
}

fn item(array: &dyn Array, index: usize, label: Option<&str>, kind: &'static str) -> Option<Item> {
    let (label, key) = if let Some(label) = label {
        (label.to_owned(), text(array, index)?)
    } else {
        let record = array.as_any().downcast_ref::<StructArray>()?;
        (
            text(record.column_by_name("label")?.as_ref(), index)?,
            text(record.column_by_name("key")?.as_ref(), index)?,
        )
    };
    Some(Item { kind, label, key })
}

pub(super) fn identities(
    catalog: &crate::Catalog,
    array: &dyn Array,
    row: usize,
    labels: Option<&(Option<String>, Option<String>)>,
) -> Option<Vec<Option<super::graph::Identity>>> {
    use super::graph::{identity, text_key};
    let read = |array: &dyn Array, index, label: Option<&str>, node| {
        if let Some(label) = label {
            let record = array.as_any().downcast_ref::<StructArray>()?;
            let keys = if node {
                catalog.nodes.get(label)?.key.clone()
            } else {
                catalog.edges.get(label)?.identity()
            };
            let keys = keys
                .iter()
                .map(|key| text_key(record.column_by_name(key)?.as_ref(), index))
                .collect::<Option<_>>()?;
            identity(catalog, node, label.to_owned(), keys)
        } else {
            let record = array.as_any().downcast_ref::<StructArray>()?;
            let label = text_key(record.column_by_name("label")?.as_ref(), index)?;
            let key = list(record.column_by_name("key")?.as_ref(), index)?;
            let key = (0..key.len())
                .map(|i| text_key(key.as_ref(), i))
                .collect::<Option<_>>()?;
            identity(catalog, node, label, key)
        }
    };
    if array.is_null(row) {
        return None;
    }
    let mut items = Vec::new();
    if let Some((node_label, edge_label)) = labels {
        let record = array.as_any().downcast_ref::<StructArray>()?;
        let nodes = list(record.column_by_name("nodes")?.as_ref(), row)?;
        let edges = list(record.column_by_name("relationships")?.as_ref(), row)?;
        if nodes.len() != edges.len() + 1 {
            return None;
        }
        for index in 0..nodes.len() {
            items.push(Some(read(
                nodes.as_ref(),
                index,
                node_label.as_deref(),
                true,
            )?));
            if index < edges.len() {
                items.push(Some(read(
                    edges.as_ref(),
                    index,
                    edge_label.as_deref(),
                    false,
                )?));
            }
        }
    } else {
        let path = list(array, row)?;
        let record = path.as_any().downcast_ref::<StructArray>()?;
        let kinds = record.column_by_name("kind")?;
        for index in 0..path.len() {
            let kind = text_key(kinds.as_ref(), index)?;
            if matches!(kind.as_str(), "vertex" | "edge") {
                items.push(Some(read(path.as_ref(), index, None, kind == "vertex")?));
            } else {
                items.push(None);
            }
        }
    }
    Some(items)
}

pub(super) fn preview(
    array: &dyn Array,
    index: usize,
    row: usize,
    column: &str,
    labels: Option<&(Option<String>, Option<String>)>,
) -> Option<PathPreview> {
    if array.is_null(index) {
        return None;
    }
    let mut items = Vec::new();
    if let Some((node_label, edge_label)) = labels {
        let record = array.as_any().downcast_ref::<StructArray>()?;
        let nodes = list(record.column_by_name("nodes")?.as_ref(), index)?;
        let edges = list(record.column_by_name("relationships")?.as_ref(), index)?;
        if nodes.len() != edges.len() + 1 {
            return None;
        }
        for i in 0..nodes.len() {
            items.push(item(nodes.as_ref(), i, node_label.as_deref(), "vertex")?);
            if i < edges.len() {
                items.push(item(edges.as_ref(), i, edge_label.as_deref(), "edge")?);
            }
        }
    } else {
        let path = list(array, index)?;
        let record = path.as_any().downcast_ref::<StructArray>()?;
        let kinds = record.column_by_name("kind")?;
        for i in 0..path.len() {
            let kind = match text(kinds.as_ref(), i)?.as_str() {
                "vertex" => "vertex",
                "edge" => "edge",
                _ => return None,
            };
            items.push(item(path.as_ref(), i, None, kind)?);
        }
    }
    (!items.is_empty()).then(|| PathPreview {
        row,
        column: column.into(),
        items,
    })
}
