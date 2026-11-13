use std::collections::BTreeMap;

use crate::{
    Catalog,
    arrow::{
        array::{Array, ArrayRef, RecordBatch, StructArray},
        compute::cast,
        datatypes::DataType,
    },
    catalog::Endpoint,
    engine::query_graph::{GraphColumn, GraphOutput},
};

use super::*;

pub(super) const MAX_ELEMENTS: usize = 240;
pub(super) const GRAPH_BYTES: usize = 120_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(in crate::admin) enum Kind {
    Node,
    Edge,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::admin) struct Identity {
    pub kind: Kind,
    pub label: String,
    pub key: Vec<String>,
}

impl Identity {
    pub fn id(&self) -> String {
        // An array of components is unambiguous for composite or delimiter-bearing keys.
        serde_json::to_string(self).expect("serializing graph identity")
    }
}

pub(super) type Properties = BTreeMap<String, Option<String>>;

#[derive(Clone, Serialize, Deserialize)]
pub(super) struct Element {
    pub id: String,
    #[serde(flatten)]
    pub identity: Identity,
    pub properties: Option<Properties>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub to: Option<String>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub transient: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub directed: Option<bool>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub property_preview: bool,
}

impl Element {
    pub fn new(identity: Identity) -> Self {
        Self {
            id: identity.id(),
            identity,
            properties: None,
            from: None,
            to: None,
            directed: None,
            transient: false,
            property_preview: false,
        }
    }

    pub fn endpoints(&mut self, from: &Identity, to: &Identity, directed: bool) {
        self.from = Some(from.id());
        self.to = Some(to.id());
        self.directed = Some(directed);
    }
}

#[derive(Clone, Default, Serialize, Deserialize)]
pub(super) struct Fragment {
    pub elements: Vec<Element>,
    pub truncated: bool,
    pub steps: Vec<[String; 2]>,
}

impl Fragment {
    pub fn prune_missing_endpoints(&mut self) {
        if !self.truncated {
            return;
        }
        let nodes = self
            .elements
            .iter()
            .filter(|item| item.identity.kind == Kind::Node)
            .map(|item| item.id.clone())
            .collect::<std::collections::BTreeSet<_>>();
        self.elements.retain(|item| {
            item.from.as_ref().is_none_or(|id| nodes.contains(id))
                && item.to.as_ref().is_none_or(|id| nodes.contains(id))
        });
        self.steps
            .retain(|[from, to]| nodes.contains(from) && nodes.contains(to));
    }
}

pub(super) struct Collector {
    items: BTreeMap<String, (Element, usize)>,
    bytes: usize,
    steps: std::collections::BTreeSet<[String; 2]>,
    truncated: bool,
    max_elements: usize,
    max_bytes: usize,
    saturated: bool,
    trim_properties: bool,
}

impl Default for Collector {
    fn default() -> Self {
        Self::bounded(MAX_ELEMENTS, GRAPH_BYTES)
    }
}

impl Collector {
    pub fn bounded(max_elements: usize, max_bytes: usize) -> Self {
        Self {
            items: BTreeMap::new(),
            bytes: 0,
            steps: Default::default(),
            truncated: false,
            max_elements,
            max_bytes,
            saturated: false,
            trim_properties: true,
        }
    }

    pub fn saturated(&self) -> bool {
        self.saturated
    }

    pub fn insert_group(&mut self, group: impl IntoIterator<Item = Element>) -> bool {
        let mut before = BTreeMap::new();
        let bytes = self.bytes;
        for item in group {
            let id = item.id.clone();
            before
                .entry(id.clone())
                .or_insert_with(|| self.items.get(&id).cloned());
            if !self.insert(item) {
                for (id, previous) in before {
                    if let Some(previous) = previous {
                        self.items.insert(id, previous);
                    } else {
                        self.items.remove(&id);
                    }
                }
                self.bytes = bytes;
                return false;
            }
        }
        true
    }

    pub fn insert(&mut self, mut next: Element) -> bool {
        if let Some((old, _)) = self.items.get(&next.id)
            && (old.properties.is_some() || next.properties.is_none())
            && (old.from.is_some() || next.from.is_none())
            && (old.directed == Some(true) || next.directed != Some(true))
        {
            return true;
        }
        let previous = self.items.get(&next.id);
        if let Some((old, _)) = previous {
            if next.properties.is_none() {
                next.properties = old.properties.clone();
                next.property_preview = old.property_preview;
            }
            if next.from.is_none() || old.directed == Some(true) && next.directed != Some(true) {
                next.from = old.from.clone();
                next.to = old.to.clone();
                next.directed = old.directed;
            }
        } else if self.items.len() >= self.max_elements {
            self.truncated = true;
            self.saturated = true;
            return false;
        }
        let cost = |item: &Element| {
            serde_json::to_vec(item)
                .expect("serializing graph element")
                .len()
                + 1
        };
        let previous_cost = previous.map_or(0, |(_, cost)| *cost);
        let mut bytes = cost(&next);
        if self.trim_properties && self.bytes - previous_cost + bytes > self.max_bytes {
            if let Some((old, _)) = previous {
                next.properties = old.properties.clone();
                next.property_preview = true;
            } else {
                next.properties = None;
                next.property_preview = true;
            }
            bytes = cost(&next);
        }
        if self.bytes - previous_cost + bytes > self.max_bytes {
            self.truncated = true;
            self.saturated = true;
            return false;
        }
        self.bytes = self.bytes - previous_cost + bytes;
        self.items.insert(next.id.clone(), (next, bytes));
        true
    }

    pub fn unresolved_edges(&self) -> BTreeMap<String, Vec<Vec<String>>> {
        let mut edges = BTreeMap::<String, Vec<Vec<String>>>::new();
        for (item, _) in self.items.values() {
            if item.identity.kind == Kind::Edge && item.directed != Some(true) {
                edges
                    .entry(item.identity.label.clone())
                    .or_default()
                    .push(item.identity.key.clone());
            }
        }
        edges
    }

    pub fn add_endpoints(
        &mut self,
        item: &Element,
        catalog: &Catalog,
        batch: &RecordBatch,
        row: usize,
    ) {
        if item.directed != Some(true) {
            return;
        }
        let edge = &catalog.edges[&item.identity.label];
        for end in [&edge.from, &edge.to] {
            if let Some(identity) =
                endpoint_identity(catalog, end, row, |name| batch.column_by_name(name))
            {
                self.insert(Element::new(identity));
            }
        }
    }

    pub fn mark_truncated(&mut self) {
        self.truncated = true;
    }

    fn path_step(&mut self, from: &Identity, to: &Identity) {
        self.insert_step([from.id(), to.id()]);
    }

    fn insert_step(&mut self, pair: [String; 2]) -> bool {
        if self.steps.contains(&pair) {
            return true;
        }
        let bytes = serde_json::to_vec(&pair)
            .expect("serializing path step")
            .len()
            + 1;
        if self.steps.len() == self.max_elements || self.bytes + bytes > self.max_bytes {
            self.truncated = true;
            self.saturated = true;
            return false;
        }
        self.bytes += bytes;
        self.steps.insert(pair);
        true
    }

    pub fn finish(self) -> Fragment {
        Fragment {
            elements: self
                .items
                .into_values()
                .map(|(element, _)| element)
                .collect(),
            truncated: self.truncated,
            steps: self.steps.into_iter().collect(),
        }
    }

    pub fn row(
        &mut self,
        catalog: &Catalog,
        output: &GraphOutput,
        batch: &RecordBatch,
        row: usize,
    ) {
        if let Some((label, node)) = &output.row_entity {
            if let Some(item) = from_fields(catalog, label, *node, row, |name| {
                batch.column_by_name(name)
            }) {
                self.add_endpoints(&item, catalog, batch, row);
                self.insert(item);
            } else if batch.num_columns() > 0 {
                self.truncated = true;
            }
        }
        for (name, shape) in &output.columns {
            if let Some(array) = batch.column_by_name(name) {
                self.value(catalog, shape, array.as_ref(), row);
            }
        }
    }

    fn value(&mut self, catalog: &Catalog, shape: &GraphColumn, array: &dyn Array, row: usize) {
        if array.is_null(row) {
            return;
        }
        match shape {
            GraphColumn::Path { labels } => {
                if let Some(items) = super::paths::identities(catalog, array, row, labels.as_ref())
                {
                    for (index, identity) in items.iter().enumerate() {
                        let Some(identity) = identity else {
                            continue;
                        };
                        let before = index.checked_sub(1).and_then(|i| items[i].as_ref());
                        let after = items.get(index + 1).and_then(Option::as_ref);
                        if identity.kind == Kind::Node
                            && let Some(next) = after
                            && next.kind == Kind::Node
                        {
                            self.path_step(identity, next);
                        }
                        let mut item = Element::new(identity.clone());
                        item.transient = identity.kind == Kind::Edge
                            && catalog.edges[&identity.label].key.is_empty();
                        if item.identity.kind == Kind::Edge
                            && let (Some(left), Some(right)) = (before, after)
                            && left.kind == Kind::Node
                            && right.kind == Kind::Node
                        {
                            let edge = &catalog.edges[&identity.label];
                            if edge.from.node != edge.to.node
                                && edge.from.node == right.label
                                && edge.to.node == left.label
                            {
                                item.endpoints(right, left, true);
                            } else {
                                item.endpoints(
                                    left,
                                    right,
                                    edge.from.node != edge.to.node
                                        && edge.from.node == left.label
                                        && edge.to.node == right.label,
                                );
                            }
                        }
                        self.insert(item);
                    }
                } else {
                    self.truncated = true;
                }
            }
            GraphColumn::Entity { label, node } => {
                let Some(record) = array.as_any().downcast_ref::<StructArray>() else {
                    return;
                };
                let item = if let Some(label) = label {
                    from_fields(catalog, label, *node, row, |name| {
                        record.column_by_name(name)
                    })
                } else {
                    tagged_entity(catalog, *node, record, row)
                };
                if let Some(item) = item {
                    self.insert(item);
                } else {
                    self.truncated = true;
                }
            }
            GraphColumn::List(item) => {
                if let Some(values) = super::paths::list(array, row) {
                    for index in 0..values.len() {
                        self.value(catalog, item, values.as_ref(), index);
                    }
                } else {
                    self.truncated = true;
                }
            }
            GraphColumn::Record(fields) => {
                if let Some(record) = array.as_any().downcast_ref::<StructArray>() {
                    for (name, shape) in fields {
                        if let Some(value) = record.column_by_name(name) {
                            self.value(catalog, shape, value.as_ref(), row);
                        }
                    }
                }
            }
        }
    }
}

pub(super) fn result_pages(graph: Fragment) -> (Vec<Fragment>, bool) {
    let mut incomplete = graph.truncated;
    let mut pages = Vec::new();
    let new_page = || Collector {
        trim_properties: false,
        ..Collector::default()
    };
    let mut page = new_page();
    let nodes: BTreeMap<_, _> = graph
        .elements
        .iter()
        .filter(|item| item.identity.kind == Kind::Node)
        .map(|item| (item.id.as_str(), item))
        .collect();
    let mut used = std::collections::BTreeSet::new();
    let mut flush = |page: &mut Collector| {
        let fragment = std::mem::replace(page, new_page()).finish();
        if !fragment.elements.is_empty() {
            pages.push(Fragment {
                truncated: false,
                ..fragment
            });
        }
    };
    for edge in graph
        .elements
        .iter()
        .filter(|item| item.identity.kind == Kind::Edge)
    {
        let endpoint = |id: &Option<String>| nodes.get(id.as_deref()?).copied();
        let (Some(from), Some(to)) = (endpoint(&edge.from), endpoint(&edge.to)) else {
            incomplete = true;
            continue;
        };
        let group = [from.clone(), to.clone(), edge.clone()];
        if !page.insert_group(group.iter().cloned()) {
            flush(&mut page);
            if !page.insert_group(group.iter().cloned()) {
                incomplete = true;
                continue;
            }
        }
        used.extend(group.iter().map(|item| item.id.clone()));
    }
    for pair in graph.steps {
        let Some(&from) = nodes.get(pair[0].as_str()) else {
            continue;
        };
        let Some(&to) = nodes.get(pair[1].as_str()) else {
            continue;
        };
        if !page.insert_group([from.clone(), to.clone()]) || !page.insert_step(pair.clone()) {
            flush(&mut page);
            if !page.insert_group([from.clone(), to.clone()]) || !page.insert_step(pair.clone()) {
                incomplete = true;
                continue;
            }
        }
        used.extend(pair);
    }
    for node in nodes.values().filter(|node| !used.contains(&node.id)) {
        if !page.insert_group([(*node).clone()]) {
            flush(&mut page);
            if !page.insert_group([(*node).clone()]) {
                incomplete = true;
            }
        }
    }
    flush(&mut page);
    (pages, incomplete)
}

pub(in crate::admin) fn text_key(array: &dyn Array, row: usize) -> Option<String> {
    let value = cast(array.slice(row, 1).as_ref(), &DataType::Utf8).ok()?;
    let value = value
        .as_any()
        .downcast_ref::<crate::arrow::array::StringArray>()?;
    if value.is_null(0) {
        return None;
    }
    let key = value.value(0);
    (key.len() <= 1024).then(|| key.to_owned())
}

pub(in crate::admin) fn identity(
    catalog: &Catalog,
    node: bool,
    label: String,
    key: Vec<String>,
) -> Option<Identity> {
    let expected = if node {
        catalog.nodes.get(&label)?.key.len()
    } else {
        catalog.edges.get(&label)?.identity().len()
    };
    (expected == key.len() && label.len() <= 512).then_some(Identity {
        label,
        key,
        kind: if node { Kind::Node } else { Kind::Edge },
    })
}

fn key_names(catalog: &Catalog, node: bool, label: &str) -> Option<Vec<String>> {
    if node {
        Some(catalog.nodes.get(label)?.key.clone())
    } else {
        Some(catalog.edges.get(label)?.identity())
    }
}

pub(super) fn from_fields<'a>(
    catalog: &Catalog,
    label: &str,
    node: bool,
    row: usize,
    get: impl Fn(&str) -> Option<&'a ArrayRef>,
) -> Option<Element> {
    let keys = key_names(catalog, node, label)?;
    let key = keys
        .iter()
        .map(|name| text_key(get(name)?.as_ref(), row))
        .collect::<Option<_>>()?;
    let identity = identity(catalog, node, label.to_owned(), key)?;
    Some(from_identity(catalog, identity, row, get))
}

pub(super) fn from_identity<'a>(
    catalog: &Catalog,
    identity: Identity,
    row: usize,
    get: impl Fn(&str) -> Option<&'a ArrayRef>,
) -> Element {
    let mut item = Element::new(identity);
    let label = &item.identity.label;
    let node = item.identity.kind == Kind::Node;
    let names = if node {
        &catalog.nodes[label].properties
    } else {
        &catalog.edges[label].properties
    };
    let (properties, preview) = property_values(
        names
            .iter()
            .filter_map(|name| Some((name.as_str(), get(name)?))),
        row,
    );
    item.properties = Some(properties);
    item.property_preview = preview;
    if !node {
        let edge = &catalog.edges[label];
        item.transient = edge.key.is_empty();
        let end = |endpoint: &Endpoint| endpoint_identity(catalog, endpoint, row, &get);
        if let (Some(from), Some(to)) = (end(&edge.from), end(&edge.to)) {
            item.endpoints(&from, &to, true);
        }
    }
    item
}

fn endpoint_identity<'a>(
    catalog: &Catalog,
    endpoint: &Endpoint,
    row: usize,
    get: impl Fn(&str) -> Option<&'a ArrayRef>,
) -> Option<Identity> {
    let keys = endpoint
        .columns
        .iter()
        .map(|name| text_key(get(name)?.as_ref(), row))
        .collect::<Option<_>>()?;
    identity(catalog, true, endpoint.node.clone(), keys)
}

fn tagged_entity(
    catalog: &Catalog,
    node: bool,
    record: &StructArray,
    row: usize,
) -> Option<Element> {
    let label = text_key(record.column_by_name("label")?.as_ref(), row)?;
    let key = super::paths::list(record.column_by_name("key")?.as_ref(), row)?;
    let values = (0..key.len())
        .map(|index| text_key(key.as_ref(), index))
        .collect::<Option<_>>()?;
    let mut item = Element::new(identity(catalog, node, label.clone(), values)?);
    item.transient = !node && catalog.edges[&label].key.is_empty();
    if let Some(properties) = record
        .column_by_name("properties")
        .and_then(|value| value.as_any().downcast_ref::<StructArray>())
    {
        let names = if node {
            &catalog.nodes[&label].properties
        } else {
            &catalog.edges[&label].properties
        };
        let (values, preview) = property_values(
            names
                .iter()
                .filter_map(|name| Some((name.as_str(), properties.column_by_name(name)?))),
            row,
        );
        item.properties = Some(values);
        item.property_preview = preview;
    }
    Some(item)
}

fn property_values<'a>(
    values: impl Iterator<Item = (&'a str, &'a ArrayRef)>,
    row: usize,
) -> (Properties, bool) {
    let mut output = Properties::new();
    let mut clipped = false;
    let mut remaining = 8192_usize;
    let format = FormatOptions::default();
    for (name, array) in values {
        if output.len() == 40 || name.len() > 1024 || name.len() + 12 > remaining {
            clipped = true;
            break;
        }
        remaining -= name.len() + 12;
        let value = if array.is_null(row) {
            None
        } else {
            let Ok(formatter) = ArrayFormatter::try_new(array.as_ref(), &format) else {
                clipped = true;
                continue;
            };
            let mut text = CellText {
                value: String::new(),
                limit: remaining.min(1024),
                exceeded: false,
            };
            if formatter.value(row).write(&mut text).is_err() {
                text.value.push('…');
                clipped = true;
            }
            remaining = remaining.saturating_sub(text.value.len());
            Some(text.value)
        };
        output.insert(name.to_owned(), value);
    }
    (output, clipped)
}
