use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result, ensure};

use super::{Entity, PATH, Step, Traversers, ident, qualified, string, text};
use crate::traversal::text_key;

pub(super) const KIND: &str = "__graphlake_gremlin_kind";

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum Kind {
    Element(Entity),
    Property(Entity, String),
    Label,
    Map(Entity, Vec<String>),
    Path,
    Count,
    Double,
    Extremum(Vec<Kind>),
    Sum(Vec<Kind>),
    List(Box<Kind>),
    Record(Vec<(String, Kind)>),
    Group(Box<Kind>),
    Variant(Vec<Kind>),
    Item(Box<Kind>),
    Null,
}

pub(super) type Tags = BTreeMap<String, BTreeSet<Entity>>;

pub(super) fn tag_column(name: &str) -> String {
    format!("__graphlake_gremlin_tag_{name}")
}

pub(super) fn slot(index: usize) -> String {
    format!("__graphlake_gremlin_item_{index}")
}

fn empty_tag() -> &'static str {
    "CASE WHEN FALSE THEN named_struct('label', '', 'kind', '', 'key', make_array('')) END"
}

pub(super) fn value_map(entity: &Entity, fields: &[String], alias: &str) -> String {
    let items = fields
        .iter()
        .flat_map(|field| {
            let value = qualified(alias, field);
            let value = if !entity.fields.contains(field) {
                "NULL".into()
            } else if entity.edge {
                value
            } else {
                format!("CASE WHEN {value} IS NOT NULL THEN make_array({value}) END")
            };
            [string(field), value]
        })
        .collect::<Vec<_>>();
    format!("named_struct({})", items.join(", "))
}

impl Kind {
    pub(super) fn depth(&self) -> usize {
        match self {
            Self::Element(_)
            | Self::Property(_, _)
            | Self::Label
            | Self::Map(_, _)
            | Self::Path
            | Self::Count
            | Self::Double
            | Self::Null => 1,
            Self::List(inner) | Self::Group(inner) | Self::Item(inner) => 1 + inner.depth(),
            Self::Extremum(items) | Self::Sum(items) | Self::Variant(items) => {
                1 + items.iter().map(Self::depth).max().unwrap_or(0)
            }
            Self::Record(fields) => 1 + fields.iter().map(|(_, k)| k.depth()).max().unwrap_or(0),
        }
    }

    pub(super) fn node_count(&self) -> usize {
        match self {
            Self::Element(_)
            | Self::Property(_, _)
            | Self::Label
            | Self::Map(_, _)
            | Self::Path
            | Self::Count
            | Self::Double
            | Self::Null => 1,
            Self::List(inner) | Self::Group(inner) | Self::Item(inner) => {
                1usize.saturating_add(inner.node_count())
            }
            Self::Extremum(items) | Self::Sum(items) | Self::Variant(items) => items
                .iter()
                .fold(1usize, |acc, k| acc.saturating_add(k.node_count())),
            Self::Record(fields) => fields
                .iter()
                .fold(1usize, |acc, (_, k)| acc.saturating_add(k.node_count())),
        }
    }

    pub(super) fn null_slot(&self, alias: &str) -> String {
        match self {
            Self::Element(_) => "CASE WHEN FALSE THEN make_array('') END".into(),
            Self::Label => "CAST(NULL AS VARCHAR)".into(),
            Self::Count => "CAST(NULL AS BIGINT)".into(),
            Self::Double => "CAST(NULL AS DOUBLE)".into(),
            Self::Sum(sources) | Self::Extremum(sources) => {
                let inputs = sources
                    .iter()
                    .map(|kind| kind.null_slot(alias))
                    .collect::<Vec<_>>();
                let function = if matches!(self, Self::Sum(_)) {
                    "__graphlake_numeric_input"
                } else {
                    "__graphlake_cypher_property"
                };
                format!("{function}({})", inputs.join(", "))
            }
            Self::Path => format!("CASE WHEN FALSE THEN make_array({}) END", empty_tag()),
            Self::Property(entity, field) => format!(
                "(SELECT {} FROM {} LIMIT 0)",
                ident(field),
                ident(&entity.label)
            ),
            Self::Map(entity, fields) => format!(
                "(SELECT {} FROM {} AS src LIMIT 0)",
                value_map(entity, fields, "src"),
                ident(&entity.label)
            ),
            Self::List(kind) => format!(
                "CASE WHEN FALSE THEN make_array({}) END",
                kind.payload_null(alias)
            ),
            Self::Record(fields) => {
                let fields = fields
                    .iter()
                    .flat_map(|(name, kind)| [string(name), kind.payload_null(alias)])
                    .collect::<Vec<_>>();
                format!(
                    "CASE WHEN FALSE THEN named_struct({}) END",
                    fields.join(", ")
                )
            }
            Self::Group(kind) => format!(
                "CASE WHEN FALSE THEN {} END",
                super::values::empty_map(kind, alias)
            ),
            Self::Variant(kinds) => {
                let mut fields = vec!["'kind'".into(), "CAST(NULL AS BIGINT)".into()];
                fields.extend(kinds.iter().enumerate().flat_map(|(index, kind)| {
                    [string(&format!("value_{index}")), kind.null_slot(alias)]
                }));
                format!(
                    "CASE WHEN FALSE THEN named_struct({}) END",
                    fields.join(", ")
                )
            }
            Self::Item(kind) => format!(
                "array_element(__graphlake_gremlin_items({}), 1)",
                kind.payload_null(alias)
            ),
            Self::Null => "NULL".into(),
        }
    }

    pub(super) fn payload_null(&self, alias: &str) -> String {
        if matches!(self, Self::Element(_)) {
            empty_tag().into()
        } else {
            self.null_slot(alias)
        }
    }
}

impl<'a> Traversers<'a> {
    fn kind(&self) -> Kind {
        self.entity.as_ref().map_or_else(
            || self.scalar_kind.clone(),
            |entity| Kind::Element(entity.clone()),
        )
    }

    pub(super) fn kinds(&self) -> Vec<Kind> {
        if self.variants.is_empty() {
            vec![self.kind()]
        } else {
            self.variants.clone()
        }
    }

    pub(super) fn visible(&self, alias: &str) -> Vec<String> {
        if !self.variants.is_empty() {
            return std::iter::once(qualified(alias, KIND))
                .chain((0..self.variants.len()).map(|index| qualified(alias, &slot(index))))
                .collect();
        }
        self.entity.as_ref().map_or_else(
            || vec![qualified(alias, "value")],
            |entity| {
                entity
                    .fields
                    .iter()
                    .map(|name| qualified(alias, name))
                    .collect()
            },
        )
    }

    pub(super) fn packed(&self, kinds: &[Kind], tags: &Tags, typed_nulls: bool) -> String {
        let mut values = Vec::new();
        if self.variants.is_empty() {
            let current = self.kind();
            let index = kinds
                .iter()
                .position(|kind| *kind == current)
                .expect("known Gremlin kind");
            values.push(format!("CAST({index} AS BIGINT) AS {KIND}"));
        } else {
            let input = qualified("v", KIND);
            let arms = self
                .variants
                .iter()
                .enumerate()
                .map(|(from, kind)| {
                    let to = kinds
                        .iter()
                        .position(|target| target == kind)
                        .expect("known Gremlin kind");
                    format!("WHEN {from} THEN {to}")
                })
                .collect::<Vec<_>>()
                .join(" ");
            values.push(format!("CAST(CASE {input} {arms} END AS BIGINT) AS {KIND}"));
        }
        for (index, kind) in kinds.iter().enumerate() {
            let value = if self.variants.is_empty() && *kind == self.kind() {
                match kind {
                    Kind::Element(entity) => text_key("v", &entity.key),
                    _ => qualified("v", "value"),
                }
            } else if let Some(previous) = self.variants.iter().position(|current| current == kind)
            {
                let slot = qualified("v", &slot(previous));
                if typed_nulls {
                    format!("coalesce({slot}, {})", kind.null_slot("v"))
                } else {
                    slot
                }
            } else if typed_nulls {
                kind.null_slot("v")
            } else {
                "NULL".into()
            };
            values.push(format!("{value} AS {}", ident(&slot(index))));
        }
        self.project(&values.join(", "), tags)
    }

    fn project(&self, values: &str, tags: &Tags) -> String {
        let mut columns = self.retained_columns("v");
        if self.path {
            columns.push_str(&format!(", {}", qualified("v", PATH)));
        }
        for name in tags.keys() {
            let column = tag_column(name);
            let value = if self.tags.contains_key(name) {
                qualified("v", &column)
            } else {
                empty_tag().into()
            };
            columns.push_str(&format!(", {value} AS {}", ident(&column)));
        }
        format!("SELECT {values}{columns} FROM ({}) AS v", self.sql)
    }

    pub(super) fn adopt(&mut self, kinds: Vec<Kind>, tags: Tags) {
        self.entity = None;
        self.variants = kinds;
        self.tags = tags;
    }

    pub(super) fn combine(&mut self, states: &[Self]) -> String {
        let kinds = states
            .iter()
            .flat_map(Self::kinds)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let mut tags = Tags::new();
        for state in states {
            self.sliced |= state.sliced;
            self.labels.extend(state.labels.iter().cloned());
            self.next_column = self.next_column.max(state.next_column);
            extend_tags(&mut tags, &state.tags);
        }
        let single = kinds.len() == 1 && states.iter().all(|state| state.variants.is_empty());
        let sql = states
            .iter()
            .map(|state| {
                if single {
                    state.project(&state.visible("v").join(", "), &tags)
                } else {
                    state.packed(&kinds, &tags, false)
                }
            })
            .collect::<Vec<_>>()
            .join(" UNION ALL ");
        if single {
            self.variants.clear();
            match &kinds[0] {
                Kind::Element(entity) => self.entity = Some(entity.clone()),
                scalar => {
                    self.entity = None;
                    self.scalar_kind = scalar.clone();
                }
            }
            self.tags = tags;
        } else {
            self.adopt(kinds, tags);
        }
        sql
    }

    pub(super) fn lanes(&self, table: &str) -> Vec<Self> {
        if self.variants.is_empty() {
            let mut state = self.clone();
            state.sql = format!("SELECT * FROM {}", ident(table));
            return vec![state];
        }
        self.variants
            .iter()
            .enumerate()
            .map(|(index, kind)| {
                let mut state = self.clone();
                state.variants.clear();
                let seed = format!("SELECT * FROM {} WHERE {KIND} = {index}", ident(table));
                let value = qualified("v", &slot(index));
                match kind {
                    Kind::Element(entity) => {
                        state.entity = Some(entity.clone());
                        state.sql = state.hydrate(entity, &value, &seed, false);
                    }
                    kind => {
                        state.entity = None;
                        state.scalar_kind = kind.clone();
                        state.sql = format!(
                            "SELECT {value} AS value{} FROM ({seed}) AS v",
                            state.carry("v", None)
                        );
                    }
                }
                state
            })
            .collect()
    }

    pub(super) fn hydrate(&self, entity: &Entity, key: &str, seed: &str, record: bool) -> String {
        let (key, test, append) = if record {
            (
                format!("get_field({key}, 'key')"),
                format!(" AND get_field({key}, 'label') = {}", string(&entity.label)),
                Some(entity.identity("r")),
            )
        } else {
            (key.to_owned(), String::new(), None)
        };
        format!(
            "SELECT {}{} FROM ({seed}) AS v JOIN {} AS r ON {} = {key}{test}",
            entity
                .fields
                .iter()
                .map(|field| qualified("r", field))
                .collect::<Vec<_>>()
                .join(", "),
            self.carry("v", append.as_deref()),
            ident(&entity.label),
            text_key("r", &entity.key)
        )
    }

    pub(super) fn shared_input(&self, table: &str, share: bool) -> Option<String> {
        // A lexical CTE or a recursive SQL frontier is only visible inside its
        // enclosing statement. Ordinary repeat frontiers are registered tables.
        if share && self.ctes.is_empty() && !self.recursive {
            self.stages.borrow_mut().push(super::repeat::Stage::Shared {
                table: table.into(),
                sql: self.sql.clone(),
            });
            None
        } else {
            Some(format!("{} AS ({})", ident(table), self.sql))
        }
    }

    pub(super) fn select_tag(&mut self, name: &str) -> Result<()> {
        let entities = self
            .tags
            .get(name)
            .with_context(|| format!("unknown step label {name}"))?
            .clone();
        let table = self.fresh("select");
        let cte = self.shared_input(&table, entities.len() > 1);
        let states = entities
            .into_iter()
            .map(|entity| {
                let mut state = self.clone();
                state.variants.clear();
                state.sql = state.hydrate(
                    &entity,
                    &qualified("v", &tag_column(name)),
                    &format!("SELECT * FROM {}", ident(&table)),
                    true,
                );
                state.entity = Some(entity);
                state
            })
            .collect::<Vec<_>>();
        let combined = self.combine(&states);
        self.sql = with_input(cte, combined);
        Ok(())
    }

    pub(super) fn dispatch(
        &mut self,
        step: &Step<'_>,
        depth: usize,
        early_limit: bool,
    ) -> Result<()> {
        let table = self.fresh("variant");
        let mut states = Vec::new();
        for state in self.lanes(&table) {
            if state.applicable(step)? {
                states.push(state);
            }
        }
        if states.is_empty() {
            self.restrict("FALSE");
            return Ok(());
        }
        let cte = self.shared_input(&table, !early_limit && states.len() > 1);
        let mut lane_unfold = Vec::with_capacity(states.len());
        for state in &mut states {
            if let Some(cte) = &cte {
                state.ctes.push(cte.clone());
            }
            state.next_column = self.next_column;
            state.apply(std::slice::from_ref(step), depth)?;
            let added_unfold = (step.name == "unfold" && state.order.len() == self.order.len() + 1)
                .then(|| state.order.remove(0).0);
            lane_unfold.push(added_unfold);
            // An implicit multi-edge hop may build an internal branch. All lanes
            // still carry the ordering of their common input.
            state.order.clone_from(&self.order);
            self.next_column = self.next_column.max(state.next_column);
        }
        if lane_unfold.iter().any(Option::is_some) {
            let shared = self.fresh("unfold_index");
            for (state, added) in states.iter_mut().zip(lane_unfold) {
                match added {
                    Some(column) if column != shared => {
                        state.sql = format!(
                            "SELECT v.*, {} AS {} FROM ({}) AS v",
                            qualified("v", &column),
                            ident(&shared),
                            state.sql
                        );
                    }
                    Some(_) => {}
                    None => {
                        state.sql = format!(
                            "SELECT v.*, CAST(0 AS BIGINT) AS {} FROM ({}) AS v",
                            ident(&shared),
                            state.sql
                        );
                    }
                }
                state.order.insert(0, (shared.clone(), false));
            }
            self.order.insert(0, (shared, false));
        }
        let combined = self.combine(&states);
        self.sql = with_input(cte, combined);
        Ok(())
    }

    fn applicable(&self, step: &Step<'_>) -> Result<bool> {
        let Some(entity) = &self.entity else {
            return Ok(matches!(step.name, "is" | "unfold" | "hasKey"));
        };
        let args = step.args.as_slice();
        Ok(match (step.name, args) {
            ("is", _) => false,
            ("hasLabel", labels) if !labels.is_empty() => {
                super::labels_match(&entity.label, labels)?
            }
            ("has", [field] | [field, _]) | ("values", [field]) => {
                entity.fields.contains(&text(field)?)
            }
            ("has", [label, field, _]) => {
                entity.label == text(label)? && entity.fields.contains(&text(field)?)
            }
            ("outV" | "inV" | "bothV" | "otherV", _) => entity.edge,
            ("out" | "in" | "both" | "outE" | "inE" | "bothE", [edge]) => {
                !entity.edge && self.edge_names(step.name)?.contains(&text(edge)?)
            }
            ("out" | "in" | "both" | "outE" | "inE" | "bothE", []) => {
                !entity.edge && !self.edge_names(step.name)?.is_empty()
            }
            _ => true,
        })
    }

    #[cfg(feature = "admin")]
    pub(super) fn graph_output(&self) -> crate::engine::query_graph::GraphOutput {
        use crate::engine::query_graph::{GraphColumn, GraphOutput};

        fn shape(kind: &Kind) -> Option<GraphColumn> {
            Some(match kind {
                Kind::Element(entity) => GraphColumn::Entity {
                    label: None,
                    node: !entity.edge,
                },
                Kind::Path => GraphColumn::Path { labels: None },
                Kind::List(item) => GraphColumn::List(Box::new(shape(item)?)),
                Kind::Record(fields) => GraphColumn::Record(
                    fields
                        .iter()
                        .filter_map(|(name, kind)| Some((name.clone(), shape(kind)?)))
                        .collect(),
                ),
                _ => return None,
            })
        }
        let mut output = GraphOutput::default();
        if !self.variants.is_empty() {
            output.gremlin_variant = true;
            let scalars = self
                .variants
                .iter()
                .filter(|kind| !matches!(kind, Kind::Element(_)))
                .count();
            let fields = self
                .variants
                .iter()
                .enumerate()
                .filter_map(|(index, kind)| {
                    let name = match kind {
                        Kind::Element(entity) => format!(
                            "{}_{}",
                            if entity.edge { "edge" } else { "vertex" },
                            entity.label
                        ),
                        _ if scalars == 1 => "value".into(),
                        _ => format!("value_{index}"),
                    };
                    Some((name, shape(kind)?))
                })
                .collect();
            output
                .columns
                .insert("value".into(), GraphColumn::Record(fields));
        } else if let Some(entity) = &self.entity {
            output.row_entity = Some((entity.label.clone(), !entity.edge));
        } else if let Some(column) = shape(&self.scalar_kind) {
            output.columns.insert("value".into(), column);
        }
        output
    }

    pub(super) fn variant_result(&self) -> String {
        let mut fields = Vec::new();
        let scalars = self
            .variants
            .iter()
            .filter(|kind| !matches!(kind, Kind::Element(_)))
            .count();
        for (index, kind) in self.variants.iter().enumerate() {
            let item = qualified("v", &slot(index));
            let (name, value) = match kind {
                Kind::Element(entity) => {
                    let name = if entity.edge { "edge" } else { "vertex" };
                    (
                        format!("{name}_{}", entity.label),
                        format!(
                            "CASE WHEN {KIND} = {index} THEN named_struct('label', {}, 'key', {item}) END",
                            string(&entity.label)
                        ),
                    )
                }
                _ => (
                    if scalars == 1 {
                        "value".into()
                    } else {
                        format!("value_{index}")
                    },
                    item,
                ),
            };
            fields.extend([string(&name), value]);
        }
        format!("named_struct({}) AS value", fields.join(", "))
    }

    pub(super) fn variant_value(&self, alias: &str) -> String {
        if self.variants.is_empty() {
            return qualified(alias, "value");
        }
        let slots = (0..self.variants.len())
            .map(|index| qualified(alias, &slot(index)))
            .collect::<Vec<_>>();
        format!("__graphlake_cypher_property({})", slots.join(", "))
    }

    pub(super) fn variant_order(&self, alias: &str) -> String {
        if self
            .variants
            .iter()
            .any(|kind| matches!(kind, Kind::Element(_)))
        {
            return format!(
                "__graphlake_gremlin_order({})",
                self.variant_identity(alias)
            );
        }
        format!("__graphlake_gremlin_order({})", self.variant_value(alias))
    }

    pub(super) fn variant_identity(&self, alias: &str) -> String {
        let kind = qualified(alias, KIND);
        let variants = self.variants.iter().enumerate().map(|(index, variant)| {
            let value = qualified(alias, &slot(index));
            let (category, label, key) = match variant {
                Kind::Element(entity) => (
                    if entity.edge { "edge" } else { "vertex" },
                    string(&entity.label),
                    value,
                ),
                _ => (
                    "scalar",
                    format!("CASE WHEN arrow_typeof({value}) IN ('Utf8','Utf8View','LargeUtf8') THEN 'Utf8' ELSE arrow_typeof({value}) END"),
                    format!("make_array(__graphlake_gremlin_text({value}))"),
                ),
            };
            format!("WHEN {index} THEN named_struct('kind','{category}','label',{label},'key',{key})")
        }).collect::<Vec<_>>().join(" ");
        format!("CASE {kind} {variants} END")
    }

    pub(super) fn order_property(&mut self, property: &str, column: &str) -> Result<()> {
        let seed = self.fresh("order_input");
        let mut reads = Vec::new();
        let projection = self.select_columns("v");
        for (index, kind) in self.variants.iter().enumerate() {
            let Kind::Element(entity) = kind else {
                continue;
            };
            if !entity.fields.iter().any(|field| field == property) {
                continue;
            }
            reads.push(format!(
                "SELECT {projection}, __graphlake_gremlin_order({}) AS {} FROM {} AS v JOIN {} AS r ON {} = {}
                 WHERE v.{KIND} = {index}",
                qualified("r", property),
                ident(column),
                ident(&seed),
                ident(&entity.label),
                qualified("v", &slot(index)),
                text_key("r", &entity.key)
            ));
        }
        ensure!(!reads.is_empty(), "unknown property {property} for order()");
        let cte = self.shared_input(&seed, reads.len() > 1);
        self.sql = with_input(cte, crate::catalog::union_all(&reads));
        Ok(())
    }
}

pub(super) fn with_input(cte: Option<String>, sql: String) -> String {
    match cte {
        Some(cte) => format!("WITH {cte} {sql}"),
        None => sql,
    }
}

pub(super) fn extend_tags(target: &mut Tags, source: &Tags) {
    for (name, entities) in source {
        target
            .entry(name.clone())
            .or_default()
            .extend(entities.iter().cloned());
    }
}
