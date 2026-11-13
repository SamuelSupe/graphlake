mod control;
mod math;
mod parse;
pub(crate) mod repeat;
mod values;
mod variants;

use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet},
    rc::Rc,
};

use anyhow::{Context, Result, bail, ensure};
use opencypher::{ExprKind, parse_expr};

use crate::{
    Catalog,
    catalog::{columns, equal_keys, ident, qualified},
    cypher::{Plan, literal, string},
};

use parse::{Step, integer, parse_chain, text};
use variants::{Kind, Tags};

const PATH: &str = "__graphlake_gremlin_path";

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Entity {
    label: String,
    key: Vec<String>,
    fields: Vec<String>,
    edge: bool,
}

impl Entity {
    fn node(catalog: &Catalog, name: &str) -> Result<Self> {
        let node = catalog
            .nodes
            .get(name)
            .with_context(|| format!("unknown vertex label {name}"))?;
        Ok(Self {
            label: name.into(),
            key: node.key.clone(),
            fields: columns([&node.key, &node.properties]),
            edge: false,
        })
    }

    fn edge(catalog: &Catalog, name: &str) -> Result<Self> {
        let edge = catalog
            .edges
            .get(name)
            .with_context(|| format!("unknown edge label {name}"))?;
        let key = edge.identity();
        Ok(Self {
            label: name.into(),
            fields: columns([&key, &edge.source_columns()]),
            key,
            edge: true,
        })
    }

    fn check(&self, field: &str) -> Result<()> {
        ensure!(
            self.fields.iter().any(|name| name == field),
            "unknown property {}.{field}",
            self.label
        );
        Ok(())
    }

    fn identity(&self, alias: &str) -> String {
        self.identity_from(|field| qualified(alias, field))
    }

    fn identity_from(&self, field: impl Fn(&str) -> String) -> String {
        let keys = self
            .key
            .iter()
            .map(|key| format!("CAST({} AS VARCHAR)", field(key)))
            .collect::<Vec<_>>();
        format!(
            "named_struct('label', {}, 'kind', '{}', 'key', make_array({}))",
            string(&self.label),
            if self.edge { "edge" } else { "vertex" },
            keys.join(", ")
        )
    }
}

#[derive(Clone)]
struct Traversers<'a> {
    catalog: &'a Catalog,
    sql: String,
    entity: Option<Entity>,
    scalar_kind: Kind,
    variants: Vec<Kind>,
    tags: Tags,
    labels: BTreeSet<String>,
    path: bool,
    next_column: usize,
    order: Vec<(String, bool)>,
    control_columns: Vec<String>,
    group: Option<control::Group>,
    ctes: Vec<String>,
    repeats: Rc<RefCell<Vec<repeat::Repeat>>>,
}

pub(crate) fn plan(catalog: &Catalog, query: &str) -> Result<Plan> {
    let steps = parse_chain(query, "g")?;
    let [start, rest @ ..] = steps.as_slice() else {
        bail!("Gremlin requires V() or E()");
    };
    let names = match start.name {
        "V" => catalog.nodes.keys().collect::<Vec<_>>(),
        "E" => catalog.edges.keys().collect::<Vec<_>>(),
        _ => bail!("Gremlin must start with g.V() or g.E()"),
    };
    let selected = match rest.first().map(|step| (step.name, step.args.as_slice())) {
        Some(("hasLabel", [label])) | Some(("has", [label, _, _])) => Some(text(label)?),
        _ => None,
    };
    let selected = selected.or_else(|| (names.len() == 1).then(|| names[0].clone()));
    if selected.is_none()
        && start.args.is_empty()
        && let [
            Step {
                name: "count",
                args,
            },
        ] = rest
        && args.is_empty()
    {
        let reads = names
            .iter()
            .map(|name| format!("SELECT COUNT(*) AS value FROM {}", ident(name)))
            .collect::<Vec<_>>();
        let sql = if reads.is_empty() {
            "SELECT CAST(0 AS BIGINT) AS value".into()
        } else {
            format!(
                "SELECT CAST(sum(value) AS BIGINT) AS value FROM ({}) AS counts",
                crate::catalog::union_all(&reads)
            )
        };
        return Ok(Plan {
            sql,
            labels: names.into_iter().cloned().collect(),
            ..Default::default()
        });
    }
    let path = needs_path(rest, 0)?;
    let id_labels = if start.args.is_empty() {
        BTreeMap::new()
    } else {
        id_labels(names.iter().map(|label| label.as_str()))
    };
    let selected_ids = start
        .args
        .iter()
        .map(|arg| Ok(qualified_id_label(&parse_expr(arg)?, &id_labels)))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .collect::<Option<BTreeSet<_>>>();
    let mixed = selected.is_none();
    let labels = selected.into_iter().collect::<Vec<_>>();
    let labels: Vec<_> = if mixed {
        names
            .into_iter()
            .filter(|label| {
                selected_ids
                    .as_ref()
                    .is_none_or(|ids| ids.is_empty() || ids.contains(label.as_str()))
            })
            .collect()
    } else {
        labels.iter().collect()
    };
    ensure!(!labels.is_empty(), "the graph has no mapped edge types");
    let mut starts = labels
        .into_iter()
        .map(|label| {
            let entity = if start.name == "V" {
                Entity::node(catalog, label)?
            } else {
                Entity::edge(catalog, label)?
            };
            let mut sql = if path {
                format!(
                    "SELECT v.*, make_array({}) AS {PATH} FROM {} AS v",
                    entity.identity("v"),
                    ident(label)
                )
            } else {
                format!("SELECT v.* FROM {} AS v", ident(label))
            };
            if !start.args.is_empty() {
                let keys = start
                    .args
                    .iter()
                    .map(|arg| key_condition(&entity, arg, mixed, &id_labels))
                    .collect::<Result<Vec<_>>>()?;
                sql.push_str(&format!(" WHERE ({})", keys.join(" OR ")));
            }
            Ok(Traversers {
                catalog,
                sql,
                entity: Some(entity),
                scalar_kind: Kind::Label,
                variants: Vec::new(),
                tags: BTreeMap::new(),
                labels: BTreeSet::from([label.clone()]),
                path,
                next_column: 0,
                order: Vec::new(),
                control_columns: Vec::new(),
                group: None,
                ctes: Vec::new(),
                repeats: Rc::default(),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let mut state = if starts.len() == 1 {
        starts.pop().expect("one start")
    } else {
        let mut state = starts[0].clone();
        state.sql = state.combine(&starts);
        state
    };
    state.apply(rest, 0)?;
    let output = if !state.variants.is_empty() {
        state.variant_result()
    } else {
        state.entity.as_ref().map_or_else(
            || "value".to_owned(),
            |entity| {
                entity
                    .fields
                    .iter()
                    .map(|field| qualified("v", field))
                    .collect::<Vec<_>>()
                    .join(", ")
            },
        )
    };
    #[cfg(feature = "admin")]
    let graph_output = state.graph_output();
    let repeats = state.repeats.take();
    Ok(Plan {
        sql: format!(
            "SELECT {output} FROM ({}) AS v{}",
            state.sql,
            state.order_clause()
        ),
        labels: state.labels,
        repeats,
        #[cfg(feature = "admin")]
        graph_output,
        ..Default::default()
    })
}

fn needs_path(steps: &[Step<'_>], depth: usize) -> Result<bool> {
    ensure!(depth <= 16, "Gremlin nesting exceeds 16");
    for step in steps {
        match step.name {
            "path" | "simplePath" | "otherV" => return Ok(true),
            "by" | "where" | "filter" | "not" | "map" | "emit" | "until" => {
                if let [argument] = step.args.as_slice()
                    && let Some(nested) = values::by_traversal(argument)
                    && needs_path(&parse_chain(&nested, "__")?, depth + 1)?
                {
                    return Ok(true);
                }
            }
            "repeat" => {
                let [nested] = step.args.as_slice() else {
                    bail!("repeat() requires one traversal");
                };
                if needs_path(&parse_chain(nested, "__")?, depth + 1)? {
                    return Ok(true);
                }
            }
            "union" | "coalesce" | "choose" => {
                for argument in &step.args {
                    if needs_path(&parse_chain(argument, "__")?, depth + 1)? {
                        return Ok(true);
                    }
                }
            }
            _ => {}
        }
    }
    Ok(false)
}

impl Traversers<'_> {
    fn entity(&self) -> Result<&Entity> {
        self.entity
            .as_ref()
            .context("step requires a vertex or edge traverser")
    }

    fn apply(&mut self, steps: &[Step<'_>], depth: usize) -> Result<()> {
        ensure!(depth <= 16, "Gremlin nesting exceeds 16");
        let mut steps = steps.iter().peekable();
        while let Some(step) = steps.next() {
            if !self.variants.is_empty()
                && matches!(
                    step.name,
                    "hasLabel"
                        | "has"
                        | "hasKey"
                        | "hasId"
                        | "out"
                        | "in"
                        | "both"
                        | "outE"
                        | "inE"
                        | "bothE"
                        | "outV"
                        | "inV"
                        | "bothV"
                        | "otherV"
                        | "values"
                        | "valueMap"
                        | "label"
                        | "id"
                        | "unfold"
                        | "is"
                        | "as"
                )
            {
                self.dispatch(step, depth)?;
                ensure!(self.sql.len() <= 256 * 1024, "Gremlin plan exceeds 256 KiB");
                continue;
            }
            match (step.name, step.args.as_slice()) {
                ("hasLabel", labels) if !labels.is_empty() => {
                    if !labels_match(&self.entity()?.label, labels)? {
                        self.restrict("FALSE");
                    }
                }
                ("hasId", ids) if !ids.is_empty() => {
                    let entity = self.entity()?;
                    let labels = if entity.edge {
                        id_labels(self.catalog.edges.keys().map(String::as_str))
                    } else {
                        id_labels(self.catalog.nodes.keys().map(String::as_str))
                    };
                    let terms = ids
                        .iter()
                        .map(|arg| key_condition(entity, arg, true, &labels))
                        .collect::<Result<Vec<_>>>()?;
                    self.restrict(&format!("({})", terms.join(" OR ")));
                }
                ("has", [field]) => {
                    self.entity()?;
                    self.has_key(&[field])?;
                }
                ("hasKey", keys) if !keys.is_empty() => self.has_key(keys)?,
                ("has", [field, value]) => self.has(field, value)?,
                ("has", [label, field, value]) => {
                    if self.entity()?.label == text(label)? {
                        self.has(field, value)?;
                    } else {
                        self.restrict("FALSE");
                    }
                }
                ("out" | "in" | "both" | "outE" | "inE" | "bothE", [edge]) => {
                    self.hop(step.name, &text(edge)?)?;
                }
                ("out" | "in" | "both" | "outE" | "inE" | "bothE", []) => {
                    let edges = self.edge_names(step.name)?;
                    if edges.is_empty() {
                        self.restrict("FALSE");
                    } else if let [edge] = edges.as_slice() {
                        self.hop(step.name, edge)?;
                    } else {
                        let branches = edges
                            .iter()
                            .map(|name| {
                                format!(
                                    "__.{}({})",
                                    step.name,
                                    serde_json::to_string(name).expect("serialize edge label")
                                )
                            })
                            .collect::<Vec<_>>();
                        self.branch(
                            "union",
                            &branches.iter().map(String::as_str).collect::<Vec<_>>(),
                            depth,
                        )?;
                    }
                }
                ("outV" | "inV", []) => self.endpoint(step.name == "outV", false)?,
                ("bothV" | "otherV", []) => {
                    let mut source = self.clone();
                    source.endpoint(true, step.name == "otherV")?;
                    let mut target = self.clone();
                    target.endpoint(false, step.name == "otherV")?;
                    self.sql = self.combine(&[source, target]);
                }
                ("values", [field]) => {
                    let field = text(field)?;
                    self.entity()?.check(&field)?;
                    let expression = qualified("v", &field);
                    self.scalar(
                        &expression,
                        true,
                        Kind::Property(self.entity()?.clone(), field),
                    );
                }
                ("valueMap", fields) => {
                    let entity = self.entity()?.clone();
                    let fields = if fields.is_empty() {
                        entity.fields.clone()
                    } else {
                        fields
                            .iter()
                            .map(|field| text(field))
                            .collect::<Result<Vec<_>>>()?
                    };
                    let expression = variants::value_map(&entity, &fields, "v");
                    self.scalar(&expression, false, Kind::Map(entity, fields));
                }
                ("identity", []) => {}
                ("union" | "coalesce" | "choose", args) => self.branch(step.name, args, depth)?,
                ("label", []) => {
                    let label = string(&self.entity()?.label);
                    self.scalar(&label, false, Kind::Label);
                }
                ("id", []) => {
                    let entity = self.entity()?;
                    let value = crate::cypher::element_id(
                        &string(&entity.label),
                        &crate::traversal::text_key("v", &entity.key),
                    );
                    self.scalar(&value, false, Kind::Label);
                }
                ("is", [value]) if self.entity.is_none() => {
                    self.restrict(&condition("v.value", value)?)
                }
                ("count", []) => self.count(),
                ("mean", []) => self.mean()?,
                ("sum", []) => self.sum()?,
                ("min" | "max", []) => self.extremum(step.name == "max")?,
                ("math", [formula]) => {
                    self.math(formula, values::modulators(&mut steps)?, depth)?
                }
                ("filter" | "not", [traversal]) => self.filter(step.name, traversal, depth)?,
                ("map", [traversal]) => {
                    let traversal =
                        values::by_traversal(traversal).context("map() expects a traversal")?;
                    self.branch("map", &[&traversal], depth)?;
                }
                ("where", args) => self.where_values(args, depth)?,
                ("groupCount", []) => {
                    let mut by = values::modulators(&mut steps)?;
                    ensure!(by.len() <= 1, "groupCount() accepts one by() modulator");
                    by.resize(1, None);
                    by.push(Some("count()"));
                    self.group_values(by, depth)?;
                }
                ("fold", []) => self.fold(),
                ("unfold", []) => self.unfold(),
                ("project", fields) => {
                    self.project_values(fields, values::modulators(&mut steps)?, depth)?
                }
                ("group", []) => self.group_values(values::modulators(&mut steps)?, depth)?,
                ("limit", [count]) => self.range(0, Some(integer(count)?)),
                ("skip", [count]) => self.range(integer(count)?, None),
                ("range", [low, high]) => {
                    let (low, high) = parse::range_bounds(low, high)?;
                    self.range(low, high);
                }
                ("order", []) => {
                    let by = steps.next_if(|step| step.name == "by");
                    let (field, descending) = match by.map(|step| step.args.as_slice()) {
                        Some([field]) => (text(field)?, false),
                        Some([field, "asc"]) => (text(field)?, false),
                        Some([field, "desc"]) => (text(field)?, true),
                        None if self.entity.is_none() => ("value".into(), false),
                        _ => bail!("order() on graph elements requires by(property[, asc|desc])"),
                    };
                    if let Some(entity) = &self.entity {
                        entity.check(&field)?;
                    } else if self.variants.is_empty() {
                        ensure!(field == "value", "order() on a scalar requires value");
                    }
                    let column = self.fresh("order");
                    if by.is_some() && !self.variants.is_empty()
                        && (field != "value" || self.variants.iter().any(|kind| matches!(kind, Kind::Element(entity) if entity.fields.contains(&field))))
                    {
                        self.order_property(&field, &column)?;
                    } else {
                        let expression = if self.variants.is_empty() { qualified("v", &field) } else { self.variant_order("v") };
                        self.sql = format!("SELECT v.*, {expression} AS {} FROM ({}) AS v", ident(&column), self.sql);
                    }
                    self.order.push((column, descending));
                }
                ("dedup", []) => {
                    self.dedup(&self.dedup_keys("v").join(", "));
                }
                ("repeat" | "until" | "times" | "emit", _) => {
                    self.repeat_step(step, &mut steps, depth)?;
                }
                ("as", [name]) => {
                    let name = text(name)?;
                    let entity = self.entity()?.clone();
                    let column = variants::tag_column(&name);
                    self.tags.remove(&name);
                    let fields = self.select_columns("v");
                    self.sql = format!(
                        "SELECT {fields}, {} AS {} FROM ({}) AS v",
                        entity.identity("v"),
                        ident(&column),
                        self.sql
                    );
                    self.tags.insert(name, BTreeSet::from([entity]));
                }
                ("select", names) if !names.is_empty() => {
                    self.select_values(names, values::modulators(&mut steps)?, depth)?
                }
                ("simplePath", []) if self.path => {
                    self.restrict(&format!(
                        "array_length({PATH}) = array_length(array_distinct({PATH}))"
                    ));
                }
                ("path", []) if self.path => {
                    self.sql = format!(
                        "SELECT {PATH} AS value{} FROM ({}) AS v",
                        self.carry("v", None),
                        self.sql
                    );
                    self.entity = None;
                    self.variants.clear();
                    self.scalar_kind = Kind::Path;
                }
                _ => bail!("unsupported Gremlin step or arguments: {}", step.name),
            }
            ensure!(self.sql.len() <= 256 * 1024, "Gremlin plan exceeds 256 KiB");
        }
        Ok(())
    }

    fn has(&mut self, field: &str, value: &str) -> Result<()> {
        let field = text(field)?;
        let test = has_condition(&qualified("v", &field), value)?;
        if self.entity()?.fields.contains(&field) {
            self.restrict(&test);
        } else {
            self.restrict("FALSE");
        }
        Ok(())
    }

    fn has_key(&mut self, keys: &[&str]) -> Result<()> {
        let keys = keys
            .iter()
            .map(|key| text(key))
            .collect::<Result<Vec<_>>>()?;
        let tests = if let Some(entity) = &self.entity {
            keys.iter()
                .filter(|key| entity.fields.contains(key))
                .map(|key| format!("{} IS NOT NULL", qualified("v", key)))
                .collect::<Vec<_>>()
        } else if let Kind::Record(fields) = &self.scalar_kind
            && fields
                .iter()
                .any(|(name, kind)| name == "key" && *kind == Kind::Label)
        {
            keys.iter()
                .map(|key| format!("get_field(v.value, 'key') = {}", string(key)))
                .collect()
        } else {
            Vec::new()
        };
        self.restrict(&if tests.is_empty() {
            "FALSE".into()
        } else {
            format!("({})", tests.join(" OR "))
        });
        Ok(())
    }

    fn edge_names(&self, step: &str) -> Result<Vec<String>> {
        let current = self.entity()?;
        ensure!(!current.edge, "{step}() requires vertices");
        let matches = self
            .catalog
            .edges
            .iter()
            .filter(|(_, edge)| {
                (!step.starts_with("in") && current.label == edge.from.node)
                    || (!step.starts_with("out") && current.label == edge.to.node)
            })
            .map(|(name, _)| name.clone())
            .collect();
        Ok(matches)
    }

    fn dedup_keys(&self, alias: &str) -> Vec<String> {
        if !self.variants.is_empty() {
            vec![self.variant_identity(alias)]
        } else if let Some(entity) = &self.entity {
            entity
                .key
                .iter()
                .map(|field| qualified(alias, field))
                .collect()
        } else {
            vec![qualified(alias, "value")]
        }
    }

    fn scalar(&mut self, expression: &str, exclude_nulls: bool, kind: Kind) {
        let identity = scalar_identity(expression);
        let carry = self.carry("v", Some(&identity));
        let predicate = if exclude_nulls {
            format!(" WHERE {expression} IS NOT NULL")
        } else {
            String::new()
        };
        self.sql = format!(
            "SELECT {expression} AS value{carry} FROM ({}) AS v{predicate}",
            self.sql
        );
        self.entity = None;
        self.scalar_kind = kind;
    }

    fn restrict(&mut self, predicate: &str) {
        self.sql = format!("SELECT v.* FROM ({}) AS v WHERE {predicate}", self.sql);
    }

    fn order_terms(&self) -> String {
        self.order
            .iter()
            .rev()
            .map(|(column, descending)| {
                format!(
                    "{} {}",
                    qualified("v", column),
                    if *descending {
                        "DESC NULLS LAST"
                    } else {
                        "ASC NULLS FIRST"
                    }
                )
            })
            .collect::<Vec<_>>()
            .join(", ")
    }

    fn order_clause(&self) -> String {
        if self.order.is_empty() {
            String::new()
        } else {
            format!(" ORDER BY {}", self.order_terms())
        }
    }

    fn order_columns(&self, alias: &str) -> String {
        self.order
            .iter()
            .map(|(column, _)| format!(", {}", qualified(alias, column)))
            .collect()
    }

    fn carry(&self, alias: &str, append: Option<&str>) -> String {
        let mut sql = self.retained_columns(alias);
        for name in self.tags.keys() {
            sql.push_str(&format!(
                ", {}",
                qualified(alias, &variants::tag_column(name))
            ));
        }
        if self.path {
            let current = qualified(alias, PATH);
            let path = append.map_or_else(
                || current.clone(),
                |value| format!("array_append({current}, {value})"),
            );
            sql.push_str(&format!(", {path} AS {PATH}"));
        }
        sql
    }

    fn retained_columns(&self, alias: &str) -> String {
        let mut sql = self.order_columns(alias);
        for name in &self.control_columns {
            sql.push_str(&format!(", {}", qualified(alias, name)));
        }
        sql
    }

    fn hop(&mut self, step: &str, name: &str) -> Result<()> {
        let current = self.entity()?;
        ensure!(!current.edge, "{step}() requires vertices");
        let edge = self
            .catalog
            .edges
            .get(name)
            .with_context(|| format!("unknown edge label {name}"))?;
        let outgoing = step.starts_with("out");
        let incoming = step.starts_with("in");
        let forward = !incoming && current.label == edge.from.node;
        let reverse = !outgoing && current.label == edge.to.node;
        let (from, to) = if reverse && !forward || incoming {
            (&edge.to, &edge.from)
        } else {
            (&edge.from, &edge.to)
        };
        let result = if step.ends_with('E') {
            Entity::edge(self.catalog, name)?
        } else {
            Entity::node(self.catalog, &to.node)?
        };
        if !forward && !reverse {
            let selected = result
                .fields
                .iter()
                .map(|field| qualified("r", field))
                .collect::<Vec<_>>();
            let carry = self.carry("l", Some(&result.identity("r")));
            self.sql = format!(
                "SELECT {}{carry} FROM ({}) AS l CROSS JOIN {} AS r WHERE FALSE",
                selected.join(", "),
                self.sql,
                ident(&result.label)
            );
            self.labels.insert(result.label.clone());
            self.entity = Some(result);
            return Ok(());
        }
        let mut projections = Vec::new();
        let mut endpoint_aliases = Vec::new();
        for (side, cols) in [("from", &from.columns), ("to", &to.columns)] {
            for (index, field) in cols.iter().enumerate() {
                let alias = format!("__graphlake_{side}_{index}");
                projections.push(format!("{} AS {alias}", ident(field)));
                endpoint_aliases.push(alias);
            }
        }
        let edge_table = ident(name);
        let mut oriented = format!("SELECT *, {} FROM {edge_table}", projections.join(", "));
        if forward && reverse {
            let projection = to
                .columns
                .iter()
                .chain(&from.columns)
                .zip(&endpoint_aliases)
                .map(|(field, alias)| format!("{} AS {alias}", ident(field)))
                .collect::<Vec<_>>();
            oriented.push_str(&format!(
                " UNION ALL SELECT *, {} FROM {edge_table}",
                projection.join(", ")
            ));
        }
        let start_keys = &endpoint_aliases[..from.columns.len()];
        let end_keys = &endpoint_aliases[from.columns.len()..];
        let start = equal_keys("l", &current.key, "e", start_keys);
        let carry = self.carry("l", Some(&result.identity("r")));
        if result.edge {
            let output = result
                .fields
                .iter()
                .map(|field| qualified("r", field))
                .collect::<Vec<_>>();
            let start = equal_keys("l", &current.key, "r", start_keys);
            self.sql = format!(
                "SELECT {}{carry} FROM ({}) AS l JOIN ({oriented}) AS r ON {start}",
                output.join(", "),
                self.sql
            );
        } else {
            let end = equal_keys("r", &result.key, "e", end_keys);
            self.sql = format!(
                "SELECT r.*{carry} FROM ({}) AS l JOIN ({oriented}) AS e ON {start} JOIN {} AS r ON {end}",
                self.sql,
                ident(&result.label)
            );
        }
        self.labels.extend([name.to_owned(), result.label.clone()]);
        self.entity = Some(result);
        Ok(())
    }

    fn endpoint(&mut self, source: bool, other: bool) -> Result<()> {
        let current = self.entity()?;
        ensure!(current.edge, "endpoint traversal requires edges");
        let edge = &self.catalog.edges[&current.label];
        let end = if source { &edge.from } else { &edge.to };
        let result = Entity::node(self.catalog, &end.node)?;
        let keys = equal_keys("l", &end.columns, "r", &result.key);
        let carry = self.carry("l", Some(&result.identity("r")));
        let filter = if other {
            let path = qualified("l", PATH);
            let previous = format!(
                "array_element(array_filter({path}, item -> get_field(item, 'kind') = 'vertex'), -1)"
            );
            let at = |endpoint: &crate::catalog::Endpoint| {
                format!(
                    "(get_field({previous}, 'kind') = 'vertex' AND get_field({previous}, 'label') = {} AND get_field({previous}, 'key') = {})",
                    string(&endpoint.node),
                    crate::traversal::text_key("l", &endpoint.columns),
                )
            };
            let from = at(&edge.from);
            if source {
                format!(" WHERE {} AND NOT {from}", at(&edge.to))
            } else {
                format!(" WHERE {from}")
            }
        } else {
            String::new()
        };
        self.sql = format!(
            "SELECT r.*{carry} FROM ({}) AS l JOIN {} AS r ON {keys}{filter}",
            self.sql,
            ident(&result.label)
        );
        self.labels.insert(result.label.clone());
        self.entity = Some(result);
        Ok(())
    }
}

fn scalar_identity(expression: &str) -> String {
    format!(
        "named_struct('label', '', 'kind', 'value', 'key', make_array(__graphlake_gremlin_text({expression})))"
    )
}

fn id_labels<'a>(labels: impl Iterator<Item = &'a str>) -> BTreeMap<String, &'a str> {
    labels
        .map(|label| {
            let mut encoded = String::new();
            crate::cypher::escape_id_component(label, &mut encoded);
            (encoded, label)
        })
        .collect()
}

fn qualified_id_label<'a>(
    expression: &opencypher::Expr,
    labels: &BTreeMap<String, &'a str>,
) -> Option<&'a str> {
    let ExprKind::String(value) = &expression.kind else {
        return None;
    };
    let (prefix, _) = value.strip_suffix(']')?.split_once('[')?;
    labels.get(prefix).copied()
}

fn key_condition(
    entity: &Entity,
    arg: &str,
    mixed: bool,
    labels: &BTreeMap<String, &str>,
) -> Result<String> {
    let expression = parse_expr(arg)?;
    if let ExprKind::String(value) = &expression.kind
        && let Some(label) = qualified_id_label(&expression, labels)
    {
        if label != entity.label {
            return Ok("FALSE".into());
        }
        let id = crate::cypher::element_id(
            &string(&entity.label),
            &crate::traversal::text_key("v", &entity.key),
        );
        return Ok(format!("{id} = {}", string(value)));
    }
    let key = if let ExprKind::List(values) = expression.kind {
        values
    } else {
        vec![expression]
    };
    if mixed {
        if key.len() != entity.key.len() {
            return Ok("FALSE".into());
        }
        let key = key
            .iter()
            .map(|value| Ok(format!("CAST({} AS VARCHAR)", literal(value)?)))
            .collect::<Result<Vec<_>>>()?;
        return Ok(format!(
            "{} = make_array({})",
            crate::traversal::text_key("v", &entity.key),
            key.join(", ")
        ));
    }
    ensure!(
        key.len() == entity.key.len(),
        "wrong business key arity for {}",
        entity.label
    );
    let terms = entity
        .key
        .iter()
        .zip(key)
        .map(|(field, expr)| Ok(format!("{} = {}", qualified("v", field), literal(&expr)?)))
        .collect::<Result<Vec<_>>>()?;
    Ok(format!("({})", terms.join(" AND ")))
}

fn labels_match(label: &str, values: &[&str]) -> Result<bool> {
    let labels = values
        .iter()
        .map(|value| text(value))
        .collect::<Result<Vec<_>>>()?;
    Ok(labels.iter().any(|name| name == label))
}

fn has_condition(field: &str, arg: &str) -> Result<String> {
    let test = condition(field, arg)?;
    Ok(if arg.eq_ignore_ascii_case("null") {
        test
    } else {
        format!("{field} IS NOT NULL AND ({test})")
    })
}

fn condition(field: &str, arg: &str) -> Result<String> {
    let expression = parse_expr(arg)?;
    if let ExprKind::FunctionCall {
        name,
        distinct: false,
        args,
    } = expression.kind
    {
        let op = match name.0.trim_start_matches("P.") {
            "eq" => "=",
            "neq" => "<>",
            "gt" => ">",
            "gte" => ">=",
            "lt" => "<",
            "lte" => "<=",
            "within" | "without" => {
                let within = name.0.ends_with("within");
                let values = if let [
                    opencypher::Expr {
                        kind: ExprKind::List(values),
                        ..
                    },
                ] = args.as_slice()
                {
                    values
                } else {
                    &args
                };
                let values = values
                    .iter()
                    .map(|value| Ok(gremlin_compare(field, "=", &literal(value)?)))
                    .collect::<Result<Vec<_>>>()?;
                let matches = if values.is_empty() {
                    "FALSE".into()
                } else {
                    values.join(" OR ")
                };
                return Ok(if within {
                    format!("({matches})")
                } else {
                    format!("NOT ({matches})")
                });
            }
            _ => bail!("unsupported Gremlin predicate {}", name.0),
        };
        let [value] = args.as_slice() else {
            bail!("{}() requires one argument", name.0);
        };
        return Ok(gremlin_compare(field, op, &literal(value)?));
    }
    if matches!(expression.kind, ExprKind::Null) {
        return Ok(format!("{field} IS NULL"));
    }
    Ok(gremlin_compare(field, "=", &literal(&expression)?))
}

fn gremlin_compare(field: &str, op: &str, value: &str) -> String {
    let unknown = match op {
        "=" => format!("({field} IS NULL AND {value} IS NULL)"),
        "<>" => format!("NOT ({field} IS NULL AND {value} IS NULL)"),
        _ => "FALSE".into(),
    };
    format!(
        "coalesce(__graphlake_cypher_compare({field}, {value}, {}), {unknown})",
        string(op)
    )
}
