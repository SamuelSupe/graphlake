use std::collections::BTreeSet;

use crate::{
    Catalog,
    catalog::{columns, ident, qualified},
};

use super::{
    Aliases, Binding, MatchPlan,
    clauses::{ExpressionJoin, Value, has_identifier, lambda_values, replace_identifiers},
    element_id, string,
};
use opencypher::{Expr, ExprKind};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct IdentityList {
    pub(super) labels: BTreeSet<String>,
    pub(super) tagged: bool,
    pub(super) node: bool,
}

impl IdentityList {
    pub(super) fn indexed_lookup(bindings: &[Binding], result: &Binding, ids: &str) -> String {
        Self::hydrate("__graphlake_csr_hydrate", bindings, result, ids)
    }

    pub(super) fn indexed_order_columns(
        bindings: &[Binding],
        result: &Binding,
        ids: &str,
    ) -> String {
        Self::hydrate("__graphlake_csr_order_columns", bindings, result, ids)
    }

    fn hydrate(function: &str, bindings: &[Binding], result: &Binding, ids: &str) -> String {
        let types = Self::lookup(bindings, result);
        let kind = if result.node { "node" } else { "relationship" };
        format!("{function}({ids}, (SELECT payload FROM ({types}) AS csr_types LIMIT 0), '{kind}')")
    }
    pub(super) fn single(label: &str) -> Self {
        Self {
            labels: BTreeSet::from([label.to_owned()]),
            tagged: false,
            node: false,
        }
    }
}

#[derive(Clone)]
pub(super) struct PathTypes {
    pub(super) nodes: IdentityList,
    pub(super) edges: IdentityList,
}

pub(super) fn identity_list(
    plan: &MatchPlan,
    expr: &Expr,
    aliases: &Aliases,
) -> Option<IdentityList> {
    match &expr.kind {
        ExprKind::Variable(name) => match aliases.get(&name.0) {
            Some((_, Value::Identities(shape))) => Some(shape.clone()),
            Some(_) => None,
            None => plan.list_types.get(&name.0).cloned(),
        },
        ExprKind::Slice { base, .. } => identity_list(plan, base, aliases),
        ExprKind::Index { base, .. } => match Value::nested_item(plan, base, aliases)? {
            Value::Identities(shape) => Some(shape),
            _ => None,
        },
        ExprKind::Case { whens, default, .. } => common_identities(
            plan,
            whens
                .iter()
                .map(|(_, value)| value)
                .chain(default.as_deref()),
            aliases,
        ),
        ExprKind::FunctionCall {
            name,
            args,
            distinct: false,
        } if name.0.eq_ignore_ascii_case("coalesce") => {
            common_identities(plan, args.iter(), aliases)
        }
        ExprKind::FunctionCall {
            name,
            args,
            distinct: false,
        } if name.0.eq_ignore_ascii_case("nodes")
            || name.0.eq_ignore_ascii_case("relationships") =>
        {
            let [arg] = args.as_slice() else { return None };
            let Value::Path(shape) = Value::inferred(plan, arg, aliases) else {
                return None;
            };
            if name.0.eq_ignore_ascii_case("nodes") {
                Some(shape.nodes)
            } else {
                Some(shape.edges)
            }
        }
        _ => None,
    }
}

fn common_identities<'a>(
    plan: &MatchPlan,
    values: impl Iterator<Item = &'a Expr>,
    aliases: &Aliases,
) -> Option<IdentityList> {
    let mut shape: Option<IdentityList> = None;
    for value in values {
        if matches!(value.kind, ExprKind::Null)
            || matches!(&value.kind, ExprKind::List(items) if items.is_empty())
        {
            continue;
        }
        let next = identity_list(plan, value, aliases)?;
        if let Some(shape) = &mut shape {
            if shape.node != next.node {
                return None;
            }
            shape.merge(&next);
        } else {
            shape = Some(next);
        }
    }
    shape
}

impl IdentityList {
    pub(super) fn merge(&mut self, other: &Self) {
        if self != other {
            self.labels.extend(other.labels.iter().cloned());
            self.tagged = true;
        }
    }

    pub(super) fn bindings(&self, catalog: &Catalog) -> Vec<Binding> {
        self.labels
            .iter()
            .map(|label| {
                if self.node {
                    let node = &catalog.nodes[label];
                    Binding {
                        alias: "e".into(),
                        label: label.clone(),
                        properties: columns([&node.key, &node.properties]),
                        keys: node.key.clone(),
                        stored: None,
                        node: true,
                        dynamic: false,
                        lanes: Default::default(),
                    }
                } else {
                    edge_binding(catalog, label)
                }
            })
            .collect()
    }

    pub(super) fn lookup(bindings: &[Binding], result: &Binding) -> String {
        let reads = bindings
            .iter()
            .map(|binding| {
                let key = crate::traversal::text_key("e", &binding.keys);
                let id = element_id(&string(&binding.label), &key);
                let value = shape(binding, result, &binding.value());
                format!(
                    "SELECT {id} AS __graphlake_lookup_id, {value} AS payload FROM {} AS e",
                    ident(&binding.label)
                )
            })
            .collect::<Vec<_>>();
        crate::catalog::union_all(&reads)
    }

    pub(super) fn tag_item(&self, catalog: &Catalog, item: &str) -> Option<(String, String)> {
        if self.tagged {
            return Some(identity(item));
        }
        let label = self.labels.first()?;
        let keys = if self.node {
            catalog.nodes[label].key.clone()
        } else {
            catalog.edges[label].identity()
        };
        let keys = keys
            .iter()
            .map(|key| format!("CAST(get_field({item}, {}) AS VARCHAR)", string(key)))
            .collect::<Vec<_>>();
        Some((string(label), format!("make_array({})", keys.join(", "))))
    }

    pub(super) fn tagged_list(&self, catalog: &Catalog, list: &str) -> String {
        if self.tagged {
            return list.to_owned();
        }
        let Some((label, key)) = self.tag_item(catalog, "item") else {
            return list.to_owned();
        };
        format!("array_transform({list}, item -> named_struct('label', {label}, 'key', {key}))")
    }

    pub(super) fn item_id(&self, catalog: &Catalog, item: &str) -> Option<String> {
        let (label, key) = self.tag_item(catalog, item)?;
        Some(element_id(&label, &key))
    }

    fn comparable(&self, catalog: &Catalog, list: &str) -> String {
        let Some((label, key)) = self.tag_item(catalog, "item") else {
            return list.to_owned();
        };
        let kind = if self.node { "node" } else { "relationship" };
        format!(
            "array_transform({list}, item -> __graphlake_cypher_entity(CASE WHEN item IS NOT NULL THEN named_struct('kind', '{kind}', 'label', {label}, 'key', {key}, 'properties', named_struct('unused', NULL)) END))"
        )
    }
}

impl MatchPlan<'_> {
    pub(super) fn comparison_value(
        &self,
        expr: &Expr,
        aliases: &Aliases,
    ) -> anyhow::Result<String> {
        if let ExprKind::List(items) = &expr.kind
            && !items.is_empty()
        {
            let items = items
                .iter()
                .map(|item| self.comparison_value(item, aliases))
                .collect::<anyhow::Result<Vec<_>>>()?;
            return Ok(format!("__graphlake_cypher_list({})", items.join(", ")));
        }
        let value = self.expr_with_aliases(expr, aliases)?;
        let Some(catalog) = self.catalog else {
            return Ok(value);
        };
        if let Some(shape) = identity_list(self, expr, aliases) {
            return Ok(shape.comparable(catalog, &value));
        }
        fn nested(catalog: &Catalog, item: &Value, value: &str, depth: usize) -> String {
            match item {
                Value::Identities(shape) => shape.comparable(catalog, value),
                Value::NestedList(child) => {
                    let var = format!("__graphlake_compare_item_{depth}");
                    let child = nested(catalog, child, &var, depth + 1);
                    format!("array_transform({value}, {var} -> {child})")
                }
                _ => value.to_owned(),
            }
        }
        if let Some(item) = Value::nested_item(self, expr, aliases) {
            return Ok(nested(
                catalog,
                &Value::NestedList(Box::new(item)),
                &value,
                0,
            ));
        }
        Ok(value)
    }

    pub(super) fn entity_list_input(
        &self,
        expr: &Expr,
        aliases: &Aliases,
    ) -> anyhow::Result<String> {
        if let ExprKind::Slice { base, from, to } = &expr.kind
            && identity_list(self, base, aliases).is_some()
            && aliases
                .values()
                .any(|(alias, _)| alias.starts_with("\"__graphlake_item_"))
        {
            let list = self.entity_list_input(base, aliases)?;
            return self.slice_list(&list, from.as_deref(), to.as_deref(), aliases);
        }
        let list = self.expr_with_aliases(expr, aliases)?;
        let Some(shape) =
            identity_list(self, expr, aliases).filter(|shape| !shape.labels.is_empty())
        else {
            return Ok(list);
        };
        let catalog = self
            .catalog
            .expect("path identity list requires graph catalog");
        let bindings = shape.bindings(catalog);
        let result = merged(&bindings);
        let lookup = IdentityList::lookup(&bindings, &result);
        self.expression_labels
            .borrow_mut()
            .extend(shape.labels.iter().cloned());
        let item = shape
            .item_id(catalog, "item")
            .expect("nonempty identity labels");
        let ids = format!("array_transform({list}, item -> {item})");
        let frames = self.lambda_inputs.borrow();
        let captured = frames
            .iter()
            .enumerate()
            .filter(|(_, (item, _))| has_identifier(&ids, item))
            .collect::<Vec<_>>();
        if !captured.is_empty() {
            let mut joins = self.expression_joins.borrow_mut();
            let alias = format!(
                "__graphlake_nested_path_{}_{}",
                expr.span.start,
                joins.len()
            );
            let mut seed = format!("{alias}_seed");
            while catalog.nodes.contains_key(&seed) || catalog.edges.contains_key(&seed) {
                seed.push('_');
            }
            let (paths, expand) = match captured.as_slice() {
                [(index, (item, input))]
                    if frames[..*index]
                        .iter()
                        .all(|(parent, _)| !has_identifier(input, parent)) =>
                {
                    let ids = replace_identifiers(&ids, &[(item, qualified("p", "item_value"))]);
                    (
                        input.clone(),
                        format!(
                            "SELECT p.paths, {ids} AS ids FROM (SELECT paths, unnest(paths) AS item_value FROM {}) AS p",
                            ident(&seed)
                        ),
                    )
                }
                _ => (
                    lambda_values(
                        &frames,
                        captured.iter().map(|(_, (item, _))| item.as_str()),
                        &ids,
                        &alias,
                    ),
                    format!("SELECT paths, unnest(paths) AS ids FROM {}", ident(&seed)),
                ),
            };
            let captures = vec![("paths".into(), paths)];
            let query = if self.indexed_csr {
                let payloads = IdentityList::indexed_lookup(&bindings, &result, "u.ids");
                format!(
                    "SELECT u.paths, named_struct('ids', u.ids, 'values', {payloads}) AS payloads FROM
                    (SELECT d.paths, array_agg(d.id ORDER BY d.id) AS ids FROM
                    (SELECT DISTINCT p.paths, unnest(p.ids) AS id FROM ({expand}) AS p) AS d GROUP BY d.paths) AS u"
                )
            } else {
                format!(
                    "SELECT u.paths, named_struct('ids', array_agg(u.id ORDER BY u.id),
                    'values', array_agg(items.payload ORDER BY u.id)) AS payloads
                    FROM (SELECT DISTINCT p.paths, unnest(p.ids) AS id FROM ({expand}) AS p) AS u
                    LEFT JOIN ({lookup}) AS items ON u.id = items.__graphlake_lookup_id GROUP BY u.paths"
                )
            };
            joins.push(ExpressionJoin {
                alias: alias.clone(),
                seed,
                captures,
                query,
                expanded: None,
                calls: Default::default(),
            });
            return Ok(format!(
                "__graphlake_cypher_hydrate({ids}, {})",
                qualified(&alias, "payloads")
            ));
        }
        drop(frames);
        let mut joins = self.expression_joins.borrow_mut();
        let captures = vec![("ids".to_owned(), ids.clone())];
        let alias = if let Some(join) = joins.iter().find(|join| {
            join.alias.starts_with("__graphlake_path_result_") && join.captures == captures
        }) {
            join.alias.clone()
        } else {
            let alias = format!(
                "__graphlake_path_result_{}_{}",
                expr.span.start,
                joins.len()
            );
            let mut seed = format!("{alias}_seed");
            while catalog.nodes.contains_key(&seed) || catalog.edges.contains_key(&seed) {
                seed.push('_');
            }
            let query = if self.indexed_csr {
                let lookup = IdentityList::indexed_lookup(&bindings, &result, "ids");
                format!("SELECT ids, {lookup} AS values FROM {}", ident(&seed))
            } else {
                format!(
                    "SELECT u.ids, array_agg(items.payload ORDER BY u.position) AS values
                 FROM (SELECT ids, unnest(ids) AS id,
                       unnest(range(0, CAST(array_length(ids) AS BIGINT))) AS position
                       FROM {}) AS u
                 LEFT JOIN ({lookup}) AS items ON u.id = items.__graphlake_lookup_id
                 GROUP BY u.ids",
                    ident(&seed)
                )
            };
            joins.push(ExpressionJoin {
                alias: alias.clone(),
                seed,
                captures,
                query,
                expanded: None,
                calls: Default::default(),
            });
            alias
        };
        Ok(format!(
            "CASE WHEN {list} IS NOT NULL THEN coalesce({}, make_array()) END",
            qualified(&alias, "values")
        ))
    }
}

pub(super) fn edge_binding(catalog: &Catalog, label: &str) -> Binding {
    let edge = &catalog.edges[label];
    let keys = edge.identity();
    Binding {
        alias: "e".into(),
        label: label.into(),
        properties: columns([&keys, &edge.properties]),
        keys,
        stored: None,
        node: false,
        dynamic: false,
        lanes: Default::default(),
    }
}

pub(super) fn merged(bindings: &[Binding]) -> Binding {
    let mut result = bindings[0].clone();
    if bindings
        .iter()
        .all(|binding| !binding.dynamic && binding.label == result.label)
    {
        return result;
    }
    result.dynamic = true;
    result.label.clear();
    result.keys.clear();
    result.properties = bindings
        .iter()
        .flat_map(|binding| binding.properties.iter().cloned())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    result.lanes.clear();
    for binding in bindings {
        if binding.dynamic {
            result.lanes.extend(binding.lanes.clone());
        } else {
            result
                .lanes
                .insert(binding.label.clone(), binding.properties.clone());
        }
    }
    result
}

pub(super) fn identity(value: &str) -> (String, String) {
    (
        format!("get_field({value}, 'label')"),
        format!("get_field({value}, 'key')"),
    )
}

pub(super) fn property(value: &str, field: &str) -> String {
    format!(
        "get_field(get_field({value}, 'properties'), {})",
        string(field)
    )
}

pub(super) fn shape(source: &Binding, target: &Binding, value: &str) -> String {
    if !target.dynamic || source.dynamic && source.lanes == target.lanes {
        return value.to_owned();
    }
    let (label, key) = identity(value);
    let kind = if target.node { "node" } else { "relationship" };
    let lanes = target
        .lanes
        .iter()
        .flat_map(|(name, properties)| {
            let lane = if source.dynamic && source.lanes.contains_key(name) {
                format!("get_field(get_field({value}, 'lanes'), {})", string(name))
            } else if !source.dynamic && name == &source.label {
                format!("get_field({value}, 'properties')")
            } else {
                // Bind the lane's type before SQL UNION coercion so lambda parameters
                // over collected elements see the same struct on every branch.
                let fields = properties
                    .iter()
                    .flat_map(|field| [string(field), qualified("__graphlake_empty_lane", field)])
                    .collect::<Vec<_>>();
                if fields.is_empty() {
                    "(CASE WHEN FALSE THEN named_struct('_empty', NULL) END)".to_owned()
                } else {
                    format!(
                        "(SELECT named_struct({}) FROM {} AS __graphlake_empty_lane LIMIT 0)",
                        fields.join(", "),
                        ident(name)
                    )
                }
            };
            [string(name), lane]
        })
        .collect::<Vec<_>>();
    format!(
        "__graphlake_cypher_entity(CASE WHEN {value} IS NULL THEN NULL ELSE named_struct('kind', '{kind}', 'label', {label}, 'key', {key}, 'lanes', named_struct({})) END)",
        lanes.join(", ")
    )
}

pub(super) fn match_key(binding: &Binding, item: &str) -> String {
    binding
        .keys
        .iter()
        .map(|field| {
            let column = qualified("e", field);
            format!("{column} = get_field({item}, {})", string(field))
        })
        .collect::<Vec<_>>()
        .join(" AND ")
}
