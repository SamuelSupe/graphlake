mod grouped;
pub(super) use grouped::Grouped;

use std::collections::{BTreeMap, BTreeSet};

use anyhow::Result;
use opencypher::{Clause, Return, ReturnBody, With};

use super::{Scope, Stage, Value, batch_call, has_identifier, replace_identifiers};
use crate::{
    Catalog,
    catalog::{ident, qualified},
    cypher::{
        Aliases, Binding, MatchPlan, OrderKey, element_id,
        entities::{self, IdentityList},
    },
};

pub(super) struct Page<'a> {
    with: Vec<&'a With>,
    output: Option<&'a ReturnBody>,
    limit: i64,
    skip: i64,
    reserved: BTreeSet<String>,
}

struct Selection {
    sorts: Vec<(String, bool)>,
    filters: Vec<String>,
    fields: BTreeSet<String>,
    marker: String,
}

pub(super) fn describe<'a>(clauses: &'a [Clause], ret: Option<&'a Return>) -> Option<Page<'a>> {
    let mut withs = Vec::new();
    let reserved = clauses
        .iter()
        .filter_map(|clause| match clause {
            Clause::With(with) => Some(&with.body),
            _ => None,
        })
        .chain(ret.map(|ret| &ret.body))
        .flat_map(|body| body.items.iter().map(output_name))
        .collect();
    for clause in clauses {
        let Clause::With(with) = clause else {
            return None;
        };
        if with.distinct
            || with
                .body
                .items
                .iter()
                .any(|item| batch_call::has_aggregate(&item.expr))
        {
            return None;
        }
        withs.push(with);
        if with.body.limit.is_some() {
            return Page::new(withs, None, &with.body, reserved);
        }
        if with.body.skip.is_some() || !with.body.order_by.is_empty() {
            return None;
        }
    }
    let ret = ret.filter(|ret| !ret.distinct)?;
    Page::new(withs, Some(&ret.body), &ret.body, reserved)
}

impl<'a> Page<'a> {
    fn new(
        with: Vec<&'a With>,
        output: Option<&'a ReturnBody>,
        body: &ReturnBody,
        reserved: BTreeSet<String>,
    ) -> Option<Self> {
        if body.order_by.is_empty()
            || body
                .items
                .iter()
                .any(|item| batch_call::has_aggregate(&item.expr))
            || body
                .order_by
                .iter()
                .any(|sort| batch_call::has_aggregate(&sort.expr))
        {
            return None;
        }
        let limit = batch_call::page(Some(body.limit.as_ref()?), 0, "LIMIT").ok()?;
        let skip = batch_call::page(body.skip.as_ref(), 0, "SKIP").ok()?;
        Some(Self {
            with,
            output,
            limit,
            skip,
            reserved,
        })
    }

    pub(super) fn distance(&self) -> usize {
        self.with.len() + usize::from(self.output.is_some())
    }

    pub(super) fn select(
        &self,
        catalog: &Catalog,
        scope: &Scope,
        edge: &str,
        shape: &IdentityList,
        bindings: &[Binding],
        result: &Binding,
        input: &str,
        item: &str,
        id: &str,
    ) -> Result<Option<(String, Vec<OrderKey>)>> {
        let Some(Selection {
            sorts,
            filters,
            fields,
            marker,
        }) = self.expressions(catalog, scope, edge, result)?
        else {
            return Ok(None);
        };
        let (label, key) = shape
            .tag_item(catalog, item)
            .expect("nonempty relationship list");
        let (source, value) = if fields.is_empty() {
            (format!("({input}) AS u"), identity_value(&label, &key))
        } else {
            let (bindings, result) = project_relationship(bindings, result, &fields);
            let payload = IdentityList::indexed_order_columns(
                &bindings,
                &result,
                &format!("make_array({id})"),
            );
            let mut name = "__graphlake_sort_relationship".to_owned();
            while scope.columns.contains_key(&name) {
                name.push('_');
            }
            let column = ident(&name);
            (
                format!(
                    "(SELECT u.*, array_element({payload}, 1) AS {column} FROM ({input}) AS u WHERE {item} IS NOT NULL) AS u"
                ),
                qualified("u", &name),
            )
        };
        let mut names: BTreeSet<_> = scope.columns.keys().cloned().collect();
        // Reserve all user-visible aliases so hidden order columns cannot collide on replay.
        names.insert(edge.to_owned());
        names.extend(self.reserved.iter().cloned());
        let mut order = Vec::new();
        let projections = sorts
            .iter()
            .enumerate()
            .map(|(index, (expression, descending))| {
                let column = crate::cypher::order_column(index, &mut names);
                let key = replace_identifiers(expression, &[(&marker, value.clone())]);
                order.push(OrderKey {
                    column: column.clone(),
                    descending: *descending,
                    native: false,
                });
                format!("{key} AS {}", ident(&column))
            })
            .collect::<Vec<_>>();
        let filters = std::iter::once(format!("{item} IS NOT NULL"))
            .chain(
                filters
                    .iter()
                    .map(|filter| replace_identifiers(filter, &[(&marker, value.clone())])),
            )
            .collect::<Vec<_>>()
            .join(" AND ");
        let terms = order
            .iter()
            .map(|key| key.term(None))
            .collect::<Vec<_>>()
            .join(", ");
        let limit = self.limit;
        let skip = self.skip;
        Ok(Some((
            format!(
                "SELECT {carry}, {} FROM {source} WHERE {filters} ORDER BY {terms} LIMIT {limit} OFFSET {skip}",
                projections.join(", "),
                carry = scope
                    .columns
                    .keys()
                    .map(|name| qualified("u", name))
                    .chain([item.to_owned()])
                    .collect::<Vec<_>>()
                    .join(", "),
            ),
            order,
        )))
    }

    fn expressions(
        &self,
        catalog: &Catalog,
        scope: &Scope,
        edge: &str,
        result: &Binding,
    ) -> Result<Option<Selection>> {
        let mut visible = scope.clone();
        visible
            .columns
            .insert(edge.to_owned(), Value::Entity(result.clone()));
        // Lower the narrow phase using the ordinary expression and alias semantics.
        // The stored value is replaced only after inspecting what was read.
        let mut placeholder = "__graphlake_path_sort_input".to_owned();
        while visible.columns.contains_key(&placeholder) || self.reserved.contains(&placeholder) {
            placeholder.push('_');
        }
        let marker = ident(&placeholder);
        let mut ingress = MatchPlan::new(catalog);
        visible.bind("u", &mut ingress);
        ingress
            .bindings
            .get_mut(edge)
            .expect("relationship binding")
            .stored = Some(marker.clone());
        let mut aliases: Aliases = visible
            .columns
            .iter()
            .map(|(name, kind)| Ok((name.clone(), (ingress.variable(name)?, kind.clone()))))
            .collect::<Result<_>>()?;
        if let Some((_, Value::Entity(binding))) = aliases.get_mut(edge) {
            binding.stored = Some(marker.clone());
        }
        let mut filters = Vec::new();
        let mut sorts = Vec::new();
        for (body, predicate) in self
            .with
            .iter()
            .enumerate()
            .map(|(index, with)| {
                // The predicate attached to the paged WITH runs after the page.
                let predicate = if self.output.is_none() && index + 1 == self.with.len() {
                    None
                } else {
                    with.where_clause.as_ref()
                };
                (&with.body, predicate)
            })
            .chain(self.output.map(|body| (body, None)))
        {
            let input = MatchPlan::new(catalog);
            let mut projected = if body.star {
                aliases.clone()
            } else {
                BTreeMap::new()
            };
            for item in &body.items {
                let name = output_name(item);
                if projected.contains_key(&name) {
                    return Ok(None);
                }
                // Only expressions used for ordering and filtering go into the narrow SQL.
                let Ok(sql) = input.expr_with_aliases(&item.expr, &aliases) else {
                    return Ok(None);
                };
                let kind = Value::inferred(&input, &item.expr, &aliases);
                projected.insert(name, (sql, kind));
            }
            let mut lookup = aliases.clone();
            lookup.extend(projected.clone());
            if let Some(predicate) = predicate {
                let Ok(sql) = input.expr_with_aliases(predicate, &lookup) else {
                    return Ok(None);
                };
                filters.push(sql);
            }
            let projected_scope = MatchPlan::new(catalog);
            for item in &body.order_by {
                // Match the normal planner's preference for the projected scope.
                let Ok(sql) = projected_scope
                    .expr_with_aliases(&item.expr, &projected)
                    .or_else(|_| input.expr_with_aliases(&item.expr, &lookup))
                else {
                    return Ok(None);
                };
                sorts.push((sql, item.descending));
            }
            aliases = projected;
            // ORDER BY and WHERE in later WITH clauses see only retained names.
            if !input.expression_joins.borrow().is_empty()
                || !input.expression_expansions.borrow().is_empty()
                || !projected_scope.expression_joins.borrow().is_empty()
                || !projected_scope.expression_expansions.borrow().is_empty()
            {
                return Ok(None);
            }
        }
        let mut fields = BTreeSet::new();
        let mut binding = result.clone();
        binding.stored = Some(marker.clone());
        let reads = result
            .properties
            .iter()
            .map(|field| (field, binding.field(field)))
            .collect::<Vec<_>>();
        for expression in sorts.iter().map(|(sql, _)| sql).chain(&filters) {
            let mut remaining = expression.clone();
            for (field, read) in &reads {
                if read != "NULL" && remaining.contains(read) {
                    fields.insert((*field).clone());
                    remaining = remaining.replace(read, "");
                }
            }
            // Entity, map and dynamic-key observations need the normal full plan.
            for read in [
                format!("get_field({marker}, 'label')"),
                format!("get_field({marker}, 'key')"),
                format!("{marker} IS NOT NULL"),
                format!("{marker} IS NULL"),
            ] {
                remaining = remaining.replace(&read, "");
            }
            if has_identifier(&remaining, &marker) {
                return Ok(None);
            }
        }
        Ok(Some(Selection {
            sorts,
            filters,
            fields,
            marker,
        }))
    }
}

pub(super) fn project_relationship(
    bindings: &[Binding],
    result: &Binding,
    fields: &BTreeSet<String>,
) -> (Vec<Binding>, Binding) {
    let mut bindings = bindings.to_vec();
    let mut result = result.clone();
    for binding in bindings.iter_mut().chain([&mut result]) {
        binding.properties.retain(|field| fields.contains(field));
        for lane in binding.lanes.values_mut() {
            lane.retain(|field| fields.contains(field));
        }
    }
    (bindings, result)
}

pub(super) fn relationship(
    catalog: &Catalog,
    shape: &IdentityList,
    bindings: &[Binding],
    result: &Binding,
    item: &str,
    id: &str,
    identity_only: bool,
) -> String {
    if identity_only {
        let (label, key) = shape
            .tag_item(catalog, item)
            .expect("relationship identity");
        return identity_value(&label, &key);
    }
    let read = IdentityList::indexed_order_columns(bindings, result, &format!("make_array({id})"));
    format!("array_element({read}, 1)")
}

fn identity_value(label: &str, key: &str) -> String {
    format!(
        "named_struct('kind', 'relationship', 'label', {label}, 'key', {key}, 'properties', named_struct('_empty', NULL))"
    )
}

pub(super) fn restore(stage: &mut Stage, catalog: &Catalog, edges: &BTreeSet<String>) {
    let mut hydrate = false;
    let mut by_label = BTreeMap::new();
    let mut values = Vec::new();
    for (name, value) in &mut stage.scope.columns {
        let input = qualified("l", name);
        let Value::Entity(binding) = value else {
            values.push(input);
            continue;
        };
        if binding.node || !edges.contains(name) {
            values.push(input);
            continue;
        }
        let labels = if binding.dynamic {
            binding.lanes.keys().cloned().collect::<Vec<_>>()
        } else {
            vec![binding.label.clone()]
        };
        let bindings = labels
            .iter()
            .map(|label| {
                by_label
                    .entry(label.clone())
                    .or_insert_with(|| entities::edge_binding(catalog, label))
                    .clone()
            })
            .collect::<Vec<_>>();
        let full = entities::merged(&bindings);
        let (label, key) = entities::identity(&input);
        let id = element_id(&label, &key);
        let read = IdentityList::indexed_lookup(&bindings, &full, &format!("make_array({id})"));
        values.push(format!("array_element({read}, 1) AS {}", ident(name)));
        *binding = full;
        hydrate = true;
    }
    if hydrate {
        values.extend(stage.order.iter().map(|key| qualified("l", &key.column)));
        stage.sql = format!("SELECT {}{}", values.join(", "), stage.input());
    }
}

fn output_name(item: &opencypher::ReturnItem) -> String {
    item.alias
        .as_ref()
        .map_or_else(|| item.expr.to_string(), |name| name.0.clone())
}
