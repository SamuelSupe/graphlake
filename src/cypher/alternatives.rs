use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Result, bail, ensure};
use opencypher::{Direction, LabelExpr, Match, NodePattern, RelationshipPattern};

use crate::Catalog;

use super::clauses::{Scope, Value};

pub(super) fn has_untyped(catalog: &Catalog, pattern: &Match) -> bool {
    pattern.patterns.iter().any(|part| {
        (catalog.nodes.len() > 1
            && part.element.chain.is_empty()
            && part.element.start.labels.is_none())
            || matches!(part.element.start.labels, Some(LabelExpr::Disjunction(..)))
            || part.element.chain.iter().any(|(rel, node)| {
                !matches!(rel.types, Some(LabelExpr::Leaf(_)))
                    || matches!(node.labels, Some(LabelExpr::Disjunction(..)))
            })
    })
}

fn node_labels(catalog: &Catalog, expression: &LabelExpr) -> Result<BTreeSet<String>> {
    match expression {
        LabelExpr::Leaf(label) => {
            ensure!(
                catalog.nodes.contains_key(label),
                "unknown node type {label}"
            );
            Ok(BTreeSet::from([label.clone()]))
        }
        LabelExpr::Disjunction(left, right) => {
            let mut labels = node_labels(catalog, left)?;
            labels.extend(node_labels(catalog, right)?);
            Ok(labels)
        }
        _ => bail!("node patterns support a label or a label disjunction"),
    }
}

pub(super) struct Node<'a> {
    pub patterns: Vec<&'a NodePattern>,
    pub labels: BTreeSet<String>,
}

pub(super) struct Domains<'a> {
    pub nodes: Vec<Node<'a>>,
    pub paths: Vec<Vec<usize>>,
    pub impossible: bool,
}

impl<'a> Domains<'a> {
    pub fn new(catalog: &Catalog, pattern: &'a Match, prior: &Scope) -> Result<Self> {
        let mut domains = Self {
            nodes: Vec::new(),
            paths: Vec::new(),
            impossible: false,
        };
        let mut named = BTreeMap::new();
        for part in &pattern.patterns {
            let mut path = Vec::new();
            for node in std::iter::once(&part.element.start)
                .chain(part.element.chain.iter().map(|(_, node)| node))
            {
                let mut allowed = match &node.labels {
                    Some(labels) => node_labels(catalog, labels)?,
                    None => catalog.nodes.keys().cloned().collect(),
                };
                if let Some(Value::Entity(binding)) = node
                    .variable
                    .as_ref()
                    .and_then(|name| prior.columns.get(&name.0))
                    && !binding.dynamic
                    && binding.node
                {
                    allowed.retain(|label| *label == binding.label);
                }
                let index = match node.variable.as_ref().and_then(|name| named.get(&name.0)) {
                    Some(&index) => index,
                    None => {
                        let index = domains.nodes.len();
                        if let Some(name) = &node.variable {
                            named.insert(name.0.clone(), index);
                        }
                        domains.nodes.push(Node {
                            patterns: Vec::new(),
                            labels: catalog.nodes.keys().cloned().collect(),
                        });
                        index
                    }
                };
                let domain = &mut domains.nodes[index];
                let intersection = domain
                    .labels
                    .intersection(&allowed)
                    .cloned()
                    .collect::<BTreeSet<_>>();
                if intersection.is_empty() {
                    domains.impossible = true;
                } else {
                    domain.labels = intersection;
                }
                domain.patterns.push(node);
                path.push(index);
            }
            domains.paths.push(path);
        }
        // Narrow local domains to a fixed point. Endpoint combinations are
        // joined at execution, including cycles and repeated variables.
        loop {
            let mut changed = false;
            for (part, path) in pattern.patterns.iter().zip(&domains.paths) {
                for (index, (rel, _)) in part.element.chain.iter().enumerate() {
                    let (a, b) = (path[index], path[index + 1]);
                    if !matches!(rel.types, Some(LabelExpr::Leaf(_))) && rel.range.is_some() {
                        edge_labels(catalog, rel)?;
                        continue;
                    }
                    let left = &domains.nodes[a].labels;
                    let right = &domains.nodes[b].labels;
                    let mut from = BTreeSet::new();
                    let mut to = BTreeSet::new();
                    for (name, swap) in orientations(catalog, rel, left, right)? {
                        let edge = &catalog.edges[&name];
                        let (src, dst) = if swap {
                            (&edge.to.node, &edge.from.node)
                        } else {
                            (&edge.from.node, &edge.to.node)
                        };
                        if a == b && src != dst {
                            continue;
                        }
                        from.insert(src.clone());
                        to.insert(dst.clone());
                    }
                    if rel.range.is_some() {
                        if rel.direction == Direction::Undirected
                            && let Some(LabelExpr::Leaf(label)) = &rel.types
                        {
                            // An undirected typed range may return to the start label.
                            let edge = &catalog.edges[label];
                            let ends = [&edge.from.node, &edge.to.node];
                            from.extend(left.iter().filter(|label| ends.contains(label)).cloned());
                            to.extend(right.iter().filter(|label| ends.contains(label)).cloned());
                        }
                        if rel.range.is_some_and(|range| range.min == Some(0)) {
                            from.extend(left.intersection(right).cloned());
                            to.extend(left.intersection(right).cloned());
                        }
                    }
                    if from.is_empty() || to.is_empty() {
                        domains.impossible = true;
                        continue;
                    }
                    for (index, labels) in [(a, from), (b, to)] {
                        let current = &mut domains.nodes[index].labels;
                        let narrowed = current
                            .intersection(&labels)
                            .cloned()
                            .collect::<BTreeSet<_>>();
                        if narrowed.is_empty() {
                            domains.impossible = true;
                        } else if narrowed != *current {
                            *current = narrowed;
                            changed = true;
                        }
                    }
                }
            }
            if !changed {
                break;
            }
        }
        Ok(domains)
    }

    pub fn single(&self, catalog: &Catalog, pattern: &Match) -> Result<Option<Match>> {
        if self.impossible || self.nodes.iter().any(|node| node.labels.len() != 1) {
            return Ok(None);
        }
        let mut typed = pattern.clone();
        for (part, path) in typed.patterns.iter_mut().zip(&self.paths) {
            let label = |index: usize| self.nodes[path[index]].labels.first().expect("singleton");
            part.element.start.labels = Some(LabelExpr::Leaf(label(0).clone()));
            for (index, (rel, node)) in part.element.chain.iter_mut().enumerate() {
                node.labels = Some(LabelExpr::Leaf(label(index + 1).clone()));
                if matches!(rel.types, Some(LabelExpr::Leaf(_))) {
                    continue;
                }
                if rel.range.is_some()
                    && (rel.types.is_some() || catalog.nodes.len() != 1 || catalog.edges.len() != 1)
                {
                    return Ok(None);
                }
                let mut choices = orientations(
                    catalog,
                    rel,
                    &self.nodes[path[index]].labels,
                    &self.nodes[path[index + 1]].labels,
                )?;
                choices.dedup_by(|a, b| a.0 == b.0);
                let [(name, _)] = choices.as_slice() else {
                    return Ok(None);
                };
                rel.types = Some(LabelExpr::Leaf(name.clone()));
            }
        }
        Ok(Some(typed))
    }
}

pub(super) fn edge_labels(
    catalog: &Catalog,
    rel: &RelationshipPattern,
) -> Result<BTreeSet<String>> {
    fn add(catalog: &Catalog, expr: &LabelExpr, labels: &mut BTreeSet<String>) -> Result<()> {
        match expr {
            LabelExpr::Leaf(label) => {
                ensure!(
                    catalog.edges.contains_key(label),
                    "unknown edge type {label}"
                );
                labels.insert(label.clone());
            }
            LabelExpr::Disjunction(left, right) => {
                add(catalog, left, labels)?;
                add(catalog, right, labels)?;
            }
            _ => bail!("relationship patterns support a type or a type disjunction"),
        }
        Ok(())
    }
    if let Some(expr) = &rel.types {
        let mut labels = BTreeSet::new();
        add(catalog, expr, &mut labels)?;
        Ok(labels)
    } else {
        Ok(catalog.edges.keys().cloned().collect())
    }
}

pub(super) fn orientations(
    catalog: &Catalog,
    rel: &RelationshipPattern,
    left: &BTreeSet<String>,
    right: &BTreeSet<String>,
) -> Result<Vec<(String, bool)>> {
    let labels = edge_labels(catalog, rel)?;
    ensure!(
        rel.direction != Direction::Both,
        "relationship arrows at both ends are unsupported"
    );
    let mut result = Vec::new();
    for (name, edge) in &catalog.edges {
        if !labels.contains(name) {
            continue;
        }
        if rel.direction != Direction::Incoming
            && left.contains(&edge.from.node)
            && right.contains(&edge.to.node)
        {
            result.push((name.clone(), false));
        }
        if rel.direction != Direction::Outgoing
            && left.contains(&edge.to.node)
            && right.contains(&edge.from.node)
        {
            result.push((name.clone(), true));
        }
    }
    Ok(result)
}
