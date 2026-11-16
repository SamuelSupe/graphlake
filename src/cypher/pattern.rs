use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result, bail, ensure};
use opencypher::{
    BinaryOp, CallKind, Clause, ComparisonOp, Direction, Expr, ExprKind, LabelExpr, NodePattern,
    Properties, Query, Variable, Visitor, walk_expr,
};

use super::{
    MatchPlan, ShortestRanking, clauses,
    entities::{IdentityList, PathTypes},
    literal, paths, property_terms, property_terms_with, start_terms, string,
};
use crate::{
    Catalog,
    catalog::{columns, equal_keys, ident, qualified},
    traversal::{Expansion, HOPS, PATH, PATH_NODES},
};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum MatchTypes {
    Explicit,
    Inferred,
}

// Correlated inline values belong in the same join predicate as WHERE.
// The independent pattern relation cannot refer to values from its left input.
pub(super) fn normalize_patterns(query: &mut Query) {
    fn defer(
        variable: Option<&Variable>,
        props: &mut Option<Properties>,
        predicate: &mut Option<Expr>,
    ) {
        let (Some(variable), Some(Properties::Map(fields))) = (variable, props.as_mut()) else {
            return;
        };
        fields.retain(|(key, value)| {
            if literal(value).is_ok() {
                return true;
            }
            let span = value.span;
            let field = Expr::new(
                ExprKind::Property {
                    base: Box::new(Expr::new(ExprKind::Variable(variable.clone()), span)),
                    key: key.clone(),
                },
                span,
            );
            let comparison = Expr::new(
                ExprKind::Comparison {
                    first: Box::new(field),
                    rest: vec![(ComparisonOp::Eq, value.clone())],
                },
                span,
            );
            and_where(predicate, comparison);
            false
        });
        if fields.is_empty() {
            *props = None;
        }
    }

    let branches = std::iter::once(&mut query.initial)
        .chain(query.unions.iter_mut().map(|union| &mut union.query));
    for branch in branches {
        for index in 1..branch.clauses.len() {
            let (before, after) = branch.clauses.split_at_mut(index);
            let (Some(Clause::Match(first)), Clause::Match(next)) = (before.last(), &mut after[0])
            else {
                continue;
            };
            let [seed] = first.patterns.as_slice() else {
                continue;
            };
            let Some(variable) = &seed.element.start.variable else {
                continue;
            };
            if first.optional
                || next.optional
                || !seed.element.chain.is_empty()
                || !next.patterns.iter().any(|part| {
                    part.element.start.variable.as_ref() == Some(variable)
                        && part
                            .element
                            .chain
                            .first()
                            .is_some_and(|(edge, _)| edge.range.is_some())
                })
            {
                continue;
            }
            if let Some(filter) = first
                .where_clause
                .as_ref()
                .and_then(|expr| anchor_filter(expr, variable))
            {
                and_where(&mut next.where_clause, filter);
            }
        }
        for clause in &mut branch.clauses {
            match clause {
                Clause::Match(pattern) => {
                    for part in &mut pattern.patterns {
                        let start = &mut part.element.start;
                        defer(
                            start.variable.as_ref(),
                            &mut start.properties,
                            &mut pattern.where_clause,
                        );
                        for (edge, node) in &mut part.element.chain {
                            if edge.range.is_none() {
                                defer(
                                    edge.variable.as_ref(),
                                    &mut edge.properties,
                                    &mut pattern.where_clause,
                                );
                            }
                            defer(
                                node.variable.as_ref(),
                                &mut node.properties,
                                &mut pattern.where_clause,
                            );
                        }
                    }
                }
                Clause::Call(call) => {
                    if let CallKind::Subquery(query) = &mut call.kind {
                        normalize_patterns(query);
                    }
                }
                _ => {}
            }
        }
    }
}

fn and_where(predicate: &mut Option<Expr>, filter: Expr) {
    let span = filter.span;
    *predicate = Some(match predicate.take() {
        Some(previous) => Expr::new(
            ExprKind::Binary {
                op: BinaryOp::And,
                lhs: Box::new(previous),
                rhs: Box::new(filter),
            },
            span,
        ),
        None => filter,
    });
}

// The previous mandatory node match already enforced these constraints on
// the same business identity. Copy only local constants into the expansion.
fn anchor_filter(expr: &Expr, variable: &Variable) -> Option<Expr> {
    let property = |expr: &Expr| {
        matches!(&expr.kind, ExprKind::Property { base, .. }
        if matches!(&base.kind, ExprKind::Variable(name) if name == variable))
    };
    match &expr.kind {
        ExprKind::Binary {
            op: BinaryOp::And,
            lhs,
            rhs,
        } => {
            let mut filter = anchor_filter(lhs, variable);
            if let Some(right) = anchor_filter(rhs, variable) {
                and_where(&mut filter, right);
            }
            filter
        }
        ExprKind::Binary {
            op: BinaryOp::In,
            lhs,
            rhs,
        } if property(lhs)
            && matches!(&rhs.kind, ExprKind::List(items) if items.iter().all(|item| literal(item).is_ok())) =>
        {
            Some(expr.clone())
        }
        ExprKind::Comparison { first, rest }
            if property(first)
                && matches!(rest.as_slice(), [(ComparisonOp::Eq, rhs)] if literal(rhs).is_ok()) =>
        {
            Some(expr.clone())
        }
        _ => None,
    }
}

pub(super) fn bind_pattern(
    catalog: &Catalog,
    pattern: &opencypher::Match,
    builder: &mut MatchPlan,
    prior: &clauses::Scope,
    shortest: &mut BTreeMap<usize, paths::Shortest>,
    expansion_base: usize,
    match_types: MatchTypes,
) -> Result<()> {
    ensure!(!pattern.patterns.is_empty(), "MATCH requires a pattern");
    for part in &pattern.patterns {
        let mut lengths = Vec::new();
        let mut segments = Vec::new();
        let path = &part.element;
        let mut mixed_nodes = false;
        let track_vertices = part.variable.is_some();
        let mixed_edges = path_edge_types_differ(path);
        let mut edge_labels = BTreeSet::new();
        let shortest = shortest.remove(&path.span.start);
        ensure!(
            shortest.is_none() || path.chain.len() == 1,
            "shortest path requires one relationship pattern"
        );
        let first_type = node_label(
            catalog,
            &path.start,
            path.chain.first().map(|(rel, _)| (rel, true)),
            if let Some((_, next)) = path.chain.first().filter(|(rel, _)| only_zero(rel)) {
                known_node_label(next, prior, builder)?
            } else {
                None
            },
            prior,
            builder,
        )?
        .to_owned();
        let mut left = builder.node(catalog, &path.start, &first_type)?;
        let first_alias = left.clone();
        let mut node_labels = BTreeSet::from([first_type.clone()]);
        let mut left_type = first_type.clone();
        for (relationship, node) in &path.chain {
            let edge_type = label(relationship.types.as_ref())?;
            edge_labels.insert(edge_type.to_owned());
            let edge = catalog
                .edges
                .get(edge_type)
                .with_context(|| format!("unknown edge type {edge_type}"))?;
            let right_type = node_label(
                catalog,
                node,
                Some((relationship, false)),
                only_zero(relationship).then_some(left_type.as_str()),
                prior,
                builder,
            )?
            .to_owned();
            let directed = match relationship.direction {
                Direction::Outgoing => Some((&edge.from, &edge.to)),
                Direction::Incoming => Some((&edge.to, &edge.from)),
                Direction::Undirected => None,
                Direction::Both => bail!("relationship arrows at both ends are unsupported"),
            };
            let (from, to) = directed.unwrap_or_else(|| {
                if edge.from.node == left_type {
                    (&edge.from, &edge.to)
                } else {
                    (&edge.to, &edge.from)
                }
            });
            let bounds = if shortest.is_some() {
                Some(paths::bounds(relationship.range)?)
            } else {
                relationship.range.map(bounded_range).transpose()?
            };
            let heterogeneous = edge.from.node != edge.to.node;
            let connects = if heterogeneous && directed.is_none() && bounds.is_some() {
                [&edge.from.node, &edge.to.node].contains(&&left_type)
                    && [&edge.from.node, &edge.to.node].contains(&&right_type)
            } else {
                left_type == from.node && right_type == to.node
            };
            let zero_only = !connects && bounds.is_some_and(|(min, _)| min == 0);
            ensure!(
                connects || zero_only || (match_types != MatchTypes::Explicit && bounds.is_none()),
                "edge {edge_type} cannot connect {left_type} to {right_type} in this direction"
            );
            node_labels.insert(right_type.clone());
            let right = builder.node(catalog, node, &right_type)?;
            if zero_only {
                // No edge is traversed. Only the two node patterns constrain the match.
                property_terms(
                    "src",
                    &columns([&edge.key, &edge.properties]),
                    relationship.properties.as_ref(),
                )?;
                let keys = &catalog.nodes[&left_type].key;
                builder.predicates.push(if left_type == right_type {
                    equal_keys(&left, keys, &right, keys)
                } else {
                    "FALSE".into()
                });
                if let Some(variable) = &relationship.variable {
                    builder.bind_list(&variable.0, "make_array()".into(), "0".into(), edge_type)?;
                }
                if track_vertices {
                    segments.push(Segment {
                        vertices: format!("make_array({})", edge_record(keys, &left)),
                        tagged_vertices: format!(
                            "make_array({})",
                            tagged_record(&left_type, keys.iter().map(|key| qualified(&left, key)))
                        ),
                        edges: None,
                    });
                }
                lengths.push("0".into());
                left = right;
                left_type = right_type;
                continue;
            }
            if let Some((min, max)) = bounds
                && (!heterogeneous
                    || directed.is_none()
                    || shortest.is_some()
                    || builder.partitioned_csr)
            {
                let rank = ranking(pattern, part, relationship, shortest, min, max)?;
                let incoming = relationship.direction == Direction::Incoming;
                let filters = property_terms(
                    "src",
                    &columns([&edge.key, &edge.properties]),
                    relationship.properties.as_ref(),
                )?;
                let left_node = if lengths.is_empty() {
                    &path.start
                } else {
                    &path.chain[lengths.len() - 1].1
                };
                let node_mapping = &catalog.nodes[&left_type];
                let allowed = columns([&node_mapping.key, &node_mapping.properties]);
                let mut starts =
                    property_terms_with("src", &allowed, left_node.properties.as_ref(), |value| {
                        Ok(literal(value).ok())
                    })?;
                let mut bound_keys = match &left_node.properties {
                    Some(Properties::Map(properties)) => properties
                        .iter()
                        .filter(|(_, value)| literal(value).is_ok())
                        .map(|(key, _)| key.0.clone())
                        .collect(),
                    _ => BTreeSet::new(),
                };
                if let Some(variable) = &left_node.variable
                    && let Some(where_clause) = &pattern.where_clause
                {
                    start_terms(
                        where_clause,
                        &variable.0,
                        &allowed,
                        &mut starts,
                        &mut bound_keys,
                    );
                }
                let anchored = node_mapping.key.iter().all(|key| bound_keys.contains(key));
                #[cfg(feature = "admin")]
                let finishes = {
                    let mapped = &catalog.nodes[&right_type];
                    let fields = columns([&mapped.key, &mapped.properties]);
                    let mut terms =
                        property_terms_with("src", &fields, node.properties.as_ref(), |value| {
                            Ok(literal(value).ok())
                        })?;
                    if let (Some(variable), Some(predicate)) =
                        (&node.variable, &pattern.where_clause)
                    {
                        start_terms(
                            predicate,
                            &variable.0,
                            &fields,
                            &mut terms,
                            &mut BTreeSet::new(),
                        );
                    }
                    BTreeMap::from([(right_type.clone(), terms)])
                };
                let table = format!(
                    "__graphlake_expand_{}",
                    expansion_base + builder.expansions.len()
                );
                let alias = builder.add_relation(&table);
                builder.expansions.push(Expansion {
                    table,
                    edge: edge_type.to_owned(),
                    mixed: None,
                    direction: if directed.is_none() {
                        crate::Direction::Undirected
                    } else if incoming {
                        crate::Direction::Incoming
                    } else {
                        crate::Direction::Outgoing
                    },
                    shortest: if rank.is_some() { None } else { shortest },
                    start_node: left_type.clone(),
                    track_vertices,
                    min,
                    max,
                    filters,
                    starts,
                    anchored,
                    #[cfg(feature = "admin")]
                    finishes,
                });
                builder.edge_uniqueness(edge_type, edge, &alias, true);
                for (prefix, node_alias, label) in
                    [("from_", &left, &left_type), ("to_", &right, &right_type)]
                {
                    let key = &catalog.nodes[label].key;
                    let offset = if heterogeneous {
                        let reverse = label == &edge.to.node;
                        builder.predicates.push(format!(
                            "{} = {}",
                            qualified(&alias, &format!("{prefix}0")),
                            u8::from(reverse)
                        ));
                        1 + if reverse {
                            catalog.nodes[&edge.from.node].key.len()
                        } else {
                            0
                        }
                    } else {
                        0
                    };
                    let endpoint = (offset..offset + key.len())
                        .map(|i| format!("{prefix}{i}"))
                        .collect::<Vec<_>>();
                    builder
                        .predicates
                        .push(equal_keys(node_alias, key, &alias, &endpoint));
                }
                if let Some(variable) = &relationship.variable {
                    builder.bind_list(
                        &variable.0,
                        qualified(&alias, PATH),
                        qualified(&alias, HOPS),
                        edge_type,
                    )?;
                }
                if let Some(consume_predicate) = rank {
                    set_ranking(
                        builder,
                        catalog,
                        [(&left, &left_type), (&right, &right_type)],
                        qualified(&alias, HOPS),
                        shortest,
                        consume_predicate,
                    );
                }
                lengths.push(qualified(&alias, HOPS));
                if track_vertices {
                    mixed_nodes |= heterogeneous;
                    if heterogeneous {
                        node_labels.extend([edge.from.node.clone(), edge.to.node.clone()]);
                    }
                    let vertices = qualified(&alias, PATH_NODES);
                    let tagged_vertices = if heterogeneous {
                        let width = catalog.nodes[&edge.from.node].key.len();
                        let encode = |label: &str, offset| {
                            tagged_record(
                                label,
                                (offset..offset + catalog.nodes[label].key.len())
                                    .map(|index| format!("get_field(item, 'c{index}')")),
                            )
                        };
                        format!(
                            "array_transform({vertices}, item -> CASE WHEN get_field(item, 'c0') = 0 THEN {} ELSE {} END)",
                            encode(&edge.from.node, 1),
                            encode(&edge.to.node, width + 1)
                        )
                    } else {
                        tagged_list(&vertices, &left_type, &catalog.nodes[&left_type].key)
                    };
                    let edges = qualified(&alias, PATH);
                    let edges = if mixed_edges {
                        tagged_list(&edges, edge_type, &edge.identity())
                    } else {
                        edges
                    };
                    segments.push(Segment {
                        vertices,
                        tagged_vertices,
                        edges: Some(edges),
                    });
                }
            } else {
                let variable = relationship.variable.as_ref().map(|v| v.0.as_str());
                let alias = builder.edge(edge_type, edge, variable.filter(|_| bounds.is_none()))?;
                if let Some((min, max)) = bounds {
                    // A directed edge between distinct types can be followed only once.
                    if !(min..=max).contains(&1) {
                        builder.predicates.push("FALSE".into());
                    }
                    if let Some(variable) = variable {
                        let record = edge_record(&edge.identity(), &alias);
                        builder.bind_list(
                            variable,
                            format!("make_array({record})"),
                            "1".into(),
                            edge_type,
                        )?;
                    }
                }
                let properties = columns([&edge.key, &edge.properties]);
                if match_types != MatchTypes::Explicit
                    && let Some(Properties::Map(filters)) = &relationship.properties
                    && filters
                        .iter()
                        .any(|(name, _)| !properties.contains(&name.0))
                {
                    builder.predicates.push("FALSE".into());
                } else {
                    let terms = property_terms_with(
                        &alias,
                        &properties,
                        relationship.properties.as_ref(),
                        |value| builder.expr(value).map(Some),
                    )?;
                    builder.predicates.extend(terms);
                }
                let keys = |from: &crate::Endpoint, to: &crate::Endpoint| {
                    format!(
                        "({} AND {})",
                        equal_keys(&left, &catalog.nodes[&left_type].key, &alias, &from.columns),
                        equal_keys(&right, &catalog.nodes[&right_type].key, &alias, &to.columns)
                    )
                };
                let forward = keys(from, to);
                if !connects {
                    builder.predicates.push("FALSE".into());
                } else if directed.is_none() && left_type == right_type {
                    let (source, target) = undirected_keys(builder, &alias, from, to);
                    builder.predicates.push(equal_keys(
                        &left,
                        &catalog.nodes[&left_type].key,
                        &alias,
                        &source,
                    ));
                    builder.predicates.push(equal_keys(
                        &right,
                        &catalog.nodes[&right_type].key,
                        &alias,
                        &target,
                    ));
                } else {
                    builder.predicates.push(forward);
                }
                if track_vertices {
                    mixed_nodes |= left_type != right_type;
                    let typed =
                        |label: &str, alias: &str| edge_record(&catalog.nodes[label].key, alias);
                    let tagged = |label: &str, alias: &str| {
                        tagged_record(
                            label,
                            catalog.nodes[label]
                                .key
                                .iter()
                                .map(|key| qualified(alias, key)),
                        )
                    };
                    let vertices = format!(
                        "make_array({}, {})",
                        typed(&left_type, &left),
                        typed(&right_type, &right)
                    );
                    let tagged_vertices = format!(
                        "make_array({}, {})",
                        tagged(&left_type, &left),
                        tagged(&right_type, &right)
                    );
                    let identity = edge.identity();
                    let edge = if mixed_edges {
                        tagged_record(edge_type, identity.iter().map(|key| qualified(&alias, key)))
                    } else {
                        edge_record(&identity, &alias)
                    };
                    segments.push(Segment {
                        vertices,
                        tagged_vertices,
                        edges: Some(format!("make_array({edge})")),
                    });
                }
                lengths.push("1".into());
            }
            builder.labels.insert(edge_type.to_owned());
            left = right;
            left_type = right_type;
        }
        if let Some(variable) = &part.variable {
            builder.bind_length(
                &variable.0,
                if lengths.is_empty() {
                    "0".into()
                } else {
                    lengths.join(" + ")
                },
            )?;
            let mut node_lists = Vec::new();
            let mut edge_lists = Vec::new();
            for segment in segments {
                let nodes = if mixed_nodes {
                    segment.tagged_vertices
                } else {
                    segment.vertices
                };
                node_lists.push(if node_lists.is_empty() {
                    nodes
                } else {
                    format!("array_pop_front({nodes})")
                });
                edge_lists.extend(segment.edges);
            }
            let nodes = if node_lists.is_empty() {
                format!(
                    "make_array({})",
                    edge_record(&catalog.nodes[&first_type].key, &first_alias)
                )
            } else {
                concatenate(node_lists)
            };
            builder
                .paths
                .insert(variable.0.clone(), concatenate(edge_lists));
            builder.path_nodes.insert(variable.0.clone(), nodes);
            builder.path_types.insert(
                variable.0.clone(),
                PathTypes {
                    nodes: IdentityList {
                        labels: node_labels,
                        tagged: mixed_nodes,
                        node: true,
                    },
                    edges: IdentityList {
                        labels: edge_labels,
                        tagged: mixed_edges,
                        node: false,
                    },
                },
            );
        }
    }
    Ok(())
}

fn undirected_keys(
    builder: &mut MatchPlan<'_>,
    alias: &str,
    from: &crate::Endpoint,
    to: &crate::Endpoint,
) -> (Vec<String>, Vec<String>) {
    let generated = |side: &str| {
        (0..from.columns.len())
            .map(|index| format!("__graphlake_oriented_{side}_{index}"))
            .collect::<Vec<_>>()
    };
    let left = generated("from");
    let right = generated("to");
    let (relation, _) = builder.relations.last_mut().expect("edge relation");
    let select = |source: &crate::Endpoint, target: &crate::Endpoint| {
        let endpoints = source
            .columns
            .iter()
            .zip(&left)
            .chain(target.columns.iter().zip(&right))
            .map(|(column, name)| format!("{} AS {}", qualified(alias, column), ident(name)))
            .collect::<Vec<_>>();
        format!(
            "SELECT {}.*, {} FROM {relation}",
            ident(alias),
            endpoints.join(", ")
        )
    };
    let forward = select(from, to);
    let reverse = select(to, from);
    let same = equal_keys(alias, &from.columns, alias, &to.columns);
    // A self-loop contributes one orientation. Parallel edge IDs stay intact.
    *relation = format!(
        "({forward} UNION ALL {reverse} WHERE NOT ({same})) AS {}",
        ident(alias)
    );
    (left, right)
}

struct Segment {
    vertices: String,
    tagged_vertices: String,
    edges: Option<String>,
}

pub(super) fn ranking(
    pattern: &opencypher::Match,
    part: &opencypher::PatternPart,
    relationship: &opencypher::RelationshipPattern,
    shortest: Option<paths::Shortest>,
    min: usize,
    max: usize,
) -> Result<Option<bool>> {
    if shortest.is_none() {
        return Ok(None);
    }
    let dependent = pattern.where_clause.as_ref().is_some_and(|expr| {
        let names = [part.variable.as_ref(), relationship.variable.as_ref()];
        let mut finder = FindVariable {
            names,
            found: false,
        };
        finder.visit_expr(expr);
        finder.found
    });
    if min <= 1 && !dependent {
        return Ok(None);
    }
    ensure!(
        pattern.patterns.len() == 1 && max != usize::MAX,
        "path-dependent shortest path requires a single bounded pattern"
    );
    Ok(Some(dependent))
}

fn set_ranking(
    builder: &mut MatchPlan,
    catalog: &Catalog,
    endpoints: [(&str, &str); 2],
    hops: String,
    shortest: Option<paths::Shortest>,
    consume_predicate: bool,
) {
    let endpoints = endpoints
        .into_iter()
        .flat_map(|(alias, label)| {
            catalog.nodes[label]
                .key
                .iter()
                .map(|key| qualified(alias, key))
        })
        .collect();
    builder.shortest_ranking = Some(ShortestRanking {
        endpoints,
        hops,
        all: matches!(shortest, Some(paths::Shortest::All)),
        consume_predicate,
    });
}

fn path_edge_types_differ(path: &opencypher::PatternElement) -> bool {
    path.chain
        .first()
        .is_some_and(|(first, _)| path.chain.iter().any(|(rel, _)| rel.types != first.types))
}

pub(super) fn concatenate(mut lists: Vec<String>) -> String {
    match lists.len() {
        0 => "make_array()".into(),
        1 => lists.pop().expect("one list"),
        _ => format!("array_concat({})", lists.join(", ")),
    }
}

fn tagged_record(label: &str, values: impl Iterator<Item = String>) -> String {
    let keys = values
        .map(|value| format!("CAST({value} AS VARCHAR)"))
        .collect::<Vec<_>>();
    format!(
        "named_struct('label', {}, 'key', make_array({}))",
        string(label),
        keys.join(", ")
    )
}

fn tagged_list(list: &str, label: &str, fields: &[String]) -> String {
    let item = tagged_record(
        label,
        fields
            .iter()
            .map(|field| format!("get_field(item, {})", string(field))),
    );
    format!("array_transform({list}, item -> {item})")
}

fn node_label<'a>(
    catalog: &'a Catalog,
    node: &'a NodePattern,
    relationship: Option<(&'a opencypher::RelationshipPattern, bool)>,
    zero_label: Option<&'a str>,
    prior: &'a clauses::Scope,
    builder: &'a MatchPlan,
) -> Result<&'a str> {
    if let Some(label) = known_node_label(node, prior, builder)?.or(zero_label) {
        return Ok(label);
    }
    if let Some((relationship, left)) = relationship {
        let edge_type = label(relationship.types.as_ref())?;
        let edge = catalog
            .edges
            .get(edge_type)
            .with_context(|| format!("unknown edge type {edge_type}"))?;
        ensure!(
            !only_zero(relationship) || edge.from.node == edge.to.node,
            "zero-hop node label is ambiguous"
        );
        return match relationship.direction {
            Direction::Outgoing | Direction::Incoming => {
                let from = left == (relationship.direction == Direction::Outgoing);
                Ok(if from { &edge.from.node } else { &edge.to.node })
            }
            Direction::Undirected if edge.from.node == edge.to.node => Ok(&edge.from.node),
            _ => bail!("ambiguous node label in undirected pattern"),
        };
    }
    if catalog.nodes.len() == 1 {
        return Ok(catalog.nodes.keys().next().expect("one label"));
    }
    bail!("node label is ambiguous")
}

fn known_node_label<'a>(
    node: &'a NodePattern,
    prior: &'a clauses::Scope,
    builder: &'a MatchPlan,
) -> Result<Option<&'a str>> {
    if node.labels.is_some() {
        return label(node.labels.as_ref()).map(Some);
    }
    if let Some(variable) = &node.variable {
        if let Some(binding) = builder.bindings.get(&variable.0) {
            return Ok(Some(&binding.label));
        }
        if let Some(clauses::Value::Entity(binding)) = prior.columns.get(&variable.0)
            && !binding.dynamic
        {
            return Ok(Some(&binding.label));
        }
    }
    Ok(None)
}

fn only_zero(rel: &opencypher::RelationshipPattern) -> bool {
    rel.range
        .is_some_and(|range| range.min == Some(0) && (!range.has_range || range.max == Some(0)))
}

pub(super) fn bounded_range(range: opencypher::RangeLiteral) -> Result<(usize, usize)> {
    let min = range.min.unwrap_or(1);
    let max = if range.has_range {
        range.max
    } else {
        range.min
    }
    .context("variable relationships require an explicit upper bound")?;
    ensure!(
        (0..=256).contains(&max) && (0..=max).contains(&min),
        "invalid bounded relationship range"
    );
    Ok((min as usize, max as usize))
}

pub(super) fn edge_record(identity: &[String], alias: &str) -> String {
    let args = identity
        .iter()
        .flat_map(|name| [string(name), qualified(alias, name)])
        .collect::<Vec<_>>()
        .join(", ");
    format!("named_struct({args})")
}

fn label(label: Option<&LabelExpr>) -> Result<&str> {
    match label {
        Some(LabelExpr::Leaf(name)) => Ok(name),
        _ => bail!("each pattern element requires exactly one explicit label or relationship type"),
    }
}

struct FindVariable<'a> {
    names: [Option<&'a opencypher::Variable>; 2],
    found: bool,
}

impl Visitor for FindVariable<'_> {
    fn visit_expr(&mut self, expr: &Expr) {
        if let ExprKind::Variable(variable) = &expr.kind {
            self.found |= self.names.iter().flatten().any(|name| name.0 == variable.0);
        } else if !self.found {
            walk_expr(self, expr);
        }
    }
}
