use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Result, bail, ensure};
use opencypher::{Direction, Expr, Match, PatternPart, Properties, RelationshipPattern};

use crate::{
    Catalog,
    catalog::{columns, equal_keys, ident, qualified, union_all},
    traversal::{Expansion, HOPS, MixedExpansion, PATH, PATH_NODES, text_key},
};

use super::{
    Binding, MatchPlan, ShortestRanking,
    alternatives::{self, Domains, Node},
    entities::{self, IdentityList, PathTypes},
    paths,
    pattern::{bounded_range, concatenate, ranking},
    property_terms, start_terms, string,
};

pub(super) fn bind<'a>(
    catalog: &'a Catalog,
    pattern: &Match,
    domains: &Domains<'_>,
    shortest: &mut BTreeMap<usize, paths::Shortest>,
    base: usize,
) -> Result<MatchPlan<'a>> {
    let mut plan = MatchPlan::new(catalog);
    let aliases = domains
        .nodes
        .iter()
        .map(|node| bind_node(catalog, &mut plan, node))
        .collect::<Result<Vec<_>>>()?;
    if domains.impossible {
        plan.predicates.push("FALSE".into());
    }
    for (part, path) in pattern.patterns.iter().zip(&domains.paths) {
        let mode = shortest.remove(&part.element.span.start);
        ensure!(
            mode.is_none() || part.element.chain.len() == 1,
            "shortest path requires one relationship pattern"
        );
        let mut hops = Vec::new();
        let mut vertices = Vec::new();
        let mut edges = Vec::new();
        let mut labels = BTreeSet::new();
        for (index, (relationship, _)) in part.element.chain.iter().enumerate() {
            let endpoints = [path[index], path[index + 1]]
                .map(|index| (&domains.nodes[index], aliases[index].as_str()));
            let step = if mode.is_some() || relationship.range.is_some() {
                let bounds = match mode {
                    Some(_) => paths::bounds(relationship.range)?,
                    None => bounded_range(relationship.range.expect("range"))?,
                };
                let rank = ranking(pattern, part, relationship, mode, bounds.0, bounds.1)?;
                let table = format!("__graphlake_expand_{}", base + plan.expansions.len());
                let step = ranged(
                    catalog,
                    &mut plan,
                    relationship,
                    endpoints[0].0,
                    table,
                    Range {
                        bounds,
                        mode: if rank.is_some() { None } else { mode },
                        track: part.variable.is_some(),
                        predicate: pattern.where_clause.as_ref(),
                        #[cfg(feature = "admin")]
                        finish: endpoints[1].0,
                    },
                )?;
                if let Some(consume_predicate) = rank {
                    plan.shortest_ranking = Some(ShortestRanking {
                        endpoints: endpoints
                            .iter()
                            .flat_map(|(_, alias)| {
                                [qualified(alias, "label"), qualified(alias, "key")]
                            })
                            .collect(),
                        hops: step.hops.clone(),
                        all: matches!(mode, Some(paths::Shortest::All)),
                        consume_predicate,
                    });
                }
                step
            } else {
                fixed(catalog, &mut plan, relationship, endpoints)?
            };
            for (index, (_, node)) in endpoints.iter().enumerate() {
                let [label, key] = &step.endpoints[index];
                plan.predicates.push(format!(
                    "{label} = {} AND {key} = {}",
                    qualified(node, "label"),
                    qualified(node, "key")
                ));
            }
            if part.variable.is_some() {
                vertices.push(if vertices.is_empty() {
                    step.vertices
                } else {
                    format!("array_pop_front({})", step.vertices)
                });
            }
            hops.push(step.hops);
            edges.push(step.edges);
            labels.extend(step.labels);
        }
        if part.variable.is_some() {
            let mut node_labels = path
                .iter()
                .flat_map(|index| domains.nodes[*index].labels.iter().cloned())
                .collect::<BTreeSet<_>>();
            for label in &labels {
                let edge = &catalog.edges[label];
                node_labels.extend([edge.from.node.clone(), edge.to.node.clone()]);
            }
            finish_path(
                &mut plan,
                part,
                &aliases[path[0]],
                hops,
                vertices,
                edges,
                PathTypes {
                    nodes: IdentityList {
                        labels: node_labels,
                        tagged: true,
                        node: true,
                    },
                    edges: IdentityList {
                        labels,
                        tagged: true,
                        node: false,
                    },
                },
            )?;
        }
    }
    Ok(plan)
}

fn finish_path(
    plan: &mut MatchPlan,
    part: &PatternPart,
    start: &str,
    hops: Vec<String>,
    mut vertices: Vec<String>,
    edges: Vec<String>,
    shape: PathTypes,
) -> Result<()> {
    let name = &part.variable.as_ref().expect("path variable").0;
    plan.bind_length(
        name,
        if hops.is_empty() {
            "0".into()
        } else {
            hops.join(" + ")
        },
    )?;
    if vertices.is_empty() {
        vertices.push(format!(
            "make_array({})",
            tagged(&qualified(start, "label"), &qualified(start, "key"))
        ));
    }
    plan.paths.insert(name.clone(), concatenate(edges));
    plan.path_nodes.insert(name.clone(), concatenate(vertices));
    plan.path_types.insert(name.clone(), shape);
    Ok(())
}

fn bind_node(catalog: &Catalog, plan: &mut MatchPlan, node: &Node<'_>) -> Result<String> {
    let options = node
        .labels
        .iter()
        .map(|label| {
            let mapping = &catalog.nodes[label];
            Binding {
                alias: "src".into(),
                label: label.clone(),
                keys: mapping.key.clone(),
                properties: columns([&mapping.key, &mapping.properties]),
                stored: None,
                node: true,
                dynamic: false,
                lanes: Default::default(),
            }
        })
        .collect::<Vec<_>>();
    let mut binding = entities::merged(&options);
    let mut reads = Vec::new();
    for option in &options {
        let terms = node_filters(node, option, options.len() > 1)?;
        let key = text_key("src", &option.keys);
        let payload = entities::shape(option, &binding, &option.value());
        reads.push(format!(
            "SELECT {} AS label, {key} AS key, {payload} AS payload FROM {} AS src{}",
            string(&option.label),
            ident(&option.label),
            where_sql(&terms)
        ));
    }
    plan.labels.extend(node.labels.iter().cloned());
    let alias = plan.add_subquery(&union_all(&reads));
    if let Some(name) = &node.patterns[0].variable {
        binding.stored = Some(qualified(&alias, "payload"));
        plan.bindings.insert(name.0.clone(), binding);
    }
    Ok(alias)
}

fn node_filters(node: &Node<'_>, binding: &Binding, polymorphic: bool) -> Result<Vec<String>> {
    let mut terms = Vec::new();
    for pattern in &node.patterns {
        terms.extend(filters(
            &binding.properties,
            pattern.properties.as_ref(),
            polymorphic,
        )?);
    }
    Ok(terms)
}

fn filters(
    allowed: &[String],
    props: Option<&Properties>,
    optional_fields: bool,
) -> Result<Vec<String>> {
    if optional_fields
        && let Some(Properties::Map(fields)) = props
        && fields.iter().any(|(name, _)| !allowed.contains(&name.0))
    {
        Ok(vec!["FALSE".into()])
    } else {
        property_terms("src", allowed, props)
    }
}

struct Step {
    endpoints: [[String; 2]; 2],
    hops: String,
    vertices: String,
    edges: String,
    labels: BTreeSet<String>,
}

fn fixed(
    catalog: &Catalog,
    plan: &mut MatchPlan,
    relationship: &RelationshipPattern,
    endpoints: [(&Node<'_>, &str); 2],
) -> Result<Step> {
    let mut choices = alternatives::orientations(
        catalog,
        relationship,
        &endpoints[0].0.labels,
        &endpoints[1].0.labels,
    )?;
    let empty = choices.is_empty();
    let labels = alternatives::edge_labels(catalog, relationship)?;
    if empty {
        choices.extend(labels.into_iter().map(|name| (name, false)));
    }
    let labels = choices
        .iter()
        .map(|(name, _)| name.clone())
        .collect::<BTreeSet<_>>();
    let options = labels
        .iter()
        .map(|name| {
            let mut binding = entities::edge_binding(catalog, name);
            binding.alias = "src".into();
            binding
        })
        .collect::<Vec<_>>();
    let mut result = if options.is_empty() {
        Binding {
            alias: String::new(),
            label: String::new(),
            properties: Vec::new(),
            keys: Vec::new(),
            stored: None,
            node: false,
            dynamic: true,
            lanes: Default::default(),
        }
    } else {
        entities::merged(&options)
    };
    let mut reads = Vec::new();
    for (name, swap) in choices {
        let edge = &catalog.edges[&name];
        let (from, to) = if swap {
            (&edge.to, &edge.from)
        } else {
            (&edge.from, &edge.to)
        };
        let from_key = &catalog.nodes[&from.node].key;
        let to_key = &catalog.nodes[&to.node].key;
        let binding = options
            .iter()
            .find(|option| option.label == name)
            .expect("edge binding");
        let payload = entities::shape(binding, &result, &binding.value());
        let mut terms = filters(
            &columns([&edge.key, &edge.properties]),
            relationship.properties.as_ref(),
            !matches!(relationship.types, Some(opencypher::LabelExpr::Leaf(_))),
        )?;
        if empty {
            terms.push("FALSE".into());
        }
        if swap && relationship.direction == Direction::Undirected && from.node == to.node {
            terms.push(format!("NOT ({})", equal_keys("a", from_key, "b", to_key)));
        }
        plan.labels
            .extend([name.clone(), from.node.clone(), to.node.clone()]);
        reads.push(format!(
            "SELECT {} AS from_label, {} AS from_key, {} AS to_label, {} AS to_key, {} AS label, {} AS key, {payload} AS payload
             FROM {} AS src JOIN {} AS a ON {} JOIN {} AS b ON {}{}",
            string(&from.node), text_key("a", from_key), string(&to.node), text_key("b", to_key),
            string(&name), text_key("src", &binding.keys), ident(&name), ident(&from.node),
            equal_keys("a", from_key, "src", &from.columns), ident(&to.node),
            equal_keys("b", to_key, "src", &to.columns), where_sql(&terms)
        ));
    }
    if reads.is_empty() {
        reads.push("SELECT '' AS from_label, make_array('') AS from_key, '' AS to_label, make_array('') AS to_key,
                    '' AS label, make_array('') AS key,
                    __graphlake_cypher_entity(named_struct('kind','relationship','label','','key',make_array(''),'lanes',named_struct('_empty',named_struct('_empty',NULL)))) AS payload WHERE FALSE".into());
    }
    let alias = plan.add_subquery(&union_all(&reads));
    if let Some(variable) = &relationship.variable {
        ensure!(
            !plan.bindings.contains_key(&variable.0) && !plan.lengths.contains_key(&variable.0),
            "repeated relationship binding {}",
            variable.0
        );
        result.stored = Some(qualified(&alias, "payload"));
        plan.bindings.insert(variable.0.clone(), result);
    }
    let ends = ["from", "to"].map(|side| {
        [
            qualified(&alias, &format!("{side}_label")),
            qualified(&alias, &format!("{side}_key")),
        ]
    });
    let vertices = format!(
        "make_array({}, {})",
        tagged(&ends[0][0], &ends[0][1]),
        tagged(&ends[1][0], &ends[1][1])
    );
    let edges = format!(
        "make_array({})",
        tagged(&qualified(&alias, "label"), &qualified(&alias, "key"))
    );
    plan.path_uniqueness(&edges);
    Ok(Step {
        endpoints: ends,
        hops: "1".into(),
        vertices,
        edges,
        labels,
    })
}

struct Range<'a> {
    bounds: (usize, usize),
    mode: Option<paths::Shortest>,
    track: bool,
    predicate: Option<&'a Expr>,
    #[cfg(feature = "admin")]
    finish: &'a Node<'a>,
}

fn ranged(
    catalog: &Catalog,
    plan: &mut MatchPlan,
    relationship: &RelationshipPattern,
    start: &Node<'_>,
    table: String,
    range: Range<'_>,
) -> Result<Step> {
    let direction = match relationship.direction {
        Direction::Outgoing => crate::Direction::Outgoing,
        Direction::Incoming => crate::Direction::Incoming,
        Direction::Undirected => crate::Direction::Undirected,
        Direction::Both => bail!("relationship arrows at both ends are unsupported"),
    };
    let allowed = alternatives::edge_labels(catalog, relationship)?;
    let edges = catalog
        .edges
        .iter()
        .filter(|(name, _)| allowed.contains(*name))
        .map(|(name, edge)| {
            let filters = filters(
                &columns([&edge.key, &edge.properties]),
                relationship.properties.as_ref(),
                !matches!(relationship.types, Some(opencypher::LabelExpr::Leaf(_))),
            )?;
            plan.labels
                .extend([name.clone(), edge.from.node.clone(), edge.to.node.clone()]);
            Ok((name.clone(), filters))
        })
        .collect::<Result<BTreeMap<_, _>>>()?;
    let labels = edges.keys().cloned().collect::<BTreeSet<_>>();
    let endpoints = |node: &Node<'_>| {
        node.labels
            .iter()
            .map(|label| {
                let mapping = &catalog.nodes[label];
                let binding = Binding {
                    alias: "src".into(),
                    label: label.clone(),
                    keys: mapping.key.clone(),
                    properties: columns([&mapping.key, &mapping.properties]),
                    stored: None,
                    node: true,
                    dynamic: false,
                    lanes: Default::default(),
                };
                let mut terms = node_filters(node, &binding, node.labels.len() > 1)?;
                if let (Some(name), Some(predicate)) = (&node.patterns[0].variable, range.predicate)
                {
                    start_terms(
                        predicate,
                        &name.0,
                        &binding.properties,
                        &mut terms,
                        &mut BTreeSet::new(),
                    );
                }
                Ok((label.clone(), terms))
            })
            .collect::<Result<BTreeMap<_, _>>>()
    };
    let starts = endpoints(start)?;
    #[cfg(feature = "admin")]
    let finishes = endpoints(range.finish)?;
    let alias = plan.add_relation(&table);
    plan.expansions.push(Expansion {
        table,
        edge: String::new(),
        mixed: Some(MixedExpansion { edges, starts }),
        direction,
        shortest: range.mode,
        start_node: String::new(),
        track_vertices: range.track,
        min: range.bounds.0,
        max: range.bounds.1,
        filters: Vec::new(),
        starts: Vec::new(),
        anchored: false,
        #[cfg(feature = "admin")]
        finishes,
    });
    let edges = qualified(&alias, PATH);
    let hops = qualified(&alias, HOPS);
    if let Some(variable) = &relationship.variable {
        plan.bind_length(&variable.0, hops.clone())?;
        plan.lists.insert(variable.0.clone(), edges.clone());
        plan.list_types.insert(
            variable.0.clone(),
            IdentityList {
                labels: labels.clone(),
                tagged: true,
                node: false,
            },
        );
    }
    plan.path_uniqueness(&edges);
    let endpoints = ["from", "to"].map(|side| {
        [
            qualified(&alias, &format!("{side}_0")),
            qualified(&alias, &format!("{side}_1")),
        ]
    });
    Ok(Step {
        endpoints,
        hops,
        vertices: qualified(&alias, PATH_NODES),
        edges,
        labels,
    })
}

fn tagged(label: &str, key: &str) -> String {
    format!("named_struct('label', {label}, 'key', {key})")
}

fn where_sql(terms: &[String]) -> String {
    if terms.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", terms.join(" AND "))
    }
}
