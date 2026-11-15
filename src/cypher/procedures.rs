use std::{collections::BTreeSet, sync::Arc};

use anyhow::{Context, Result, bail, ensure};
use datafusion::{
    common::TableReference, execution::memory_pool::MemoryReservation, prelude::SessionContext,
};
use opencypher::{Expr, ExprKind, ProcedureCall, UnaryOp};
use serde_json::{Map, Value};

use crate::{
    Catalog, Direction, GraphProjection, ProjectionAlgorithm, Route, RouteStrategy, ScalarValue,
    VertexKey,
    catalog::ident,
    engine::QueryScope,
    traversal::{projection_table, route_table},
};

use super::{Plan, string};

pub(crate) struct Procedure {
    table: String,
    projection: GraphProjection,
    kind: Algorithm,
}

enum Algorithm {
    Analyze(ProjectionAlgorithm),
    Route(Route, RouteStrategy),
}

pub(crate) struct SchemaProcedure {
    table: String,
    nodes: bool,
}

impl SchemaProcedure {
    pub(crate) fn labels(&self, catalog: &Catalog) -> BTreeSet<String> {
        if self.nodes {
            catalog.nodes.keys().cloned().collect()
        } else {
            catalog.edges.keys().cloned().collect()
        }
    }

    pub(crate) async fn register(
        &self,
        catalog: &Catalog,
        schema_context: &SessionContext,
        context: &SessionContext,
    ) -> Result<()> {
        let mut rows = Vec::new();
        for label in self.labels(catalog) {
            let table = schema_context
                .table_provider(TableReference::bare(label.as_str()))
                .await?;
            let schema = table.schema();
            let properties = if self.nodes {
                &catalog.nodes[&label].properties
            } else {
                &catalog.edges[&label].properties
            };
            for name in properties {
                let field = schema.field_with_name(name)?;
                let label = string(&label);
                let name = string(name);
                let ty = string(&property_type(field.data_type()));
                let mandatory = if field.is_nullable() { "FALSE" } else { "TRUE" };
                rows.push(if self.nodes {
                    format!("({label}, make_array({label}), {name}, make_array({ty}), {mandatory})")
                } else {
                    format!("({label}, {name}, make_array({ty}), {mandatory})")
                });
            }
        }
        let names = if self.nodes {
            "\"nodeType\", \"nodeLabels\", \"propertyName\", \"propertyTypes\", mandatory"
        } else {
            "\"relType\", \"propertyName\", \"propertyTypes\", mandatory"
        };
        let empty = if self.nodes {
            "CAST(NULL AS VARCHAR), make_array(CAST(NULL AS VARCHAR)), CAST(NULL AS VARCHAR), make_array(CAST(NULL AS VARCHAR)), FALSE"
        } else {
            "CAST(NULL AS VARCHAR), CAST(NULL AS VARCHAR), make_array(CAST(NULL AS VARCHAR)), FALSE"
        };
        let sql = if rows.is_empty() {
            format!("SELECT * FROM (VALUES ({empty})) AS fields({names}) WHERE FALSE")
        } else {
            format!(
                "SELECT * FROM (VALUES {}) AS fields({names})",
                rows.join(", ")
            )
        };
        let frame = context.sql(&sql).await?;
        context.register_table(TableReference::bare(self.table.as_str()), frame.into_view())?;
        Ok(())
    }
}

fn property_type(ty: &datafusion::arrow::datatypes::DataType) -> String {
    use datafusion::arrow::datatypes::DataType::*;
    match ty {
        Boolean => "Boolean".into(),
        Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64 => "Integer".into(),
        Float16 | Float32 | Float64 => "Float".into(),
        Decimal32(..) | Decimal64(..) | Decimal128(..) | Decimal256(..) => "Decimal".into(),
        Utf8 | LargeUtf8 | Utf8View => "String".into(),
        Date32 | Date64 => "Date".into(),
        Timestamp(_, None) => "LocalDateTime".into(),
        Timestamp(_, Some(_)) => "DateTime".into(),
        Time32(_) | Time64(_) => "LocalTime".into(),
        Interval(_) | Duration(_) => "Duration".into(),
        List(item) | LargeList(item) | FixedSizeList(item, _) => {
            format!("List<{}>", property_type(item.data_type()))
        }
        _ => ty.to_string(),
    }
}

impl Procedure {
    pub(crate) async fn register(
        &self,
        catalog: &Catalog,
        context: &SessionContext,
        scope: &Arc<QueryScope>,
        explain_only: bool,
    ) -> Result<MemoryReservation> {
        let (table, reservation) = match &self.kind {
            Algorithm::Analyze(algorithm) => {
                projection_table(
                    catalog,
                    &self.projection,
                    *algorithm,
                    context,
                    scope,
                    explain_only,
                )
                .await?
            }
            Algorithm::Route(route, strategy) => {
                route_table(
                    catalog,
                    &self.projection,
                    route,
                    *strategy,
                    context,
                    scope,
                    explain_only,
                )
                .await?
            }
        };
        context.register_table(TableReference::bare(self.table.as_str()), table)?;
        Ok(reservation)
    }
}

pub(super) fn relation(
    catalog: &Catalog,
    call: &ProcedureCall,
    plan: &mut Plan,
) -> Result<(String, Vec<(&'static str, &'static str)>)> {
    let name = call.name.0.to_ascii_lowercase();
    let args = call.args.as_deref().unwrap_or_default();
    let list = match name.as_str() {
        "db.labels" => Some((
            "label",
            catalog.nodes.keys().cloned().collect::<BTreeSet<_>>(),
        )),
        "db.relationshiptypes" => {
            Some(("relationshipType", catalog.edges.keys().cloned().collect()))
        }
        "db.propertykeys" => Some((
            "propertyKey",
            catalog
                .nodes
                .values()
                .flat_map(|node| node.key.iter().chain(&node.properties))
                .chain(
                    catalog
                        .edges
                        .values()
                        .flat_map(|edge| edge.key.iter().chain(&edge.properties)),
                )
                .cloned()
                .collect(),
        )),
        _ => None,
    };
    if let Some((field, values)) = list {
        ensure!(args.is_empty(), "{name} does not take arguments");
        let sql = if values.is_empty() {
            format!(
                "SELECT CAST(NULL AS VARCHAR) AS {} WHERE FALSE",
                ident(field)
            )
        } else {
            let rows = values
                .iter()
                .map(|value| format!("({})", string(value)))
                .collect::<Vec<_>>();
            format!(
                "SELECT {} FROM (VALUES {}) AS metadata({})",
                ident(field),
                rows.join(", "),
                ident(field)
            )
        };
        return Ok((sql, vec![(field, field)]));
    }
    if matches!(name.as_str(), "db.ping" | "db.info") {
        ensure!(args.is_empty(), "{name} does not take arguments");
        return if name == "db.ping" {
            Ok((
                "SELECT TRUE AS success".into(),
                vec![("success", "success")],
            ))
        } else {
            Ok((
                format!("SELECT {} AS name", string(&catalog.graph)),
                vec![("name", "name")],
            ))
        };
    }
    if name == "db.schema.visualization" {
        ensure!(args.is_empty(), "{name} does not take arguments");
        return Ok((
            schema_visualization(catalog),
            vec![("nodes", "nodes"), ("relationships", "relationships")],
        ));
    }
    if matches!(
        name.as_str(),
        "db.schema.nodetypeproperties" | "db.schema.reltypeproperties"
    ) {
        ensure!(args.is_empty(), "{name} does not take arguments");
        let nodes = name == "db.schema.nodetypeproperties";
        let mut table = format!(
            "__graphlake_schema_properties_{}",
            plan.schema_procedures.len()
        );
        while catalog.nodes.contains_key(&table) || catalog.edges.contains_key(&table) {
            table.push('_');
        }
        let sql = format!("SELECT * FROM {}", ident(&table));
        plan.schema_procedures
            .push(SchemaProcedure { table, nodes });
        let mut fields = if nodes {
            vec![("nodeType", "nodeType"), ("nodeLabels", "nodeLabels")]
        } else {
            vec![("relType", "relType"), ("relationshipType", "relType")]
        };
        fields.extend([
            ("propertyName", "propertyName"),
            ("propertyTypes", "propertyTypes"),
            ("mandatory", "mandatory"),
        ]);
        return Ok((sql, fields));
    }
    if name == "dbms.components" || name == "dbms.info" {
        ensure!(args.is_empty(), "{name} does not take arguments");
        return if name == "dbms.info" {
            Ok((
                format!("SELECT {} AS name", string(&catalog.graph)),
                vec![("name", "name")],
            ))
        } else {
            Ok((
                format!(
                    "SELECT 'GraphLake' AS name, make_array({}) AS versions, 'community' AS edition",
                    string(env!("CARGO_PKG_VERSION"))
                ),
                vec![
                    ("name", "name"),
                    ("versions", "versions"),
                    ("edition", "edition"),
                ],
            ))
        };
    }
    ensure!(
        name.starts_with("algo."),
        "unsupported Cypher procedure {}",
        call.name.0
    );
    let mut args = match args {
        [] => Map::new(),
        [arg] => constant(arg)?
            .as_object()
            .cloned()
            .context("algorithm arguments must be one map")?,
        _ => bail!("algorithm arguments must be one map"),
    };
    let mut projection = GraphProjection {
        nodes: take(&mut args, "labels")?.unwrap_or_default(),
        edges: take(&mut args, "relationshipTypes")?.unwrap_or_default(),
        weight: take(&mut args, "relationshipWeightProperty")?,
        direction: match take::<String>(&mut args, "direction")?.as_deref() {
            None | Some("outgoing") => Direction::Outgoing,
            Some("incoming") => Direction::Incoming,
            Some("undirected") => Direction::Undirected,
            _ => bail!("direction must be outgoing, incoming or undirected"),
        },
    };
    if projection.nodes.is_empty() && projection.edges.is_empty() {
        projection.nodes.extend(catalog.nodes.keys().cloned());
        projection.edges.extend(catalog.edges.keys().cloned());
    }
    let (kind, fields) = match name.as_str() {
        "algo.route" | "algo.shortestpath" | "algo.daglongestpath" => {
            let start = vertex(args.remove("start").context("route requires start")?)?;
            let target = vertex(args.remove("target").context("route requires target")?)?;
            let k = take(&mut args, "k")?.unwrap_or(1);
            let mut strategy = take::<String>(&mut args, "strategy")?
                .map(|strategy| strategy.parse())
                .transpose()?
                .unwrap_or_default();
            if name == "algo.daglongestpath" {
                strategy = RouteStrategy::DagLongest;
            }
            (
                Algorithm::Route(Route { start, target, k }, strategy),
                vec![
                    ("rank", "rank"),
                    ("cost", "cost"),
                    ("costExact", "cost_exact"),
                    ("hops", "hops"),
                    ("vertices", "vertices"),
                    ("edges", "edges"),
                ],
            )
        }
        _ => {
            let (algorithm, output, column) = match name.as_str() {
                "algo.pagerank" | "algo.paral.pagerank" => (
                    ProjectionAlgorithm::PageRank {
                        damping: take(&mut args, "damping")?.unwrap_or(0.85),
                        tolerance: take(&mut args, "tolerance")?.unwrap_or(1e-9),
                        max_iterations: take(&mut args, "maxIterations")?.unwrap_or(100),
                    },
                    "rank",
                    "__graphlake_rank",
                ),
                "algo.wcc" | "algo.weakcomponents" => (
                    ProjectionAlgorithm::WeaklyConnectedComponents,
                    "component",
                    "__graphlake_component",
                ),
                "algo.labelpropagation" => (
                    ProjectionAlgorithm::LabelPropagation {
                        max_iterations: take(&mut args, "maxIterations")?.unwrap_or(20),
                    },
                    "communityId",
                    "__graphlake_community",
                ),
                "algo.louvain" | "algo.leiden" => {
                    let defaults = crate::CommunityOptions::default();
                    let options = crate::CommunityOptions {
                        resolution: take(&mut args, "resolution")?.unwrap_or(defaults.resolution),
                        max_iterations: take(&mut args, "maxIterations")?
                            .unwrap_or(defaults.max_iterations),
                        seed: take(&mut args, "seed")?.unwrap_or(defaults.seed),
                    };
                    (
                        if name == "algo.louvain" {
                            ProjectionAlgorithm::Louvain(options)
                        } else {
                            ProjectionAlgorithm::Leiden(options)
                        },
                        "communityId",
                        "__graphlake_community",
                    )
                }
                "algo.node2vec" => {
                    let settings = args
                        .remove("settings")
                        .unwrap_or_else(|| Value::Object(Map::new()));
                    (
                        ProjectionAlgorithm::Node2Vec(serde_json::from_value(settings)?),
                        "embedding",
                        "__graphlake_embedding",
                    )
                }
                _ => bail!("unsupported Cypher procedure {}", call.name.0),
            };
            (
                Algorithm::Analyze(algorithm),
                vec![("id", "vertex"), (output, column)],
            )
        }
    };
    ensure!(
        args.is_empty(),
        "unknown algorithm settings: {}",
        args.keys().cloned().collect::<Vec<_>>().join(", ")
    );
    plan.labels
        .extend(projection.nodes.iter().chain(&projection.edges).cloned());
    let table = format!("__graphlake_procedure_{}", plan.procedures.len());
    ensure!(
        !catalog.nodes.contains_key(&table) && !catalog.edges.contains_key(&table),
        "graph label collides with internal procedure name"
    );
    let sql = format!("SELECT * FROM {}", ident(&table));
    plan.procedures.push(Procedure {
        table,
        projection,
        kind,
    });
    Ok((sql, fields))
}

fn schema_visualization(catalog: &Catalog) -> String {
    let nodes = catalog.nodes.keys().map(|label| {
        let label = string(label);
        format!("named_struct('id', {label}, 'label', {label}, 'name', {label}, 'indexes', make_array(), 'constraints', make_array())")
    }).collect::<Vec<_>>();
    let relationship = |name: &str, from: &str, to: &str| {
        let name = string(name);
        format!(
            "named_struct('id', {name}, 'type', {name}, 'name', {name}, 'source', {}, 'target', {})",
            string(from),
            string(to)
        )
    };
    let edges = catalog
        .edges
        .iter()
        .map(|(name, edge)| relationship(name, &edge.from.node, &edge.to.node))
        .collect::<Vec<_>>();
    let relationships = if edges.is_empty() {
        format!(
            "array_slice(make_array({}), 1, 0)",
            relationship("", "", "")
        )
    } else {
        format!("make_array({})", edges.join(", "))
    };
    format!(
        "SELECT make_array({}) AS nodes, {relationships} AS relationships",
        nodes.join(", ")
    )
}

fn take<T: serde::de::DeserializeOwned>(
    args: &mut Map<String, Value>,
    key: &str,
) -> Result<Option<T>> {
    args.remove(key)
        .map(serde_json::from_value)
        .transpose()
        .with_context(|| format!("invalid algorithm setting {key}"))
}

fn vertex(value: Value) -> Result<VertexKey> {
    #[derive(serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Key {
        label: String,
        key: Vec<Value>,
    }
    let Key { label, key } = serde_json::from_value(value)?;
    let key = key
        .into_iter()
        .map(|value| match value {
            Value::String(s) => Ok(ScalarValue::Utf8(Some(s))),
            Value::Number(n) if n.is_i64() || n.is_u64() => {
                Ok(ScalarValue::Utf8(Some(n.to_string())))
            }
            Value::Bool(b) => Ok(ScalarValue::Utf8(Some(b.to_string()))),
            _ => bail!("route key values must be non-null strings, integers or booleans"),
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(VertexKey { label, key })
}

fn constant(expr: &Expr) -> Result<Value> {
    Ok(match &expr.kind {
        ExprKind::Null => Value::Null,
        ExprKind::Boolean(value) => Value::from(*value),
        ExprKind::Integer(value) => Value::from(*value),
        ExprKind::Float(value) if value.is_finite() => Value::from(*value),
        ExprKind::String(value) => Value::from(value.clone()),
        ExprKind::List(items) => Value::Array(items.iter().map(constant).collect::<Result<_>>()?),
        ExprKind::Map(items) => Value::Object(
            items
                .iter()
                .map(|(key, value)| Ok((key.0.clone(), constant(value)?)))
                .collect::<Result<_>>()?,
        ),
        ExprKind::Unary {
            op: UnaryOp::Minus,
            operand,
        } => match &operand.kind {
            ExprKind::Integer(value) => {
                Value::from(value.checked_neg().context("integer argument overflow")?)
            }
            ExprKind::Float(value) if value.is_finite() => Value::from(-value),
            _ => bail!("expected a numeric procedure argument"),
        },
        _ => bail!("procedure arguments must be literals or bound parameters"),
    })
}
