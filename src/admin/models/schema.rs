use std::{collections::BTreeMap, sync::Arc, time::Duration};

use axum::{
    Extension, Json,
    extract::{Path, State},
};
use serde::Serialize;
use serde_json::{Value, json};

use crate::{
    Catalog, Endpoint, LabelSemantics, ModelSemantics, PropertySemantics, Scd2,
    arrow::datatypes::SchemaRef,
};

use super::super::{
    ApiResult, App,
    access::{self, Principal},
    users,
};

#[derive(Serialize)]
pub(in crate::admin) struct GraphSchema<'a> {
    graph: &'a str,
    #[serde(flatten)]
    semantics: Option<SemanticMetadata<'a, ModelSemantics>>,
    nodes: BTreeMap<&'a str, GraphLabel<'a>>,
    edges: BTreeMap<&'a str, GraphEdge<'a>>,
}

#[derive(Serialize)]
struct GraphLabel<'a> {
    key: &'a [String],
    properties: &'a [String],
    #[serde(flatten)]
    details: Option<LabelDetails<'a>>,
}

#[derive(Serialize)]
struct GraphEdge<'a> {
    #[serde(flatten)]
    label: GraphLabel<'a>,
    from: &'a Endpoint,
    to: &'a Endpoint,
}

#[derive(Serialize)]
struct LabelDetails<'a> {
    #[serde(flatten)]
    semantics: SemanticMetadata<'a, LabelSemantics>,
    property_details: BTreeMap<&'a str, PropertyDetails<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    temporal: Option<Value>,
}

#[derive(Serialize)]
struct SemanticMetadata<'a, T> {
    semantic_status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    semantics: Option<&'a T>,
}

impl<'a, T> SemanticMetadata<'a, T> {
    fn new(semantics: Option<&'a T>) -> Self {
        let semantic_status = if semantics.is_some() {
            "declared"
        } else {
            "unspecified"
        };
        Self {
            semantic_status,
            semantics,
        }
    }
}

#[derive(Serialize)]
struct PropertyDetails<'a> {
    data_type: Option<String>,
    nullable: Option<bool>,
    #[serde(flatten)]
    semantics: Option<&'a PropertySemantics>,
}

type Types = BTreeMap<String, SchemaRef>;

fn graph<'a>(catalog: &'a Catalog, enriched: bool, types: Option<&Types>) -> GraphSchema<'a> {
    let label = |name: &str,
                 key: &'a [String],
                 properties: &'a [String],
                 semantics: Option<&'a LabelSemantics>,
                 scd2: Option<&Scd2>| {
        GraphLabel {
            key,
            properties,
            details: enriched.then(|| {
                let schema = types.and_then(|types| types.get(name));
                let exposed = |column: &String| key.contains(column) || properties.contains(column);
                LabelDetails {
                    semantics: SemanticMetadata::new(semantics),
                    property_details: key
                        .iter()
                        .chain(properties)
                        .map(|name| {
                            let field = schema.and_then(|schema| schema.field_with_name(name).ok());
                            (
                                name.as_str(),
                                PropertyDetails {
                                    data_type: field.map(|field| field.data_type().to_string()),
                                    nullable: field.map(|field| field.is_nullable()),
                                    semantics: semantics
                                        .and_then(|metadata| metadata.properties.get(name)),
                                },
                            )
                        })
                        .collect(),
                    temporal: scd2.map(|rule| temporal(rule, exposed)),
                }
            }),
        }
    };
    GraphSchema {
        graph: &catalog.graph,
        semantics: enriched.then(|| SemanticMetadata::new(catalog.semantics.as_ref())),
        nodes: catalog
            .nodes
            .iter()
            .map(|(name, node)| {
                (
                    name.as_str(),
                    label(
                        name,
                        &node.key,
                        &node.properties,
                        node.semantics.as_ref(),
                        node.scd2.as_ref(),
                    ),
                )
            })
            .collect(),
        edges: catalog
            .edges
            .iter()
            .map(|(name, edge)| {
                (
                    name.as_str(),
                    GraphEdge {
                        label: label(
                            name,
                            &edge.key,
                            &edge.properties,
                            edge.semantics.as_ref(),
                            edge.scd2.as_ref(),
                        ),
                        from: &edge.from,
                        to: &edge.to,
                    },
                )
            })
            .collect(),
    }
}

fn temporal(scd2: &Scd2, exposed: impl Fn(&String) -> bool) -> Value {
    let history = scd2
        .valid_from
        .as_ref()
        .zip(scd2.valid_to.as_ref())
        .map(|(from, to)| {
            json!({
                "bounds": "[)",
                "valid_from": Some(from).filter(|name| exposed(name)),
                "valid_to": Some(to).filter(|name| exposed(name)),
                "null_start_is_unbounded": true,
                "open_end": scd2.open_end,
            })
        });
    let current = if let Some(flag) = &scd2.current_flag {
        json!({
            "mode": "current_flag",
            "field": Some(flag).filter(|name| exposed(name)),
            "value": scd2.current_value.clone().unwrap_or(Value::Bool(true)),
        })
    } else {
        json!({
            "mode": "open_end",
            "field": scd2.valid_to.as_ref().filter(|name| exposed(name)),
            "value": scd2.open_end,
        })
    };
    json!({"history": history, "current": current})
}

pub(in crate::admin) async fn schema(
    State(app): State<Arc<App>>,
    Path((id, revision)): Path<(String, String)>,
    Extension(principal): Extension<Principal>,
) -> ApiResult<Json<Value>> {
    let (_, _, engine) = app.query_model(&id, &revision, &principal).await?;
    Ok(Json(json!(graph(&engine.catalog, false, None))))
}

pub(in crate::admin) async fn mcp_schema(
    app: &Arc<App>,
    principal: &Principal,
    id: &str,
    include_types: bool,
    timeout: Duration,
) -> ApiResult<Json<Value>> {
    access::check_query(app, principal, id)?;
    let revision = app.published_model_revision(id).await?;
    let (_, _, mut engine) = app.query_model(id, &revision, principal).await?;
    let mut options = app.options.clone();
    options.timeout = options.timeout.min(timeout);
    let partition = access::partition(app, principal, id)?;
    if let Some(term) = &partition {
        users::apply_partition(&engine.catalog, &mut options.graph, term)?;
    }
    // Keep the published schema available when sources are offline or metadata slots are full.
    // Connector diagnostics may contain connection information and stay out of this response.
    let types = if include_types && let Ok(_permit) = app.schema_work.try_acquire() {
        crate::admin::ops::limit_model_probe(&mut options);
        tokio::time::timeout(options.timeout, async {
            let _selection = super::super::local_tables::managed::resolve(
                app,
                &mut engine,
                id,
                options.timeout.as_secs() + 30,
            )
            .await
            .ok()?;
            Box::pin(engine.inspect_model_with_admission(options, false, &app.probe_admission))
                .await
                .ok()
        })
        .await
        .ok()
        .flatten()
    } else {
        None
    };
    access::check_query(app, principal, id)?;
    if access::partition(app, principal, id)? != partition {
        return Err(access::forbidden());
    }
    let resolution = if types.is_some() {
        json!({"status": "resolved", "basis": "logical_mapping",
            "checked_at": chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)})
    } else if include_types {
        json!({"status": "unavailable"})
    } else {
        json!({"status": "not_requested"})
    };
    Ok(Json(json!({
        "model_revision": revision,
        "tasks": engine.catalog.tasks,
        "schema": graph(&engine.catalog, true, types.as_ref()),
        "type_resolution": resolution,
    })))
}
