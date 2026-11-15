use std::sync::Arc;

use axum::{
    Extension, Json,
    extract::State,
    http::{HeaderMap, StatusCode, header},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::super::{ApiResult, App, Error, access::Principal, models, users};
use super::Service;

#[derive(Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub(in crate::admin) struct Config {
    pub enabled: bool,
    pub query: bool,
    pub modeling: bool,
    pub publish: bool,
    pub max_rows: usize,
    pub timeout_ms: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            enabled: false,
            query: true,
            modeling: false,
            publish: false,
            max_rows: 100,
            timeout_ms: 10_000,
        }
    }
}

impl Config {
    pub(in crate::admin) fn validate(&self) -> ApiResult<()> {
        if !(1..=500).contains(&self.max_rows) || !(1..=60_000).contains(&self.timeout_ms) {
            return Err(Error::invalid(
                "invalid_mcp_limits",
                "MCP 返回行数须为 1 至 500，查询超时须为 1 至 60000 毫秒",
            ));
        }
        if self.publish && !self.modeling {
            return Err(Error::invalid(
                "invalid_mcp_permissions",
                "允许发布前须先启用建模",
            ));
        }
        Ok(())
    }

    pub(in crate::admin) fn revision(&self) -> String {
        models::revision(&serde_json::to_string(self).expect("serializable MCP policy"))
    }
}

fn snapshot(app: &App, config: &Config) -> Value {
    json!({ "revision": config.revision(), "config": config, "endpoint": format!("{}/mcp", app.origin),
        "protocol_versions": super::VERSIONS, "server_timeout_ms": app.options.timeout.as_millis() as u64 })
}

pub(in crate::admin) async fn read(State(app): State<Arc<App>>) -> Json<Value> {
    Json(snapshot(&app, &app.users.mcp_config()))
}

pub(in crate::admin) async fn save(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Json(config): Json<Config>,
) -> ApiResult<Json<Value>> {
    config.validate()?;
    let expected = headers
        .get(header::IF_MATCH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix('"'))
        .and_then(|value| value.strip_suffix('"'))
        .ok_or_else(|| {
            Error::new(
                StatusCode::PRECONDITION_REQUIRED,
                "revision_required",
                "保存前需要读取 MCP 配置版本",
            )
        })?;
    users::save_mcp(app.clone(), config, expected.into()).await?;
    Ok(Json(snapshot(&app, &app.users.mcp_config())))
}

pub(in crate::admin) async fn access(
    State(app): State<Arc<App>>,
    Extension(principal): Extension<Principal>,
) -> ApiResult<Json<Value>> {
    let config = app.users.mcp_config();
    let mut value = snapshot(&app, &config);
    let service = |kind: Service, suffix: &str| {
        json!({
            "endpoint": format!("{}/mcp/{suffix}", app.origin),
            "enabled": kind.enabled(&config),
            "allowed": kind.allows(&app, &principal),
        })
    };
    value["services"] = json!({
        "query": service(Service::Query, "query"),
        "modeling": service(Service::Modeling, "modeling"),
    });
    value["owner"] = json!(principal.audit_name());
    let account_allowed = app.users.mcp_allowed(&principal);
    value["account_allowed"] = json!(account_allowed);
    value["can_issue"] =
        json!(super::can_use(&app, &principal) && app.auth.ai_owner_key().is_some());
    let mut effective = config;
    effective.query &= effective.enabled && account_allowed;
    effective.modeling &= effective.enabled && account_allowed;
    effective.publish &= effective.modeling;
    value["scope"] = super::tools::scope(&app, &principal, &effective)?;
    Ok(Json(value))
}
