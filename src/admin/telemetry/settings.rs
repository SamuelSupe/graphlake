use std::{collections::BTreeMap, path::Path, sync::Mutex};

use anyhow::{Context, Result, ensure};
use axum::{
    Json,
    http::{HeaderMap, StatusCode, header},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::{
    CONFIG_LIMIT, Config, EXPORT_FAILURES, EXPORT_FULL_OK, EXPORT_HTTP_OK, EXPORT_REJECTED,
    Relaxed, SIGNALS, TELEMETRY, endpoint,
};
use crate::admin::{ApiResult, Error, models};

static WRITE: Mutex<()> = Mutex::new(());
const TEMPLATE: &str = include_str!("../../../deploy/prometheus-rules.yaml");

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Alert {
    enabled: bool,
    delay_seconds: u32,
    window_seconds: Option<u32>,
    threshold: Option<f64>,
}

pub(super) type AlertOverrides = BTreeMap<String, Alert>;

fn defaults() -> AlertOverrides {
    [
        ("GraphLakeMetricsUnavailable", 120, None, None),
        ("GraphLakeRequestFailures", 300, Some(300), Some(0.1)),
        ("GraphLakeAuditWriteFailures", 60, Some(300), Some(0.0)),
        ("GraphLakeCoordinatorNotReady", 60, None, None),
        ("GraphLakeOtlpExportFailed", 120, Some(300), Some(0.0)),
        ("GraphLakeOtlpPartialRejected", 30, None, None),
        ("GraphLakeMonitorUnavailable", 60, None, None),
        ("GraphLakeCandidateUnavailable", 60, None, None),
        ("GraphLakeLeaderUnavailable", 60, None, None),
        ("GraphLakeWorkerUnavailable", 60, None, None),
        ("GraphLakeIngressUnavailable", 60, None, None),
        ("GraphLakeModelDiscoveryFailed", 60, None, None),
        ("GraphLakeModelUnavailable", 60, None, None),
        ("GraphLakeBackupFailed", 60, None, None),
        ("GraphLakeBackupStale", 60, None, None),
    ]
    .into_iter()
    .map(|(name, delay_seconds, window_seconds, threshold)| {
        (
            name.to_owned(),
            Alert {
                enabled: true,
                delay_seconds,
                window_seconds,
                threshold,
            },
        )
    })
    .collect()
}

pub(super) fn rules(overrides: &AlertOverrides) -> Result<(AlertOverrides, String)> {
    let mut settings = defaults();
    for (name, options) in overrides {
        let rule = settings.get_mut(name).context("未知的告警规则")?;
        ensure!(
            options.delay_seconds <= 86_400
                && options.window_seconds.is_some() == rule.window_seconds.is_some()
                && options
                    .window_seconds
                    .is_none_or(|v| (15..=86_400).contains(&v))
                && options.threshold.is_some() == rule.threshold.is_some()
                && options
                    .threshold
                    .is_none_or(|v| (0.0..=1_000_000.0).contains(&v)),
            "告警规则 {name} 的阈值、窗口或等待时间无效"
        );
        *rule = options.clone();
    }
    if settings == defaults() {
        return Ok((settings, TEMPLATE.to_owned()));
    }
    let mut doc: Value = serde_saphyr::from_str(TEMPLATE)?;
    let items = doc["groups"][0]["rules"]
        .as_array_mut()
        .context("invalid bundled rules")?;
    let mut enabled = Vec::with_capacity(items.len());
    for mut rule in items.drain(..) {
        let name = rule["alert"].as_str().context("invalid bundled rule")?;
        let Some(options) = settings.get(name) else {
            // Deployment rules outside the legacy settings API retain their packaged values.
            enabled.push(rule);
            continue;
        };
        if !options.enabled {
            continue;
        }
        rule["for"] = json!(format!("{}s", options.delay_seconds));
        if let Some(window) = options.window_seconds {
            let (expression, _) = rule["expr"]
                .as_str()
                .and_then(|expr| expr.rsplit_once(" > "))
                .context("invalid bundled expression")?;
            rule["expr"] = json!(format!(
                "{} > {}",
                expression.replace("[5m]", &format!("[{window}s]")),
                options.threshold.context("missing threshold")?
            ));
        }
        enabled.push(rule);
    }
    *items = enabled;
    if items.is_empty() {
        doc["groups"] = json!([]);
    }
    Ok((settings, serde_saphyr::to_string(&doc)?))
}

fn revision(text: Option<&str>) -> String {
    match text {
        Some(text) => models::revision(text),
        None => "unset".into(),
    }
}

fn load(path: &Path) -> ApiResult<Option<String>> {
    models::read_file(path, CONFIG_LIMIT).map_err(|_| {
        Error::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "settings_storage",
            "无法读取配置文件，请在服务主机检查文件和权限",
        )
    })
}

fn parse(text: Option<&str>) -> ApiResult<Config> {
    let config: Config = text
        .map(serde_saphyr::from_str)
        .transpose()
        .map_err(|_| {
            Error::invalid(
                "invalid_settings",
                "磁盘上的运行配置格式无效，请在服务主机修复",
            )
        })?
        .unwrap_or_default();
    endpoint(&config).map_err(|_| {
        Error::invalid(
            "invalid_settings",
            "磁盘上的 OTLP 地址无效，请在服务主机修复",
        )
    })?;
    Ok(config)
}

fn snapshot() -> ApiResult<Json<Value>> {
    let runtime = TELEMETRY.get();
    let active = runtime.map(|t| t.config.clone()).unwrap_or_default();
    let path = runtime.and_then(|t| t.path.as_deref());
    let text = path.map(load).transpose()?.flatten();
    let config = if path.is_some() {
        parse(text.as_deref())?
    } else {
        active.clone()
    };
    let (alerts, rules_yaml) = rules(&config.alerts).map_err(|_| {
        Error::invalid("invalid_settings", "磁盘上的告警规则无效，请在服务主机修复")
    })?;
    let restart_required = Config {
        alerts: active.alerts.clone(),
        ..config.clone()
    } != active;
    let signals: Vec<_> = SIGNALS.into_iter().enumerate().map(|(index, name)| {
        let last = EXPORT_HTTP_OK[index].load(Relaxed);
        let full = EXPORT_FULL_OK[index].load(Relaxed);
        json!({ "name": name, "failures": EXPORT_FAILURES[index].load(Relaxed), "last_http_ok_ms": (last != 0).then_some(last),
            "last_success_ms": (full != 0).then_some(full), "rejected": EXPORT_REJECTED[index].load(Relaxed) })
    }).collect();
    Ok(Json(json!({
        "active": active, "config": config, "alerts": alerts, "rules_yaml": rules_yaml,
        "revision": revision(text.as_deref()), "restart_required": restart_required,
        "file": path, "role": runtime.map(|t| t.role), "signals": signals,
    })))
}

pub(in crate::admin) async fn read() -> ApiResult<Json<Value>> {
    tokio::task::spawn_blocking(snapshot)
        .await
        .map_err(|_| Error::storage())?
}

fn allow_headers(candidate: &Config, active: &Config) -> Result<()> {
    if candidate.headers_env.is_empty() {
        return Ok(());
    }
    let origin = endpoint(candidate)?.map(|url| url.origin());
    let active_origin = endpoint(active)?.map(|url| url.origin());
    if origin.is_some()
        && origin == active_origin
        && candidate
            .headers_env
            .iter()
            .all(|(name, var)| active.headers_env.get(name) == Some(var))
    {
        return Ok(());
    }
    ensure!(
        origin.is_some()
            && std::env::var("GRAPHLAKE_OTLP_ALLOWED_ORIGINS")
                .unwrap_or_default()
                .split(',')
                .filter_map(|value| url::Url::parse(value.trim()).ok())
                .any(|url| Some(url.origin()) == origin)
            && candidate
                .headers_env
                .values()
                .all(|name| name.starts_with("GRAPHLAKE_OTLP_")
                    && name != "GRAPHLAKE_OTLP_ALLOWED_ORIGINS"),
        "凭据须使用 GRAPHLAKE_OTLP_ 环境变量，并由部署端通过 GRAPHLAKE_OTLP_ALLOWED_ORIGINS 允许接收端"
    );
    Ok(())
}

pub(in crate::admin) async fn save(
    headers: HeaderMap,
    Json(mut config): Json<Config>,
) -> ApiResult<Json<Value>> {
    let matched = headers
        .get(header::IF_MATCH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix('"')?.strip_suffix('"'))
        .ok_or_else(|| {
            Error::new(
                StatusCode::PRECONDITION_REQUIRED,
                "settings_revision_required",
                "保存前请先加载当前配置",
            )
        })?
        .to_owned();
    tokio::task::spawn_blocking(move || {
        let runtime = TELEMETRY
            .get()
            .filter(|t| t.path.is_some())
            .ok_or_else(|| {
                Error::new(
                    StatusCode::CONFLICT,
                    "settings_read_only",
                    "请从服务主机指定本进程的运行配置文件",
                )
            })?;
        let path = runtime.path.as_ref().unwrap();
        let _write = WRITE.lock().unwrap();
        let text = load(path)?;
        if matched != revision(text.as_deref()) {
            return Err(Error::new(
                StatusCode::PRECONDITION_FAILED,
                "settings_conflict",
                "配置已在其他位置修改。请重新加载，核对后再保存。",
            ));
        }
        // Store only overrides so new packaged defaults can be adopted on upgrade.
        let default = defaults();
        config
            .alerts
            .retain(|name, value| default.get(name) != Some(value));
        allow_headers(&config, &runtime.config)
            .and_then(|_| config.validate())
            .map_err(|e| Error::invalid("invalid_settings", e.to_string()))?;
        let yaml = serde_saphyr::to_string(&config).map_err(|_| Error::storage())?;
        if yaml.len() > CONFIG_LIMIT {
            return Err(Error::invalid("settings_limit", "运行配置超过 32 KiB"));
        }
        models::atomic_write(path, yaml.as_bytes(), text.is_none()).map_err(|_| {
            Error::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "settings_storage",
                "无法保存运行配置，请在服务主机检查配置目录的写入权限",
            )
        })?;
        snapshot()
    })
    .await
    .map_err(|_| Error::storage())?
}
