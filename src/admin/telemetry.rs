use std::{
    io::Write as _,
    path::{Path, PathBuf},
    sync::{
        OnceLock,
        atomic::{AtomicU64, Ordering::Relaxed},
    },
    time::{Duration, SystemTime},
};

use anyhow::{Context as _, Result, ensure};
use axum::http::{HeaderMap, HeaderName, HeaderValue};
use opentelemetry::{
    Context, KeyValue,
    logs::{LogRecord, Logger, LoggerProvider, Severity},
    metrics::{Histogram, MeterProvider},
    propagation::TextMapPropagator,
    trace::{SpanKind, Status, TraceContextExt, Tracer, TracerProvider},
};
use opentelemetry_http::{HeaderExtractor, HeaderInjector, HttpClient, HttpError};
use opentelemetry_otlp::{Protocol, RetryPolicy, WithExportConfig, WithHttpConfig};
use opentelemetry_sdk::{
    Resource,
    logs::{SdkLogger, SdkLoggerProvider},
    metrics::{PeriodicReader, SdkMeterProvider},
    propagation::TraceContextPropagator,
    trace::{Sampler, SdkTracer, SdkTracerProvider},
};
use prost::Message;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

pub(super) const BUCKETS: [f64; 13] = [
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 15.0, 60.0,
];

static TELEMETRY: OnceLock<Telemetry> = OnceLock::new();
static EXPORT_FAILURES: [AtomicU64; 3] = [const { AtomicU64::new(0) }; 3];
static EXPORT_HTTP_OK: [AtomicU64; 3] = [const { AtomicU64::new(0) }; 3];
static EXPORT_FULL_OK: [AtomicU64; 3] = [const { AtomicU64::new(0) }; 3];
static EXPORT_REJECTED: [AtomicU64; 3] = [const { AtomicU64::new(0) }; 3];
static EXPORT_LAST_REJECTED: [AtomicU64; 3] = [const { AtomicU64::new(0) }; 3];
const SIGNALS: [&str; 3] = ["metrics", "logs", "traces"];
pub const DEFAULT_FILE: &str = ".graphlake-telemetry.yaml";
const CONFIG_LIMIT: usize = 32 * 1024;
pub(super) mod settings;

#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum LogFormat {
    #[default]
    Text,
    Json,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(default, deny_unknown_fields)]
pub(super) struct Config {
    enabled: bool,
    endpoint: Option<String>,
    service_name: String,
    instance_id: Option<String>,
    metrics: bool,
    logs: bool,
    traces: bool,
    trace_sample_ratio: f64,
    export_interval_ms: u64,
    timeout_ms: u64,
    log_format: LogFormat,
    include_audit_identity: bool,
    allow_insecure_http: bool,
    headers_env: std::collections::BTreeMap<String, String>,
    #[serde(skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    alerts: settings::AlertOverrides,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            enabled: false,
            endpoint: None,
            service_name: "graphlake".into(),
            instance_id: None,
            metrics: true,
            logs: true,
            traces: true,
            trace_sample_ratio: 0.1,
            export_interval_ms: 15_000,
            timeout_ms: 1_500,
            log_format: LogFormat::Text,
            include_audit_identity: false,
            allow_insecure_http: false,
            headers_env: Default::default(),
            alerts: Default::default(),
        }
    }
}

struct Telemetry {
    path: Option<PathBuf>,
    config: Config,
    log_format: LogFormat,
    include_audit_identity: bool,
    role: &'static str,
    metrics: Option<SdkMeterProvider>,
    latency: Option<Histogram<f64>>,
    log_provider: Option<SdkLoggerProvider>,
    logger: Option<SdkLogger>,
    trace_provider: Option<SdkTracerProvider>,
    tracer: Option<SdkTracer>,
}

#[derive(Clone, Debug)]
struct Collector {
    client: reqwest::blocking::Client,
    headers: HeaderMap,
}

// All three OTLP ExportServiceResponses use the same field numbers for
// partial_success and its rejected count, including when the response is empty.
#[derive(Message)]
struct ExportResponse {
    #[prost(message, optional, tag = "1")]
    partial_success: Option<PartialSuccess>,
}

#[derive(Message)]
struct PartialSuccess {
    #[prost(int64, tag = "1")]
    rejected: i64,
}

#[async_trait::async_trait]
impl HttpClient for Collector {
    async fn send_bytes(
        &self,
        mut request: opentelemetry_http::Request<bytes::Bytes>,
    ) -> Result<opentelemetry_http::Response<bytes::Bytes>, HttpError> {
        let signal = request.uri().path().rsplit('/').next().unwrap_or_default();
        let index = SIGNALS
            .iter()
            .position(|name| *name == signal)
            .expect("OTLP signal path");
        let signal = SIGNALS[index];
        // The configured collector receives only its own credentials, even when
        // the process inherits OTEL_EXPORTER_OTLP_HEADERS from another service.
        let original_headers = std::mem::take(request.headers_mut());
        for key in ["content-type", "content-encoding", "accept-encoding"] {
            if let Some(value) = original_headers.get(key) {
                request.headers_mut().insert(key, value.clone());
            }
        }
        request.headers_mut().extend(self.headers.clone());
        let result = self.client.send_bytes(request).await;
        let success = result.as_ref().ok().filter(|res| res.status().is_success());
        if let Some(response) = success {
            let now = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            EXPORT_HTTP_OK[index].store(now, Relaxed);
            if let Ok(decoded) = ExportResponse::decode(response.body().clone()) {
                let rejected = decoded
                    .partial_success
                    .map_or(0, |partial| partial.rejected);
                if let Ok(rejected) = u64::try_from(rejected) {
                    let previous = EXPORT_LAST_REJECTED[index].swap(rejected, Relaxed);
                    if rejected == 0 {
                        EXPORT_FULL_OK[index].store(now, Relaxed);
                        if previous > 0 {
                            local_log("INFO", "otlp_export_recovered", &json!({"signal": signal}));
                        }
                    } else {
                        EXPORT_REJECTED[index].fetch_add(rejected, Relaxed);
                        if previous == 0 {
                            local_log(
                                "WARN",
                                "otlp_partial_rejection",
                                &json!({"signal": signal, "rejected": rejected}),
                            );
                        }
                    }
                    return result;
                }
            }
        }
        {
            let failures = EXPORT_FAILURES[index].fetch_add(1, Relaxed) + 1;
            if failures == 1 || failures.is_multiple_of(64) {
                let status = result
                    .as_ref()
                    .ok()
                    .map(|response| response.status().as_u16());
                local_log(
                    "WARN",
                    "otlp_export_failed",
                    &json!({
                        "signal": SIGNALS[index], "http_status": status, "failures": failures,
                        "reason": if success.is_some() { "invalid_response" } else { "transport_or_http" }
                    }),
                );
            }
        }
        result
    }
}

fn endpoint(config: &Config) -> Result<Option<url::Url>> {
    let Some(value) = &config.endpoint else {
        return Ok(None);
    };
    let endpoint = url::Url::parse(value).context("invalid telemetry endpoint URL")?;
    let loopback = endpoint.host_str() == Some("localhost")
        || match endpoint.host() {
            Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
            Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
            _ => false,
        };
    ensure!(
        endpoint.host().is_some()
            && (endpoint.scheme() == "https"
                || endpoint.scheme() == "http" && (loopback || config.allow_insecure_http))
            && endpoint.username().is_empty()
            && endpoint.password().is_none()
            && endpoint.query().is_none()
            && endpoint.fragment().is_none(),
        "invalid telemetry endpoint; use HTTPS, loopback HTTP, or explicit allow_insecure_http"
    );
    Ok(Some(endpoint))
}

fn headers(config: &Config, check_values: bool) -> Result<HeaderMap> {
    ensure!(
        config.headers_env.len() <= 16,
        "telemetry supports at most 16 headers"
    );
    let mut headers = HeaderMap::new();
    for (header, env) in &config.headers_env {
        let name = HeaderName::try_from(header).context("invalid telemetry header name")?;
        ensure!(
            !matches!(
                name.as_str(),
                "host"
                    | "content-type"
                    | "content-encoding"
                    | "content-length"
                    | "transfer-encoding"
                    | "connection"
                    | "proxy-authorization"
                    | "accept-encoding"
            ),
            "reserved telemetry header"
        );
        ensure!(
            !env.is_empty()
                && env.len() <= 128
                && env.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_'),
            "invalid telemetry header environment variable"
        );
        let value = if check_values {
            std::env::var(env)
                .with_context(|| format!("missing telemetry header environment variable {env}"))?
        } else {
            String::from("placeholder")
        };
        ensure!(
            !value.is_empty() && value.len() <= 4096,
            "invalid telemetry header value length"
        );
        let mut value = HeaderValue::from_str(&value).context("invalid telemetry header value")?;
        value.set_sensitive(true);
        ensure!(
            headers.insert(name, value).is_none(),
            "duplicate telemetry header"
        );
    }
    Ok(headers)
}

impl Config {
    fn validate(&self) -> Result<()> {
        let url = endpoint(self)?;
        ensure!(
            !self.enabled || url.is_some(),
            "telemetry.endpoint is required when enabled"
        );
        ensure!(
            !self.enabled || self.metrics || self.logs || self.traces,
            "enable at least one telemetry signal"
        );
        ensure!(
            (0.0..=1.0).contains(&self.trace_sample_ratio),
            "trace_sample_ratio must be between 0 and 1"
        );
        ensure!(
            (100..=5_000).contains(&self.timeout_ms)
                && (1_000..=300_000).contains(&self.export_interval_ms),
            "invalid telemetry timeout or export interval"
        );
        for name in [Some(&self.service_name), self.instance_id.as_ref()]
            .into_iter()
            .flatten()
        {
            ensure!(
                !name.is_empty() && name.len() <= 128 && !name.chars().any(char::is_control),
                "invalid telemetry service_name or instance_id"
            );
        }
        headers(self, self.enabled)?;
        settings::rules(&self.alerts)?;
        Ok(())
    }
}

fn collector(config: &Config) -> Result<(url::Url, Collector)> {
    let endpoint = endpoint(config)?.context("telemetry.endpoint is required when enabled")?;
    let headers = headers(config, true)?;
    let client = reqwest::blocking::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_millis(config.timeout_ms))
        .build()?;
    Ok((endpoint, Collector { client, headers }))
}

/// Load process telemetry from an explicit file or the local catalog's default.
pub fn init(explicit: Option<&Path>, default: Option<&Path>, role: &'static str) -> Result<()> {
    let path = explicit.or(default).map(Path::to_path_buf);
    let text = path
        .as_deref()
        .map(|path| {
            super::models::read_file(path, CONFIG_LIMIT).map_err(|_| {
                anyhow::anyhow!(
                    "cannot read telemetry configuration (regular file, at most 32 KiB)"
                )
            })
        })
        .transpose()?
        .flatten();
    ensure!(
        explicit.is_none() || text.is_some(),
        "telemetry configuration does not exist"
    );
    let config: Config = text
        .as_deref()
        .map(serde_saphyr::from_str)
        .transpose()
        .context("invalid telemetry configuration")?
        .unwrap_or_default();
    config.validate()?;
    let mut runtime = Telemetry {
        path,
        config: config.clone(),
        log_format: config.log_format,
        include_audit_identity: config.include_audit_identity,
        role,
        metrics: None,
        latency: None,
        log_provider: None,
        logger: None,
        trace_provider: None,
        tracer: None,
    };
    if config.enabled {
        let (endpoint, collector) = collector(&config)?;
        let resource = Resource::builder_empty()
            .with_service_name(config.service_name)
            .with_attributes([
                KeyValue::new(
                    "service.instance.id",
                    config
                        .instance_id
                        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
                ),
                KeyValue::new(
                    "service.version",
                    option_env!("GRAPHLAKE_BUILD_VERSION").unwrap_or(env!("CARGO_PKG_VERSION")),
                ),
                KeyValue::new("graphlake.role", role),
            ])
            .build();
        let interval = Duration::from_millis(config.export_interval_ms);
        let timeout = Duration::from_millis(config.timeout_ms);
        let endpoint = endpoint.as_str().trim_end_matches('/');
        macro_rules! exporter {
            ($ty:ident, $signal:literal) => {
                opentelemetry_otlp::$ty::builder()
                    .with_http()
                    .with_protocol(Protocol::HttpBinary)
                    .with_endpoint(format!("{endpoint}/v1/{}", $signal))
                    .with_timeout(timeout)
                    .with_http_client(collector.clone())
                    .with_retry_policy(RetryPolicy::disabled())
                    .build()?
            };
        }
        if config.metrics {
            let reader = PeriodicReader::builder(exporter!(MetricExporter, "metrics"))
                .with_interval(interval)
                .build();
            let provider = SdkMeterProvider::builder()
                .with_resource(resource.clone())
                .with_reader(reader)
                .build();
            runtime.latency = Some(
                provider
                    .meter("graphlake")
                    .f64_histogram("graphlake.request.duration")
                    .with_unit("s")
                    .with_boundaries(BUCKETS.to_vec())
                    .build(),
            );
            runtime.metrics = Some(provider);
        }
        if config.logs {
            use opentelemetry_sdk::logs::{BatchConfigBuilder, BatchLogProcessor};
            let batch = BatchConfigBuilder::default()
                .with_max_queue_size(2048)
                .with_max_export_batch_size(256)
                .with_scheduled_delay(interval.min(Duration::from_secs(5)))
                .build();
            let processor = BatchLogProcessor::builder(exporter!(LogExporter, "logs"))
                .with_batch_config(batch)
                .build();
            let provider = SdkLoggerProvider::builder()
                .with_resource(resource.clone())
                .with_log_processor(processor)
                .build();
            runtime.logger = Some(provider.logger("graphlake"));
            runtime.log_provider = Some(provider);
        }
        if config.traces {
            use opentelemetry_sdk::trace::{BatchConfigBuilder, BatchSpanProcessor};
            let batch = BatchConfigBuilder::default()
                .with_max_queue_size(2048)
                .with_max_export_batch_size(256)
                .with_scheduled_delay(interval.min(Duration::from_secs(5)))
                .build();
            let processor = BatchSpanProcessor::builder(exporter!(SpanExporter, "traces"))
                .with_batch_config(batch)
                .build();
            let provider = SdkTracerProvider::builder()
                .with_resource(resource)
                .with_sampler(Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(
                    config.trace_sample_ratio,
                ))))
                .with_span_processor(processor)
                .build();
            runtime.tracer = Some(provider.tracer("graphlake"));
            runtime.trace_provider = Some(provider);
        }
    }
    TELEMETRY
        .set(runtime)
        .map_err(|_| anyhow::anyhow!("telemetry already initialized"))?;
    Ok(())
}

pub(super) fn audit_to_journal(entry: &Value) {
    if TELEMETRY
        .get()
        .is_some_and(|t| matches!(t.log_format, LogFormat::Json))
    {
        local_log("INFO", "graphlake.audit", entry);
    } else {
        let _ = writeln!(std::io::stderr().lock(), "graphlake-audit {entry}");
    }
}

/// Report a service or backup command ending without exposing its diagnostic, which can include paths.
pub fn report_exit(succeeded: bool) {
    if TELEMETRY.get().is_some() {
        event(
            if succeeded {
                Severity::Info
            } else {
                Severity::Error
            },
            if succeeded {
                "process_stopped"
            } else {
                "process_failed"
            },
            json!({}),
        );
    }
}

/// Flush bounded queues after the GraphLake runtime has stopped.
pub fn shutdown() {
    if let Some(telemetry) = TELEMETRY.get() {
        std::thread::scope(|scope| {
            if let Some(provider) = &telemetry.metrics {
                scope.spawn(move || {
                    let _ = provider.shutdown();
                });
            }
            if let Some(provider) = &telemetry.log_provider {
                scope.spawn(move || {
                    let _ = provider.shutdown();
                });
            }
            if let Some(provider) = &telemetry.trace_provider {
                scope.spawn(move || {
                    let _ = provider.shutdown();
                });
            }
        });
    }
}

pub(super) fn meter() -> Option<opentelemetry::metrics::Meter> {
    Some(TELEMETRY.get()?.metrics.as_ref()?.meter("graphlake"))
}

pub(super) fn failures() -> impl Iterator<Item = (&'static str, u64)> {
    SIGNALS
        .into_iter()
        .zip(EXPORT_FAILURES.iter().map(|count| count.load(Relaxed)))
}

pub(super) fn export_delivery() -> impl Iterator<Item = (&'static str, u64, u64, u64)> {
    SIGNALS.into_iter().enumerate().map(|(i, signal)| {
        (
            signal,
            EXPORT_FULL_OK[i].load(Relaxed),
            EXPORT_REJECTED[i].load(Relaxed),
            EXPORT_LAST_REJECTED[i].load(Relaxed),
        )
    })
}

pub(super) fn duration(
    protocol: &'static str,
    operation: &'static str,
    outcome: &'static str,
    seconds: f64,
) {
    if let Some(histogram) = TELEMETRY.get().and_then(|t| t.latency.as_ref()) {
        histogram.record(
            seconds,
            &[
                KeyValue::new("protocol", protocol),
                KeyValue::new("operation", operation),
                KeyValue::new("outcome", outcome),
            ],
        );
    }
}

pub(super) fn start_span(
    operation: &'static str,
    protocol: &'static str,
    incoming: Option<&HeaderMap>,
) -> Option<Context> {
    let tracer = TELEMETRY.get()?.tracer.as_ref()?;
    let parent = incoming.map_or_else(Context::current, |headers| {
        TraceContextPropagator::new().extract(&HeaderExtractor(headers))
    });
    let span = tracer
        .span_builder(operation)
        .with_kind(SpanKind::Server)
        .with_attributes([KeyValue::new("graphlake.protocol", protocol)])
        .start_with_context(tracer, &parent);
    Some(parent.with_span(span))
}

pub(super) fn trace_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    if TELEMETRY.get().is_some_and(|t| t.tracer.is_some()) {
        TraceContextPropagator::new()
            .inject_context(&Context::current(), &mut HeaderInjector(&mut headers));
    }
    headers
}

pub(super) fn finish_span(context: &Context, status: u16) {
    let span = context.span();
    span.set_attribute(KeyValue::new("graphlake.status_code", i64::from(status)));
    if status >= 400 {
        span.set_status(Status::error("operation failed"));
    }
    span.end();
}

pub(super) fn query_io_span(values: [u64; 4], peak_bytes: usize) {
    if TELEMETRY.get().is_none_or(|t| t.tracer.is_none()) {
        return;
    }
    let context = Context::current();
    let span = context.span();
    for (key, value) in [
        "graphlake.query.output_rows",
        "graphlake.query.source_requests",
        "graphlake.query.object_bytes",
        "graphlake.query.spilled_bytes",
        "graphlake.query.peak_reserved_bytes",
    ]
    .into_iter()
    .zip(values.into_iter().chain([peak_bytes as u64]))
    {
        span.set_attribute(KeyValue::new(key, i64::try_from(value).unwrap_or(i64::MAX)));
    }
}

fn local_log(level: &str, event: &str, fields: &Value) {
    if let Some(t) = TELEMETRY
        .get()
        .filter(|t| matches!(t.log_format, LogFormat::Json))
    {
        let _ = writeln!(
            std::io::stderr().lock(),
            "{}",
            json!({
                "timestamp": chrono::Utc::now().to_rfc3339(),
                "role": t.role, "level": level, "event": event, "fields": fields
            })
        );
    } else {
        let _ = writeln!(
            std::io::stderr().lock(),
            "GraphLake {level} {event} {fields}"
        );
    }
}

pub(super) fn event(level: Severity, name: &'static str, fields: Value) {
    local_log(level.name(), name, &fields);
    emit(level, name, &fields);
}

pub(super) fn audit(entry: &Value) {
    let Some(runtime) = TELEMETRY.get().filter(|t| t.logger.is_some()) else {
        return;
    };
    let mut fields = json!({
        "action": entry["action"], "protocol": entry["protocol"],
        "status": entry["status"], "outcome": entry["outcome"], "subject_type": entry["subject_type"]
    });
    if runtime.include_audit_identity {
        for key in ["subject", "model", "target"] {
            fields[key] = entry[key].clone();
        }
    }
    emit(
        if entry["status"].as_u64().is_some_and(|status| status >= 500) {
            Severity::Warn
        } else {
            Severity::Info
        },
        "graphlake.audit",
        &fields,
    );
}

fn emit(level: Severity, name: &'static str, fields: &Value) {
    let Some(logger) = TELEMETRY.get().and_then(|t| t.logger.as_ref()) else {
        return;
    };
    let mut record = logger.create_log_record();
    record.set_timestamp(SystemTime::now());
    record.set_event_name(name);
    record.set_severity_text(level.name());
    record.set_severity_number(level);
    record.set_body(name.into());
    for (key, value) in fields.as_object().into_iter().flatten() {
        if let Some(value) = value.as_i64() {
            record.add_attribute(key.clone(), value);
        } else if let Some(value) = value.as_str() {
            record.add_attribute(key.clone(), value.to_owned());
        } else if let Some(value) = value.as_bool() {
            record.add_attribute(key.clone(), value);
        }
    }
    let context = Context::current();
    let span = context.span();
    let span = span.span_context();
    if span.is_valid() {
        record.set_trace_context(span.trace_id(), span.span_id(), Some(span.trace_flags()));
    }
    logger.emit(record);
}
