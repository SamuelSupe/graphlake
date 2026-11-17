#![cfg(feature = "admin")]

#[path = "admin/mcp.rs"]
mod mcp;

#[path = "admin/local_tables.rs"]
mod local_tables;

use std::{
    fs,
    io::{BufRead, BufReader},
    path::Path,
    process::{Child, Command, Stdio},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, ensure};
use argon2::{Argon2, Params, PasswordHasher, password_hash::SaltString};
#[cfg(unix)]
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use graphlake::arrow::array::{
    Decimal128Array, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray,
};
use parquet::arrow::ArrowWriter;
use reqwest::{Client, Method, Response, StatusCode};
use serde_json::{Value, json};
use tokio_postgres::NoTls;
use uuid::Uuid;

struct Server {
    child: Child,
    base: String,
    http: Client,
    cookie: Option<String>,
}

const PASSWORD: &str = "graphlake-test-password";

impl Server {
    fn start(root: &Path, dsn: &str, port: u16) -> Result<Self> {
        Self::start_with(root, dsn, port, &[])
    }

    fn start_with(root: &Path, dsn: &str, port: u16, extra: &[&str]) -> Result<Self> {
        let timeout: &[_] = if extra.contains(&"--timeout-ms") {
            &[]
        } else {
            &["--timeout-ms", "2500"]
        };
        let mut child = Command::new(env!("CARGO_BIN_EXE_graphlake-admin"))
            .arg("--port")
            .arg(port.to_string())
            .args(timeout)
            .arg("--catalog-dir")
            .arg(root)
            .arg("--assets-dir")
            .arg(root.join("assets"))
            .args(extra)
            .env("GRAPHLAKE_CLUSTER_TEST_TOKEN", PASSWORD.repeat(2))
            .env("GRAPHLAKE_ADMIN_TEST_PG_DSN", dsn)
            .env(
                "GRAPHLAKE_ADMIN_OFFLINE",
                "postgresql://reader@127.0.0.1:1/fixture?sslmode=disable",
            )
            .stderr(Stdio::piped())
            .spawn()?;
        let mut message = String::new();
        let mut stderr = BufReader::new(child.stderr.take().unwrap());
        while stderr.read_line(&mut message)? > 0 && message.starts_with("GraphLake model ") {
            message.clear();
        }
        let Some(base) = message.trim().strip_prefix("GraphLake admin ") else {
            let _ = child.kill();
            let _ = child.wait();
            anyhow::bail!("admin server failed to start: {message}");
        };
        std::thread::spawn(move || {
            let _ = std::io::copy(&mut stderr, &mut std::io::sink());
        });
        Ok(Self {
            child,
            base: base.into(),
            http: Client::new(),
            cookie: None,
        })
    }

    fn guest(&self, method: Method, path: &str) -> reqwest::RequestBuilder {
        self.http
            .request(method, format!("{}/api/v1/{path}", self.base))
            .header("Origin", &self.base)
    }

    fn request(&self, method: Method, path: &str) -> reqwest::RequestBuilder {
        let request = self.guest(method, path);
        match &self.cookie {
            Some(cookie) => request.header("Cookie", cookie),
            None => request,
        }
    }

    async fn login(&mut self) -> Result<()> {
        let response = self
            .guest(Method::POST, "auth/login")
            .json(&json!({"username": "admin", "password": PASSWORD}))
            .send()
            .await?
            .error_for_status()?;
        let cookie = response.headers()["Set-Cookie"].to_str()?;
        let port = self.base.rsplit_once(':').unwrap().1;
        assert!(cookie.starts_with(&format!("graphlake_admin_{port}=")));
        assert!(cookie.contains("HttpOnly") && cookie.contains("SameSite=Strict"));
        self.cookie = Some(cookie.split_once(';').unwrap().0.to_owned());
        Ok(())
    }

    async fn query(&self, request_id: Uuid, language: &str, query: &str) -> Result<Response> {
        let model = self.model("payments").await?;
        self.execute(
            "query",
            request_id,
            language,
            query,
            &model["active_revision"],
            None,
        )
        .await
    }

    async fn model(&self, id: &str) -> Result<Value> {
        Ok(self
            .request(Method::GET, &format!("models/{id}"))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?)
    }

    async fn execute(
        &self,
        operation: &str,
        request_id: Uuid,
        language: &str,
        query: &str,
        revision: &Value,
        timeout: Option<u64>,
    ) -> Result<Response> {
        Ok(self
            .request(Method::POST, &format!("models/payments/{operation}"))
            .json(&json!({
                "request_id": request_id.to_string(), "language": language, "query": query, "model_revision": revision, "timeout_ms": timeout,
            }))
            .send()
            .await?)
    }

    async fn validate(&self, id: &str, yaml: &str) -> Result<Value> {
        let response = self
            .request(Method::POST, "models/validate")
            .json(&json!({ "id": id, "yaml": yaml }))
            .send()
            .await?
            .error_for_status()?;
        Ok(response.json().await?)
    }
}

#[tokio::test]
async fn roles_scope_management_and_revoke_existing_sessions() -> Result<()> {
    let root = tempfile::tempdir()?;
    setup_assets(root.path())?;
    setup_credentials(root.path())?;
    let yaml = r#"
graph: Payments
sources:
  src:
    type: parquet
    tables: { rows: [./rows.parquet] }
nodes:
  Account: { source: src.rows, key: [id] }
"#;
    fs::write(root.path().join("payments.yaml"), yaml)?;
    fs::write(root.path().join("private.yaml"), yaml)?;
    let mut server = Server::start(root.path(), "", 0)?;
    server.login().await?;
    let user = server
        .request(Method::PUT, "users/analyst")
        .json(&json!({"password": PASSWORD, "disabled": false}))
        .send()
        .await?
        .error_for_status()?
        .json::<Value>()
        .await?;
    assert!(user.get("password_hash").is_none());
    let group = server
        .request(Method::PUT, "roles/payments-operator")
        .json(&json!({"members":["analyst"], "grants":{"payments":["query"]}}))
        .send()
        .await?
        .error_for_status()?
        .json::<Value>()
        .await?;
    server
        .request(Method::PUT, "roles/payments-editor")
        .json(&json!({"members":["analyst"], "grants":{"payments":["modeler"]}}))
        .send()
        .await?
        .error_for_status()?;
    let token = "graphlake-role-member-service-token";
    let service = server
        .request(Method::PUT, "users/automated")
        .json(&json!({"service": true, "token": token, "disabled": false}))
        .send()
        .await?
        .error_for_status()?
        .json::<Value>()
        .await?;
    assert_eq!(service["service"], true);
    assert!(service.get("token_sha256").is_none());
    server
        .request(Method::PUT, "roles/automated-readers")
        .json(&json!({"members":["automated"], "grants":{"payments":["query"]}}))
        .send()
        .await?
        .error_for_status()?;
    for (method, path, status) in [
        (Method::GET, "models", StatusCode::OK),
        (
            Method::POST,
            "models/payments/export",
            StatusCode::FORBIDDEN,
        ),
        (
            Method::HEAD,
            "models/private/schema/revision",
            StatusCode::FORBIDDEN,
        ),
        (Method::GET, "users", StatusCode::FORBIDDEN),
        (Method::GET, "settings", StatusCode::FORBIDDEN),
        (Method::PUT, "settings", StatusCode::FORBIDDEN),
    ] {
        assert_eq!(
            server
                .guest(method, path)
                .basic_auth("svc/automated", Some(token))
                .send()
                .await?
                .status(),
            status,
            "{path}"
        );
    }
    let accounts = server
        .request(Method::GET, "users")
        .send()
        .await?
        .error_for_status()?
        .json::<Vec<Value>>()
        .await?;
    let current = accounts
        .iter()
        .find(|entry| entry["username"] == "automated")
        .context("service member missing after role assignment")?;
    server
        .request(Method::PUT, "users/automated")
        .json(&json!({"version": current["version"], "service": true, "disabled": true}))
        .send()
        .await?
        .error_for_status()?;
    assert_eq!(
        server
            .guest(Method::GET, "models")
            .basic_auth("svc/automated", Some(token))
            .send()
            .await?
            .status(),
        StatusCode::UNAUTHORIZED
    );
    let member_login = || async {
        let response = server
            .guest(Method::POST, "auth/login")
            .json(&json!({"username":"analyst", "password":PASSWORD}))
            .send()
            .await?
            .error_for_status()?;
        let cookie = response.headers()["Set-Cookie"]
            .to_str()?
            .split(';')
            .next()
            .unwrap()
            .to_owned();
        let status = response.json::<Value>().await?;
        assert_eq!(status["administrator"], false);
        assert_eq!(status["username"], "analyst");
        anyhow::Ok(cookie)
    };
    let first_cookie = member_login().await?;
    for _ in 0..5 {
        member_login().await?;
    }
    assert_eq!(
        server
            .guest(Method::GET, "models")
            .header("Cookie", first_cookie)
            .send()
            .await?
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        server.request(Method::GET, "models").send().await?.status(),
        StatusCode::OK
    );
    let cookie = member_login().await?;
    let member = |method, path| server.guest(method, path).header("Cookie", &cookie);
    let grants = member(Method::GET, "my-roles")
        .send()
        .await?
        .error_for_status()?
        .json::<Value>()
        .await?;
    assert_eq!(grants, json!({"payments":["query", "modeler"]}));
    let models = member(Method::GET, "models")
        .send()
        .await?
        .error_for_status()?
        .json::<Value>()
        .await?;
    assert_eq!(models.as_array().unwrap().len(), 1);
    assert_eq!(models[0]["id"], "payments");
    for (method, path) in [
        (Method::GET, "users"),
        (Method::HEAD, "models/private"),
        (Method::POST, "models/payments/publish"),
        (Method::GET, "roles"),
    ] {
        assert_eq!(
            member(method, path).send().await?.status(),
            StatusCode::FORBIDDEN,
            "{path}"
        );
    }
    for (method, path) in [
        (Method::POST, "models/payments/export"),
        (
            Method::HEAD,
            "models/payments/results/00000000-0000-0000-0000-000000000000",
        ),
    ] {
        assert_eq!(
            member(method, path).send().await?.status(),
            StatusCode::FORBIDDEN
        );
    }
    let model = member(Method::GET, "models/payments")
        .send()
        .await?
        .error_for_status()?
        .json::<Value>()
        .await?;
    let etag = format!("\"{}\"", model["revision"].as_str().unwrap());
    let save = |yaml: String| {
        member(Method::PUT, "models/payments")
            .header("If-Match", &etag)
            .json(&json!({"yaml":yaml}))
    };
    let escaped_source = save(yaml.replace("./rows.parquet", "/etc/passwd"))
        .send()
        .await?;
    assert_eq!(escaped_source.status(), StatusCode::FORBIDDEN);
    assert_eq!(
        escaped_source.json::<Value>().await?["code"],
        "source_scope"
    );
    save(yaml.replace("graph: Payments", "graph: Edited"))
        .send()
        .await?
        .error_for_status()?;
    server
        .request(Method::DELETE, "roles/payments-operator")
        .json(&group["version"])
        .send()
        .await?
        .error_for_status()?;
    assert_eq!(
        member(Method::GET, "models").send().await?.status(),
        StatusCode::UNAUTHORIZED
    );
    let cookie = member_login().await?;
    let grants = server
        .guest(Method::GET, "my-roles")
        .header("Cookie", &cookie)
        .send()
        .await?
        .error_for_status()?
        .json::<Value>()
        .await?;
    assert_eq!(grants, json!({"payments":["modeler"]}));
    for attempt in 0..31 {
        let response = server
            .guest(Method::POST, "auth/login")
            .json(&json!({"username":format!("missing-{attempt}"),"password":"incorrect test password"}))
            .send().await?;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert!(response.headers().get("Retry-After").is_none());
        assert_eq!(
            response.json::<Value>().await?["code"],
            "invalid_credentials"
        );
    }
    member_login().await?;
    for _ in 0..5 {
        assert_eq!(
            server
                .guest(Method::POST, "auth/login")
                .json(&json!({"username":"analyst","password":"incorrect test password"}))
                .send()
                .await?
                .status(),
            StatusCode::UNAUTHORIZED
        );
    }
    assert_eq!(
        server
            .guest(Method::POST, "auth/login")
            .json(&json!({"username":"analyst","password":PASSWORD}))
            .send()
            .await?
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        server
            .guest(Method::POST, "auth/login")
            .json(&json!({"username":"admin","password":PASSWORD}))
            .send()
            .await?
            .status(),
        StatusCode::OK
    );
    let audit = fs::read_to_string(root.path().join(".graphlake-auth/audit.jsonl"))?;
    assert!(
        audit
            .lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .any(|event| event["subject"] == "analyst"
                && event["action"] == "save_model"
                && event["outcome"] == "ok")
    );
    assert_eq!(
        server
            .guest(Method::POST, "models/payments/unpublish")
            .json(&json!({}))
            .send()
            .await?
            .status(),
        StatusCode::UNAUTHORIZED
    );
    let anonymous = fs::read_to_string(root.path().join(".graphlake-auth/audit-anonymous.jsonl"))?;
    let logged_denial = |contents: &str| {
        contents
            .lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .any(|entry| {
                entry["status"] == 401
                    && entry["subject"] == "anonymous"
                    && entry["action"] == "unpublish"
            })
    };
    assert!(logged_denial(&anonymous), "{anonymous}");
    assert!(!logged_denial(&fs::read_to_string(
        root.path().join(".graphlake-auth/audit.jsonl")
    )?));
    Ok(())
}

fn setup_assets(root: &Path) -> Result<()> {
    fs::create_dir(root.join("assets"))?;
    fs::write(
        root.join("assets/index.html"),
        "<!doctype html><title>GraphLake</title>",
    )?;
    Ok(())
}

#[tokio::test]
async fn current_scd2_exploration_starts_fresh_and_keeps_cursor_time() -> Result<()> {
    use graphlake::arrow::array::TimestampMillisecondArray;

    let root = tempfile::tempdir()?;
    setup_assets(root.path())?;
    setup_credentials(root.path())?;
    let write_parquet = |name: &str, batch: &RecordBatch| -> Result<()> {
        let staging = root.path().join(format!("{name}.staging"));
        let mut writer = ArrowWriter::try_new(fs::File::create(&staging)?, batch.schema(), None)?;
        writer.write(batch)?;
        writer.close()?;
        fs::rename(staging, root.path().join(format!("{name}.parquet")))?;
        Ok(())
    };
    write_parquet(
        "nodes",
        &RecordBatch::try_from_iter([("id", Arc::new(Int64Array::from(vec![1, 2, 3])) as _)])?,
    )?;
    let write_edges = |boundary| -> Result<()> {
        write_parquet(
            "edges",
            &RecordBatch::try_from_iter([
                ("id", Arc::new(Int64Array::from(vec![10, 20])) as _),
                ("src", Arc::new(Int64Array::from(vec![1, 1])) as _),
                ("dst", Arc::new(Int64Array::from(vec![2, 3])) as _),
                (
                    "since",
                    Arc::new(TimestampMillisecondArray::from(vec![0, boundary])) as _,
                ),
                (
                    "until",
                    Arc::new(TimestampMillisecondArray::from(vec![Some(boundary), None])) as _,
                ),
            ])?,
        )
    };
    write_edges(chrono::Utc::now().timestamp_millis() + 60_000)?;
    fs::write(
        root.path().join("payments.yaml"),
        r#"
graph: temporal
sources:
  store: {type: parquet, tables: {nodes: [nodes.parquet], edges: [edges.parquet]}}
nodes:
  Account: {source: store.nodes, key: [id]}
edges:
  Payment:
    source: store.edges
    key: [id]
    from: {node: Account, columns: [src]}
    to: {node: Account, columns: [dst]}
    scd2: {valid_from: since, valid_to: until}
"#,
    )?;
    let mut server = Server::start(root.path(), "", 0)?;
    server.login().await?;
    let model = server.model("payments").await?;
    let boundary = chrono::Utc::now().timestamp_millis() + 4000;
    write_edges(boundary)?;
    let explore = |cursor: Value| {
        server
            .request(Method::POST, "models/payments/explore")
            .json(&json!({
                "model_revision": model["active_revision"],
                "neighbors": true,
                "element": {"kind":"node", "label":"Account", "key":["1"]},
                "graph_options": {},
                "cursor": cursor,
            }))
    };
    let initial: Value = explore(Value::Null)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let edge = |result: &Value| -> Value {
        result["graph"]["elements"]
            .as_array()
            .unwrap()
            .iter()
            .find(|item| item["kind"] == "edge")
            .unwrap()["key"]
            .clone()
    };
    assert_eq!(edge(&initial), json!(["10"]));
    let wait = (boundary - chrono::Utc::now().timestamp_millis()).max(0) as u64;
    tokio::time::sleep(Duration::from_millis(wait + 20)).await;
    let fresh: Value = explore(Value::Null)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(edge(&fresh), json!(["20"]));
    let cursor = initial["page_cursor"].clone();
    assert!(cursor.is_string(), "{initial}");
    let replayed: Value = explore(cursor)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(edge(&replayed), json!(["10"]));
    Ok(())
}

#[tokio::test]
async fn operational_settings_are_admin_only_versioned_and_applied_on_restart() -> Result<()> {
    let root = tempfile::tempdir()?;
    setup_assets(root.path())?;
    setup_credentials(root.path())?;
    let mut server = Server::start(root.path(), "", 0)?;
    assert_eq!(
        server
            .http
            .get(format!("{}/settings", server.base))
            .send()
            .await?
            .status(),
        StatusCode::OK
    );
    for method in [Method::GET, Method::HEAD, Method::PUT] {
        assert_eq!(
            server.guest(method, "settings").send().await?.status(),
            StatusCode::UNAUTHORIZED
        );
    }
    server.login().await?;
    let state: Value = server
        .request(Method::GET, "settings")
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(state["revision"], "unset");
    assert_eq!(state["active"]["log_format"], "text");
    let mut config = state["config"].clone();
    config["enabled"] = json!(true);
    config["endpoint"] = json!("http://127.0.0.1:1");
    config["log_format"] = json!("json");
    config["alerts"]["GraphLakeRequestFailures"] =
        state["alerts"]["GraphLakeRequestFailures"].clone();
    config["alerts"]["GraphLakeRequestFailures"]["threshold"] = json!(0.25);
    config["alerts"]["GraphLakeRequestFailures"]["window_seconds"] = json!(240);
    assert_eq!(
        server
            .request(Method::PUT, "settings")
            .header("Origin", "https://foreign.example")
            .json(&config)
            .send()
            .await?
            .status(),
        StatusCode::FORBIDDEN
    );
    let mut unsafe_config = config.clone();
    unsafe_config["headers_env"] = json!({"authorization":"GRAPHLAKE_CLUSTER_TEST_TOKEN"});
    assert_eq!(
        server
            .request(Method::PUT, "settings")
            .header("If-Match", "\"unset\"")
            .json(&unsafe_config)
            .send()
            .await?
            .status(),
        StatusCode::UNPROCESSABLE_ENTITY
    );
    let request = server
        .request(Method::PUT, "settings")
        .header("If-Match", "\"unset\"")
        .json(&config);
    let saved: Value = request
        .try_clone()
        .unwrap()
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(saved["active"]["enabled"], false);
    assert_eq!(saved["restart_required"], true);
    let yaml: Value = serde_saphyr::from_str(saved["rules_yaml"].as_str().unwrap())?;
    let original_yaml: Value = serde_saphyr::from_str(state["rules_yaml"].as_str().unwrap())?;
    assert!(
        yaml["groups"][0]["rules"]
            .as_array()
            .unwrap()
            .iter()
            .any(|rule| {
                rule["alert"] == "GraphLakeRequestFailures"
                    && rule["expr"]
                        .as_str()
                        .is_some_and(|expr| expr.contains("[240s]") && expr.ends_with("> 0.25"))
            })
    );
    let deployment_rule = |doc: &Value| {
        doc["groups"][0]["rules"]
            .as_array()
            .unwrap()
            .iter()
            .find(|rule| rule["alert"] == "GraphLakeFreshnessBudgetExceeded")
            .cloned()
            .context("missing deployment freshness rule")
    };
    assert_eq!(deployment_rule(&yaml)?, deployment_rule(&original_yaml)?);
    assert_eq!(
        request.send().await?.status(),
        StatusCode::PRECONDITION_FAILED
    );
    drop(server);
    let mut server = Server::start(root.path(), "", 0)?;
    server.login().await?;
    let after: Value = server
        .request(Method::GET, "settings")
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(after["active"]["enabled"], true);
    assert_eq!(after["active"]["log_format"], "json");
    assert_eq!(after["restart_required"], false);
    assert_eq!(after["rules_yaml"], saved["rules_yaml"]);
    Ok(())
}

#[tokio::test]
async fn telemetry_exports_three_signals_and_isolates_collector_failure() -> Result<()> {
    use axum::{Router, body::Bytes, extract::State, http::HeaderMap, routing::post};
    use std::sync::atomic::{AtomicU8, Ordering::Relaxed};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let behavior = Arc::new(AtomicU8::new(0));
    let (sent, mut received) = tokio::sync::mpsc::channel(32);
    let router = Router::new()
        .route(
            "/ingest/v1/{signal}",
            post(
                |State((sent, behavior)): State<(
                    tokio::sync::mpsc::Sender<(String, Bytes)>,
                    Arc<AtomicU8>,
                )>,
                 axum::extract::Path(signal): axum::extract::Path<String>,
                 headers: HeaderMap,
                 body: Bytes| async move {
                    let expected_token = PASSWORD.repeat(2);
                    if headers
                        .get("content-type")
                        .is_none_or(|v| v != "application/x-protobuf")
                        || headers
                            .get("x-test-token")
                            .is_none_or(|v| v != expected_token.as_str())
                    {
                        return (StatusCode::UNAUTHORIZED, Bytes::new());
                    }
                    match behavior.load(Relaxed) {
                        1 => (StatusCode::SERVICE_UNAVAILABLE, Bytes::new()),
                        2 => (
                            StatusCode::OK,
                            Bytes::from_static(&[0x0a, 0x02, 0x08, 0x01]),
                        ),
                        _ => {
                            let _ = sent.try_send((signal, body));
                            (StatusCode::OK, Bytes::new())
                        }
                    }
                },
            ),
        )
        .with_state((sent, behavior.clone()));
    let collector = tokio::spawn(async move { axum::serve(listener, router).await });
    let root = tempfile::tempdir()?;
    setup_assets(root.path())?;
    setup_credentials(root.path())?;
    fs::write(
        root.path().join("payments.yaml"),
        include_str!("../examples/finance.yaml"),
    )?;
    let config = root.path().join(".telemetry.yaml");
    fs::write(
        &config,
        format!(
            "enabled: true\nendpoint: http://{address}/ingest\ntrace_sample_ratio: 1\nexport_interval_ms: 1000\ntimeout_ms: 200\nheaders_env:\n  x-test-token: GRAPHLAKE_CLUSTER_TEST_TOKEN\n"
        ),
    )?;
    let mut server = Server::start_with(
        root.path(),
        "",
        0,
        &["--telemetry-config", config.to_str().unwrap()],
    )?;
    server.login().await?;
    let query_marker = "graphlake-query-text-must-stay-local";
    let response = server
        .query(
            Uuid::new_v4(),
            "cypher",
            &format!("RETURN '{query_marker}' AS x"),
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let contains = |bytes: &[u8], value: &str| {
        bytes
            .windows(value.len())
            .any(|chunk| chunk == value.as_bytes())
    };
    tokio::time::timeout(Duration::from_secs(8), async {
        let (mut metrics, mut logs, mut traces) = (false, false, false);
        while !(metrics && logs && traces) {
            let (signal, body) = received.recv().await.context("collector terminated")?;
            assert!(!contains(&body, query_marker));
            assert!(!contains(&body, PASSWORD));
            match signal.as_str() {
                "metrics" => metrics |= contains(&body, "graphlake.request.duration"),
                "logs" => logs |= contains(&body, "graphlake.audit"),
                "traces" => traces |= contains(&body, "query"),
                _ => anyhow::bail!("unknown OTLP signal"),
            }
        }
        anyhow::Ok(())
    })
    .await??;

    behavior.store(1, Relaxed);
    let response = server
        .query(Uuid::new_v4(), "cypher", "RETURN 42 AS n")
        .await?;
    assert_eq!(response.json::<Value>().await?["rows"], json!([["42"]]));
    tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            let metrics = server.http.get(format!("{}/metrics", server.base)).send().await?.error_for_status()?.text().await?;
            if metrics.lines().any(|line| line.strip_prefix("graphlake_otlp_export_failures_total{signal=\"metrics\"} ").is_some_and(|n| n != "0")) {
                assert!(metrics.contains("graphlake_request_duration_seconds_bucket{protocol=\"http\",operation=\"query\""));
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        anyhow::Ok(())
    }).await??;
    let signal = |body: &str, name: &str| -> f64 {
        body.lines()
            .find_map(|line| {
                line.strip_prefix(&format!("{name}{{signal=\"metrics\"}} "))
                    .and_then(|x| x.parse().ok())
            })
            .unwrap()
    };
    let snapshot = || async {
        server
            .http
            .get(format!("{}/metrics", server.base))
            .send()
            .await?
            .error_for_status()?
            .text()
            .await
    };
    let before = signal(
        &snapshot().await?,
        "graphlake_otlp_last_success_timestamp_seconds",
    );
    assert!(before > 0.0);
    behavior.store(2, Relaxed);
    tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            let metrics = snapshot().await?;
            if signal(&metrics, "graphlake_otlp_last_rejected") > 0.0 {
                assert!(signal(&metrics, "graphlake_otlp_rejected_total") > 0.0);
                assert_eq!(
                    signal(&metrics, "graphlake_otlp_last_success_timestamp_seconds"),
                    before
                );
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        anyhow::Ok(())
    })
    .await??;
    behavior.store(0, Relaxed);
    tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            let metrics = snapshot().await?;
            if signal(&metrics, "graphlake_otlp_last_success_timestamp_seconds") > before {
                assert_eq!(signal(&metrics, "graphlake_otlp_last_rejected"), 0.0);
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        anyhow::Ok(())
    })
    .await??;
    collector.abort();
    Ok(())
}

fn setup_credentials(root: &Path) -> Result<()> {
    use std::io::Write;

    let dir = root.join(".graphlake-auth");
    let mut builder = fs::DirBuilder::new();
    let mut file = fs::OpenOptions::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
        builder.mode(0o700);
        file.mode(0o600);
    }
    builder.create(&dir)?;
    let params = Params::new(19_456, 2, 1, Some(32)).map_err(anyhow::Error::msg)?;
    let salt = SaltString::encode_b64(b"test-only-salt!!").map_err(anyhow::Error::msg)?;
    let hash = Argon2::new(argon2::Algorithm::Argon2id, argon2::Version::V0x13, params)
        .hash_password(PASSWORD.as_bytes(), &salt)
        .map_err(anyhow::Error::msg)?;
    write!(
        file.create_new(true)
            .write(true)
            .open(dir.join("password.phc"))?,
        "{hash}"
    )?;
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn admin_https_proxy_uses_public_host_origin_and_secure_cookie() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let root = dir.path();
    setup_assets(root)?;
    setup_credentials(root)?;

    let socket = std::net::TcpListener::bind("127.0.0.1:0")?;
    let port = socket.local_addr()?.port();
    drop(socket);
    let public = "https://graphlake.example:8443";
    let server = Server::start_with(root, "", port, &["--public-url", public])?;
    let endpoint = format!("http://127.0.0.1:{port}/api/v1/auth");
    let login = || {
        server
            .http
            .post(format!("{endpoint}/login"))
            .header("Host", "graphlake.example:8443")
            .json(&json!({"username":"admin","password":PASSWORD}))
    };
    let denied = login()
        .header("Origin", "http://graphlake.example:8443")
        .header("X-Forwarded-Proto", "https")
        .send()
        .await?;
    assert_eq!(denied.status(), StatusCode::FORBIDDEN);
    let denied = server
        .http
        .post(format!("{endpoint}/login"))
        .header("X-Forwarded-Host", "graphlake.example:8443")
        .header("Origin", public)
        .send()
        .await?;
    assert_eq!(denied.status(), StatusCode::FORBIDDEN);

    let logged_in = login()
        .header("Origin", public)
        .send()
        .await?
        .error_for_status()?;
    let set_cookie = logged_in.headers()["Set-Cookie"].to_str()?;
    assert!(set_cookie.starts_with("graphlake_admin_8443="));
    assert!(set_cookie.split("; ").any(|item| item == "Secure"));
    let cookie = set_cookie.split_once(';').unwrap().0;
    let logout = server
        .http
        .post(format!("{endpoint}/logout"))
        .header("Host", "graphlake.example:8443")
        .header("Origin", public)
        .header("Cookie", cookie)
        .send()
        .await?;
    assert_eq!(logout.status(), StatusCode::NO_CONTENT);
    assert!(
        logout.headers()["Set-Cookie"]
            .to_str()?
            .split("; ")
            .any(|item| item == "Secure")
    );
    let session = server
        .http
        .get(format!("{endpoint}/session"))
        .header("Host", "graphlake.example:8443")
        .header("Cookie", cookie)
        .send()
        .await?
        .json::<Value>()
        .await?;
    assert_eq!(session["authenticated"], false);
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn admin_auth_gates_management_and_revokes_sessions() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let root = dir.path();
    setup_assets(root)?;
    setup_credentials(root)?;
    fs::create_dir(root.join("assets/assets"))?;
    fs::write(root.join("assets/assets/demo.js"), "/* bundled js */")?;
    std::os::unix::fs::symlink(
        root.join(".graphlake-auth/password.phc"),
        root.join("assets/assets/leak.js"),
    )?;

    let token = "graphlake-local-service-test-token";
    let config = root.join(".graphlake-auth/service-accounts.json");
    let digest = <sha2::Sha256 as sha2::Digest>::digest(token.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    fs::write(
        &config,
        json!({"analytics":{"token_sha256":digest,"models":["payments"]}}).to_string(),
    )?;
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(&config, fs::Permissions::from_mode(0o600))?;

    let mut server = Server::start(root, "", 0)?;
    let account = |method, path: &str| {
        server
            .http
            .request(method, format!("{}/api/v1/{path}", server.base))
            .basic_auth("analytics", Some(token))
    };
    for method in [Method::GET, Method::HEAD] {
        assert_eq!(
            server
                .guest(method.clone(), "backups/status")
                .send()
                .await?
                .status(),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            account(method, "backups/status").send().await?.status(),
            StatusCode::FORBIDDEN
        );
    }
    let response = account(Method::GET, "models")
        .send()
        .await?
        .error_for_status()?;
    assert_eq!(response.json::<Value>().await?, json!([]));
    let dropped_in = include_str!("../examples/finance.yaml");
    let next_token = "graphlake-rotated-service-test-token";
    let digest = <sha2::Sha256 as sha2::Digest>::digest(next_token.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let replacement = tempfile::NamedTempFile::new_in(config.parent().unwrap())?;
    fs::write(
        replacement.path(),
        json!({
            "analytics":{"token_sha256": digest, "models":["payments"]}
        })
        .to_string(),
    )?;
    replacement.persist(&config)?;
    assert_eq!(
        account(Method::GET, "models").send().await?.status(),
        StatusCode::UNAUTHORIZED
    );
    let rotated = server
        .http
        .get(format!("{}/api/v1/models", server.base))
        .basic_auth("analytics", Some(next_token))
        .send()
        .await?;
    assert_eq!(rotated.status(), StatusCode::OK);
    // Keep checking the pre-existing access boundaries using the rotated credential.
    let account = |method, path: &str| {
        server
            .http
            .request(method, format!("{}/api/v1/{path}", server.base))
            .basic_auth("analytics", Some(next_token))
    };
    fs::write(root.join("payments.yaml"), dropped_in)?;
    let revision = URL_SAFE_NO_PAD.encode(<sha2::Sha256 as sha2::Digest>::digest(dropped_in));
    assert_eq!(
        account(Method::GET, &format!("models/payments/schema/{revision}"))
            .send()
            .await?
            .status(),
        StatusCode::NOT_FOUND
    );
    fs::remove_file(root.join("payments.yaml"))?;
    assert_eq!(
        account(Method::POST, "models/hidden/query")
            .json(&json!({}))
            .send()
            .await?
            .status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        account(Method::GET, "models/payments/history")
            .send()
            .await?
            .status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        account(Method::POST, "models/payments/query")
            .header("Origin", "http://localhost:7311")
            .json(&json!({}))
            .send()
            .await?
            .status(),
        StatusCode::FORBIDDEN
    );
    let audit = fs::read_to_string(root.join(".graphlake-auth/audit.jsonl"))?;
    assert!(!audit.contains(token));
    let events: Vec<Value> = audit
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<_, _>>()?;
    assert!(events.iter().any(|event| event["subject"] == "analytics"
        && event["protocol"] == "http"
        && event["model"] == "hidden"
        && event["action"] == "query"
        && event["status"] == 403));

    let probe = |path: &str| server.http.get(format!("{}/{path}", server.base));
    let live = probe("healthz").send().await?.error_for_status()?;
    assert_eq!(live.headers()["Cache-Control"], "no-store");
    assert_eq!(live.json::<Value>().await?["status"], "alive");
    let ready = probe("readyz").send().await?.error_for_status()?;
    assert_eq!(ready.json::<Value>().await?["published_models"], 0);
    fs::write(
        root.join(".graphlake-payments.json"),
        "invalid version state",
    )?;
    assert_eq!(
        probe("readyz").send().await?.status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(probe("healthz").send().await?.status(), StatusCode::OK);
    fs::remove_file(root.join(".graphlake-payments.json"))?;
    let orphan = root.join(format!(".graphlake-payments.{}.yaml", "a".repeat(43)));
    fs::write(&orphan, "orphaned version")?;
    assert_eq!(
        probe("readyz").send().await?.status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    fs::remove_file(orphan)?;
    assert_eq!(probe("readyz").send().await?.status(), StatusCode::OK);
    assert_eq!(
        probe("metrics")
            .header("Host", "localhost")
            .send()
            .await?
            .status(),
        StatusCode::FORBIDDEN
    );
    let client_errors = |metrics: &str| -> Result<u64> {
        Ok(metrics
            .lines()
            .find_map(|line| {
                line.strip_prefix("graphlake_http_requests_total{status_class=\"4xx\"} ")
            })
            .context("missing HTTP client error counter")?
            .parse()?)
    };
    let prior_metrics = probe("metrics")
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;
    let protected_endpoints = [
        (Method::GET, "models"),
        (Method::GET, "models/payments"),
        (Method::POST, "models"),
        (Method::PUT, "models/payments"),
        (Method::GET, "models/payments/history"),
        (Method::GET, "models/payments/history/revision"),
        (Method::POST, "models/validate"),
        (Method::POST, "models/payments/query"),
        (Method::POST, "models/payments/explore"),
        (Method::POST, "models/payments/export"),
        (Method::DELETE, "queries/request"),
        (Method::GET, "downloads/ticket"),
        (Method::HEAD, "downloads/ticket"),
        (Method::GET, "models/payments/results/snapshot"),
        (Method::HEAD, "models/payments/results/snapshot"),
        (
            Method::GET,
            "models/payments/results/00000000-0000-0000-0000-000000000000/graph/0",
        ),
        (
            Method::HEAD,
            "models/payments/results/00000000-0000-0000-0000-000000000000/graph/0",
        ),
    ];
    let denied_count = protected_endpoints.len() as u64;
    for (method, path) in protected_endpoints {
        let response = server.guest(method, path).send().await?;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED, "{path}");
        assert_eq!(response.headers()["Cache-Control"], "no-store");
    }
    let metrics = probe("metrics")
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;
    assert!(
        metrics
            .lines()
            .any(|line| line == "graphlake_query_slots 1")
    );
    assert_eq!(
        client_errors(&metrics)? - client_errors(&prior_metrics)?,
        denied_count
    );
    let response = server.guest(Method::GET, "auth/session").send().await?;
    assert_eq!(response.json::<Value>().await?["authenticated"], false);
    assert_eq!(
        server
            .guest(Method::GET, "auth/session")
            .header("Host", "localhost")
            .send()
            .await?
            .status(),
        StatusCode::FORBIDDEN
    );
    for path in ["auth/login", "auth/logout", "models"] {
        let response = server
            .http
            .post(format!("{}/api/v1/{path}", server.base))
            .header("Origin", "http://example.test")
            .send()
            .await?;
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        assert_eq!(response.headers()["Cache-Control"], "no-store");
    }
    for (path, status) in [
        ("/models/payments/query", StatusCode::OK),
        ("/assets/demo.js", StatusCode::OK),
        ("/.graphlake-auth/password.phc", StatusCode::NOT_FOUND),
        ("/assets/leak.js", StatusCode::NOT_FOUND),
        (
            "/assets/%2e%2e%2f.graphlake-auth%2fpassword.phc",
            StatusCode::NOT_FOUND,
        ),
        ("/payments.yaml", StatusCode::NOT_FOUND),
    ] {
        let response = server
            .http
            .get(format!("{}{path}", server.base))
            .send()
            .await?;
        assert_eq!(response.status(), status, "{path}");
    }
    let response = server
        .guest(Method::POST, "auth/login")
        .json(&json!({ "username": "admin", "password": "a".repeat(4096) }))
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    server.login().await?;
    let response = server.request(Method::GET, "auth/session").send().await?;
    assert_eq!(response.json::<Value>().await?["username"], "admin");
    assert_eq!(
        server.request(Method::GET, "models").send().await?.status(),
        StatusCode::OK
    );
    let response = server.request(Method::POST, "auth/logout").send().await?;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert!(
        response.headers()["Set-Cookie"]
            .to_str()?
            .contains("Max-Age=0")
    );
    assert_eq!(
        server.request(Method::GET, "models").send().await?.status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        server
            .guest(Method::POST, "auth/logout")
            .send()
            .await?
            .status(),
        StatusCode::NO_CONTENT
    );

    server.login().await?;
    let cookie = server.cookie.take().unwrap();
    let port = server.base.rsplit_once(':').unwrap().1.parse()?;
    drop(server);
    let server = Server::start(root, "", port)?;
    assert_eq!(
        server
            .guest(Method::GET, "models")
            .header("Cookie", cookie)
            .send()
            .await?
            .status(),
        StatusCode::UNAUTHORIZED
    );

    for _ in 0..5 {
        let (username, password) = ("admin", "incorrect test password");
        let response = server
            .guest(Method::POST, "auth/login")
            .json(&json!({"username": username, "password": password}))
            .send()
            .await?;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            response.json::<Value>().await?["code"],
            "invalid_credentials"
        );
    }
    let response = server
        .guest(Method::POST, "auth/login")
        .json(&json!({"username": "admin", "password": PASSWORD}))
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let response = server
        .guest(Method::POST, "auth/login")
        .json(&json!({"username": "admin", "password": "incorrect test password"}))
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    assert!(response.headers()["Retry-After"].to_str()?.parse::<u64>()? > 0);
    Ok(())
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn parquet_file(path: &Path, narrow_endpoint: bool) -> Result<()> {
    payments_file(path, narrow_endpoint, &[8])
}

fn payments_file(path: &Path, narrow_endpoint: bool, ids: &[i64]) -> Result<()> {
    let count = ids.len();
    let target = if narrow_endpoint {
        Arc::new(Int32Array::from(vec![7; count])) as _
    } else {
        Arc::new(Int64Array::from(vec![7; count])) as _
    };
    let batch = RecordBatch::try_from_iter([
        ("tenant_id", Arc::new(Int64Array::from(vec![1; count])) as _),
        ("payment_id", Arc::new(Int64Array::from(ids.to_vec())) as _),
        (
            "from_account_id",
            Arc::new(Int64Array::from(vec![9_007_199_254_740_993; count])) as _,
        ),
        ("to_merchant_id", target),
        (
            "score",
            Arc::new(Float64Array::from_iter_values(
                ids.iter().map(|&id| if id == 8 { -0.0 } else { 1.5 }),
            )) as _,
        ),
        (
            "channel",
            Arc::new(StringArray::from_iter_values(
                ids.iter()
                    .map(|&id| if id == 8 { "web-long-字符串" } else { "1" }),
            )) as _,
        ),
        (
            "amount",
            Arc::new(
                Decimal128Array::from_iter_values(ids.iter().map(|&id| {
                    if id == 8 {
                        12_250
                    } else {
                        12_000 + i128::from(id)
                    }
                }))
                .with_precision_and_scale(12, 3)?,
            ) as _,
        ),
    ])?;
    let mut writer = ArrowWriter::try_new(fs::File::create(path)?, batch.schema(), None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

#[tokio::test]
async fn object_storage_exploration_replays_pages_and_detects_object_replacement() -> Result<()> {
    use axum::{Router, body::Bytes, http::HeaderMap};
    use std::sync::atomic::{AtomicUsize, Ordering};

    let root = tempfile::tempdir()?;
    setup_assets(root.path())?;
    setup_credentials(root.path())?;
    let file = root.path().join("payments.parquet");
    payments_file(&file, false, &(1000..=1120).collect::<Vec<_>>())?;
    let contents = Bytes::from(fs::read(&file)?);
    let tag = Arc::new(AtomicUsize::new(1));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let router = Router::new().fallback({
        let tag = Arc::clone(&tag);
        move |method: Method, headers: HeaderMap| {
            let tag = tag.load(Ordering::Relaxed);
            let contents = contents.clone();
            async move {
                let total = contents.len();
                let mut response = axum::http::Response::builder()
                    .header("last-modified", "Mon, 01 Jun 2026 00:00:00 GMT")
                    .header("etag", format!("\"{tag}\""));
                let body = if method == Method::HEAD {
                    response = response.header("content-length", total);
                    Bytes::new()
                } else if let Some(range) = headers.get("range") {
                    let (start, end) = range
                        .to_str()
                        .unwrap()
                        .strip_prefix("bytes=")
                        .unwrap()
                        .split_once('-')
                        .unwrap();
                    let start: usize = start.parse().unwrap();
                    let end: usize = end.parse().unwrap();
                    response = response
                        .status(StatusCode::PARTIAL_CONTENT)
                        .header("content-range", format!("bytes {start}-{end}/{total}"))
                        .header("content-length", end + 1 - start);
                    contents.slice(start..=end)
                } else {
                    response = response.header("content-length", total);
                    contents
                };
                response.body(axum::body::Body::from(body)).unwrap()
            }
        }
    });
    let fixture = tokio::spawn(async move { axum::serve(listener, router).await });
    fs::write(
        root.path().join("accounts.jsonl"),
        "{\"tenant_id\":1,\"account_id\":9007199254740993}\n",
    )?;
    fs::write(
        root.path().join("merchants.jsonl"),
        "{\"tenant_id\":1,\"merchant_id\":7}\n",
    )?;
    let model = format!(
        r#"graph: payments
sources:
  nodes:
    type: jsonl
    tables:
      accounts:
        files: [accounts.jsonl]
        columns: [{{name: tenant_id, data_type: Int64}}, {{name: account_id, data_type: Int64}}]
      merchants:
        files: [merchants.jsonl]
        columns: [{{name: tenant_id, data_type: Int64}}, {{name: merchant_id, data_type: Int64}}]
  store:
    type: object_storage
    storage:
      provider: minio
      endpoint: 'http://{address}'
      bucket: 'fixture'
      access_key_env: GRAPHLAKE_CLUSTER_TEST_TOKEN
      secret_key_env: GRAPHLAKE_CLUSTER_TEST_TOKEN
    format: parquet
    tables:
      payments: {{files: [payments.parquet]}}
nodes:
  Account: {{source: nodes.accounts, key: [tenant_id, account_id]}}
  Merchant: {{source: nodes.merchants, key: [tenant_id, merchant_id]}}
edges:
  Payment:
    source: store.payments
    key: [tenant_id, payment_id]
    from: {{node: Account, columns: [tenant_id, from_account_id]}}
    to: {{node: Merchant, columns: [tenant_id, to_merchant_id]}}
"#
    );
    fs::write(root.path().join("payments.yaml"), model)?;
    let mut server = Server::start(root.path(), "", 0)?;
    server.login().await?;
    let revision = server.model("payments").await?["active_revision"].clone();
    let page =
        |cursor: Value| {
            server.request(Method::POST, "models/payments/explore").json(&json!({
            "model_revision": revision, "neighbors": true,
            "element": {"kind": "node", "label": "Account", "key": ["1", "9007199254740993"]},
            "graph_options": {}, "filter": {}, "cursor": cursor
        })).send()
        };
    let first: Value = page(Value::Null).await?.error_for_status()?.json().await?;
    let cursor = first["next_cursor"].clone();
    assert!(cursor.is_string(), "{first}");
    let second: Value = page(cursor.clone())
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(second["page_number"], 2);
    tag.store(2, Ordering::Relaxed);
    let stale = page(cursor).await?;
    assert_eq!(stale.status(), StatusCode::CONFLICT);
    assert_eq!(
        stale.json::<Value>().await?["code"],
        "graph_sources_changed"
    );
    fixture.abort();
    Ok(())
}

#[tokio::test]
#[ignore = "requires a writable PostgreSQL fixture in GRAPHLAKE_ADMIN_TEST_PG_DSN"]
async fn admin_persists_validates_and_queries_a_real_cross_source_model() -> Result<()> {
    let dsn = std::env::var("GRAPHLAKE_ADMIN_TEST_PG_DSN")?;
    let (pg, driver) = tokio_postgres::connect(&dsn, NoTls).await?;
    let connection = tokio::spawn(driver);
    let schema = format!("admin_{}", Uuid::new_v4().simple());
    pg.batch_execute(&format!(
        "CREATE SCHEMA {schema};
         CREATE TABLE {schema}.accounts (tenant_id BIGINT, account_id BIGINT, status TEXT, PRIMARY KEY (tenant_id, account_id));
         INSERT INTO {schema}.accounts SELECT 1, x, 'fixture' FROM generate_series(1, 600) x;
         INSERT INTO {schema}.accounts VALUES (1, 9007199254740993, 'source');
         CREATE TABLE {schema}.merchants (tenant_id BIGINT, merchant_id BIGINT, PRIMARY KEY (tenant_id, merchant_id));
         INSERT INTO {schema}.merchants VALUES (1, 7)"
    )).await?;

    let root = tempfile::tempdir()?;
    setup_assets(root.path())?;
    setup_credentials(root.path())?;
    parquet_file(&root.path().join("payments.parquet"), false)?;
    parquet_file(&root.path().join("wrong.parquet"), true)?;
    let yaml = format!(
        "graph: 支付关系图
sources:
  ledger: {{type: postgres, dsn_env: GRAPHLAKE_ADMIN_TEST_PG_DSN}}
  lake: {{type: parquet, tables: {{finance.payments: [payments.parquet]}}}}
nodes:
  Account:
    source: ledger.{schema}.accounts
    key: [tenant_id, account_id]
    properties: [status]
  Merchant:
    source: ledger.{schema}.merchants
    key: [tenant_id, merchant_id]
edges:
  Payment:
    source: lake.finance.payments
    key: [tenant_id, payment_id]
    from: {{node: Account, columns: [tenant_id, from_account_id]}}
    to: {{node: Merchant, columns: [tenant_id, to_merchant_id]}}
    properties: [amount]
export_locations: {{local: .}}
"
    );
    fs::write(root.path().join("legacy.yaml"), &yaml)?;
    let mut server = Server::start(root.path(), &dsn, 0)?;
    server.login().await?;
    let legacy = server.model("legacy").await?;
    assert_eq!(legacy["active_revision"], legacy["revision"]);
    fs::write(root.path().join("legacy.yaml"), "not a graph catalog")?;
    let broken_legacy = server.model("legacy").await?;
    assert_ne!(broken_legacy["revision"], legacy["revision"]);
    assert_eq!(broken_legacy["active_revision"], legacy["revision"]);
    let legacy_result: Value = server.request(Method::POST, "models/legacy/query")
        .json(&json!({ "request_id": Uuid::new_v4().to_string(), "model_revision": legacy["revision"],
            "language": "cypher", "query": "MATCH (:Account)-[e:Payment]->(:Merchant) RETURN e.amount" }))
        .send().await?.error_for_status()?.json().await?;
    assert_eq!(legacy_result["rows"], json!([["12.250"]]));
    let cross_origin = server
        .request(Method::POST, "models")
        .header("Origin", "https://unrelated.example")
        .json(&json!({ "id": "payments", "yaml": yaml }))
        .send()
        .await?;
    assert_eq!(cross_origin.status(), StatusCode::FORBIDDEN);
    let wrong_host = server
        .request(Method::GET, "models")
        .header("Host", "unrelated.example")
        .send()
        .await?;
    assert_eq!(wrong_host.status(), StatusCode::FORBIDDEN);

    let report = server.validate("payments", &yaml).await?;
    assert_eq!(report["status"], "passed", "{report}");
    assert!(
        report["labels"]["Payment"]
            .as_array()
            .context("no edge fields")?
            .iter()
            .any(|field| field["name"] == "to_merchant_id" && field["data_type"] == "Int64")
    );
    for (bad, location) in [
        (
            yaml.replace("key: [tenant_id, merchant_id]", "key: [tenant_id, 缺失键]"),
            json!({"kind": "table", "table": format!("ledger.{schema}.merchants"), "column": "缺失键"}),
        ),
        (
            yaml.replace("properties: [amount]", "properties: [absent_amount]"),
            json!({"kind": "table", "table": "lake.finance.payments", "column": "absent_amount"}),
        ),
        (
            yaml.replace(
                "columns: [tenant_id, to_merchant_id]",
                "columns: [tenant_id, amount]",
            ),
            json!({"kind": "field", "label": "Payment", "field": "to", "index": 1}),
        ),
        (
            yaml.replace("source: lake.finance.payments", "inputs:\n      - table: lake.finance.payments\n      - table: lake.finance.payments\n        columns: {to_merchant_id: amount}"),
            json!({"kind": "input_column", "label": "Payment", "input": 1, "column": "to_merchant_id"}),
        ),
    ] {
        let report = server.validate("payments", &bad).await?;
        assert_eq!(report["status"], "failed", "{report}");
        assert_eq!(report["location"], location, "{report}");
    }
    let response = server
        .request(Method::POST, "models")
        .json(&json!({ "id": "payments", "yaml": yaml }))
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::CREATED);
    let version = response.headers()["ETag"].clone();
    let created: Value = response.json().await?;
    assert!(created["active_revision"].is_null());
    let rev1 = created["revision"].clone();
    assert_eq!(
        server
            .execute("query", Uuid::new_v4(), "sql", "SELECT 1", &rev1, None)
            .await?
            .status(),
        StatusCode::CONFLICT
    );
    let published: Value = server
        .request(Method::POST, "models/payments/publish")
        .json(&json!({ "revision": rev1, "active_revision": null }))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(published["active_revision"], rev1);
    pg.batch_execute(&format!(
        "CREATE VIEW {schema}.duplicate_accounts AS SELECT * FROM {schema}.accounts UNION ALL SELECT * FROM {schema}.accounts WHERE account_id = 1;
         CREATE VIEW {schema}.null_accounts AS SELECT * FROM {schema}.accounts UNION ALL SELECT 1, NULL::bigint, 'invalid';
         CREATE VIEW {schema}.empty_merchants AS SELECT * FROM {schema}.merchants WHERE false"
    )).await?;
    for (bad, expected) in [
        (
            yaml.replace(".accounts\n", ".duplicate_accounts\n"),
            "duplicate business key",
        ),
        (yaml.replace(".accounts\n", ".null_accounts\n"), "空业务键"),
        (
            yaml.replace(".merchants\n", ".empty_merchants\n"),
            "端点无法匹配",
        ),
    ] {
        assert_eq!(server.validate("bad", &bad).await?["status"], "passed");
        let id = format!("bad-{}", Uuid::new_v4().simple());
        let draft: Value = server
            .request(Method::POST, "models")
            .json(&json!({ "id": id, "yaml": bad }))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        let path = format!("models/{id}/publish");
        let denied = server
            .request(Method::POST, &path)
            .json(&json!({ "revision": draft["revision"], "active_revision": null }))
            .send()
            .await?;
        assert_eq!(denied.status(), StatusCode::UNPROCESSABLE_ENTITY);
        assert!(
            denied.json::<Value>().await?["message"]
                .as_str()
                .unwrap()
                .contains(expected)
        );
        assert!(server.model(&id).await?["active_revision"].is_null());
    }
    let historical: Value = server
        .request(
            Method::GET,
            &format!("models/payments/history/{}", rev1.as_str().unwrap()),
        )
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(historical["yaml"], yaml);
    let saved: Value = server.request(Method::POST, "models/payments/query").json(&json!({
        "request_id": Uuid::new_v4(), "model_revision": rev1, "language":"sql", "capture_result": true,
        "query": "SELECT account_id, status FROM \"Account\" ORDER BY account_id",
        "graph_options": { "logical_partition": [{"column": "tenant_id", "op": "eq", "value":1}] }
    })).send().await?.error_for_status()?.json().await?;
    assert_eq!(saved["rows"].as_array().unwrap().len(), 500);
    assert_eq!(saved["snapshot_complete"], true);
    pg.execute(&format!("UPDATE {schema}.accounts SET status = 'new snapshot' WHERE account_id = 9007199254740993"), &[]).await?;
    let path = format!(
        "models/payments/results/{}",
        saved["snapshot"].as_str().unwrap()
    );
    assert_eq!(
        server.request(Method::HEAD, &path).send().await?.status(),
        StatusCode::OK
    );
    let old = server
        .request(Method::GET, &path)
        .send()
        .await?
        .error_for_status()?
        .bytes()
        .await?;
    let rows = graphlake::arrow::ipc::reader::StreamReader::try_new(old.as_ref(), None)?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    assert_eq!(rows.iter().map(RecordBatch::num_rows).sum::<usize>(), 601);
    let last = rows.last().unwrap();
    let status = last
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(status.value(last.num_rows() - 1), "source");
    pg.execute(
        &format!(
            "UPDATE {schema}.accounts SET status = 'source' WHERE account_id = 9007199254740993"
        ),
        &[],
    )
    .await?;

    for (source, schema_name, table, key, expected) in [
        ("ledger", None, None, "schemas", schema.as_str()),
        ("ledger", Some(schema.as_str()), None, "tables", "accounts"),
        (
            "ledger",
            Some(schema.as_str()),
            Some("accounts"),
            "columns",
            "account_id",
        ),
        ("lake", None, None, "tables", "finance.payments"),
        ("lake", None, Some("finance.payments"), "columns", "amount"),
    ] {
        let listing: Value = server
            .request(Method::POST, "models/payments/browse")
            .json(&json!({ "yaml": yaml, "source": source, "schema": schema_name, "table": table }))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        assert!(
            listing[key]
                .as_array()
                .unwrap()
                .iter()
                .any(|entry| entry == expected || entry["name"] == expected),
            "{listing}"
        );
    }
    let summary: Value = server
        .request(Method::GET, "models")
        .send()
        .await?
        .json()
        .await?;
    assert_eq!(
        summary
            .as_array()
            .unwrap()
            .iter()
            .find(|model| model["id"] == "payments")
            .unwrap()["status"],
        "passed"
    );

    let cross: Value = server.query(Uuid::new_v4(), "cypher",
        "MATCH (a:Account)-[e:Payment]->(b:Merchant) RETURN a.account_id AS id, e.amount AS amount, b.merchant_id AS merchant")
        .await?.error_for_status()?.json().await?;
    assert_eq!(cross["rows"], json!([["9007199254740993", "12.250", "7"]]));
    assert_eq!(cross["sources"].as_array().unwrap().len(), 2);
    assert_eq!(cross["model_revision"], rev1);
    let reduced: Value = server.query(Uuid::new_v4(), "cypher",
        "MATCH p=(a:Account)-[:Payment]->(:Merchant) WITH reduce(s=0, e IN relationships(p) | s+e.amount) AS total RETURN total = 12.25 AS exact")
        .await?.error_for_status()?.json().await?;
    assert_eq!(reduced["rows"], json!([["true"]]));
    let skipped: Value = server.query(Uuid::new_v4(), "gremlin",
        "g.V().hasLabel('Account').hasKey('status').not(__.out('Payment')).order().by('account_id').skip(1).limit(1).values('account_id')")
        .await?.error_for_status()?.json().await?;
    assert_eq!(skipped["rows"], json!([["2"]]));
    for (language, query) in [
        (
            "cypher",
            "MATCH p = (a:Account)-[:Payment]->(b:Merchant) RETURN p",
        ),
        (
            "gremlin",
            "g.V().hasLabel('Account').outE('Payment').inV().path()",
        ),
    ] {
        let result: Value = server
            .query(Uuid::new_v4(), language, query)
            .await?
            .error_for_status()?
            .json()
            .await?;
        let path = result["paths"]
            .as_array()
            .unwrap()
            .first()
            .with_context(|| format!("missing {language} path: {result}"))?;
        assert_eq!(
            path["items"]
                .as_array()
                .unwrap()
                .iter()
                .map(|item| item["label"].as_str().unwrap())
                .collect::<Vec<_>>(),
            ["Account", "Payment", "Merchant"]
        );
    }
    for (language, query) in [
        (
            "cypher",
            "MATCH p=(a:Account)-[e:Payment]->(b:Merchant) RETURN a, e, b, p",
        ),
        (
            "cypher",
            "MATCH (a:Account)-[e:Payment]->(b:Merchant) RETURN e",
        ),
        (
            "cypher",
            "MATCH p=(m:Merchant)<-[e:Payment]-(a:Account) RETURN p, e",
        ),
        ("gremlin", "g.E().hasLabel('Payment').limit(1)"),
    ] {
        let reply = server.query(Uuid::new_v4(), language, query).await?;
        let code = reply.status();
        let result: Value = reply.json().await?;
        assert!(code.is_success(), "{query}: {code} {result}");
        let elements = result["graph"]["elements"].as_array().unwrap();
        let find = |label: &str| {
            elements
                .iter()
                .find(|item| item["label"] == label)
                .unwrap_or_else(|| panic!("{query}: {result}"))
        };
        assert_eq!(elements.len(), 3, "{query}: {result}");
        assert_eq!(find("Account")["key"], json!(["1", "9007199254740993"]));
        assert_eq!(find("Payment")["from"], find("Account")["id"]);
        assert_eq!(find("Payment")["to"], find("Merchant")["id"]);
        assert_eq!(find("Payment")["directed"], true);
        assert_eq!(find("Payment")["properties"]["amount"], "12.250");
    }
    let retained: Value = server
        .request(Method::POST, "models/payments/query")
        .json(&json!({
            "request_id": Uuid::new_v4(), "model_revision": rev1, "language": "cypher",
            "query": "MATCH (a:Account) RETURN a ORDER BY a.account_id",
            "capture_graph": true,
        }))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(retained["rows"].as_array().unwrap().len(), 500);
    let snapshot = retained["graph_snapshot"]
        .as_str()
        .context("missing graph snapshot")?;
    pg.execute(&format!("UPDATE {schema}.accounts SET status = 'changed after graph query' WHERE account_id = 9007199254740993"), &[]).await?;
    let mut next = retained["graph_next"].as_u64();
    assert!(next.is_some());
    let mut retained_ids = std::collections::BTreeSet::new();
    let mut graph = retained["graph"].clone();
    let mut replacement = next == Some(0);
    loop {
        if !replacement {
            for element in graph["elements"].as_array().unwrap() {
                assert_eq!(
                    element["properties"]["status"],
                    if element["key"][1] == "9007199254740993" {
                        "source"
                    } else {
                        "fixture"
                    }
                );
                assert!(retained_ids.insert(element["id"].as_str().unwrap().to_owned()));
            }
        }
        replacement = false;
        let Some(page) = next else { break };
        let path = format!("models/payments/results/{snapshot}/graph/{page}");
        let reply: Value = server
            .request(Method::GET, &path)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        assert_eq!(reply["model_revision"], rev1);
        assert_eq!(reply["incomplete"], false);
        next = reply["next"].as_u64();
        graph = reply["graph"].clone();
    }
    assert_eq!(retained_ids.len(), 601);
    pg.execute(
        &format!(
            "UPDATE {schema}.accounts SET status = 'source' WHERE account_id = 9007199254740993"
        ),
        &[],
    )
    .await?;
    let payments = root.path().join("payments.parquet");
    payments_file(&payments, false, &(1001..=1601).collect::<Vec<_>>())?;
    let retained: Value = server
        .request(Method::POST, "models/payments/query")
        .json(&json!({
            "request_id": Uuid::new_v4(), "model_revision": rev1, "language": "cypher",
            "query": "MATCH (:Account)-[e:Payment]->(:Merchant) RETURN e",
            "capture_graph": true,
        }))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(retained["rows"].as_array().unwrap().len(), 500);
    let snapshot = retained["graph_snapshot"]
        .as_str()
        .context("missing edge snapshot")?;
    let mut graph = retained["graph"].clone();
    let mut next = retained["graph_next"].as_u64();
    let mut retained_edges = std::collections::BTreeSet::new();
    loop {
        if next != Some(0) {
            let items = graph["elements"].as_array().unwrap();
            for edge in items.iter().filter(|item| item["kind"] == "edge") {
                for endpoint in ["from", "to"] {
                    assert!(
                        items
                            .iter()
                            .any(|item| item["kind"] == "node" && item["id"] == edge[endpoint])
                    );
                }
                assert!(retained_edges.insert(edge["key"][1].as_str().unwrap().parse::<i64>()?));
            }
        }
        let Some(page) = next else { break };
        let path = format!("models/payments/results/{snapshot}/graph/{page}");
        let reply: Value = server
            .request(Method::GET, &path)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        assert_eq!(reply["incomplete"], false);
        next = reply["next"].as_u64();
        graph = reply["graph"].clone();
    }
    assert!(retained_edges.into_iter().eq(1001..=1601));
    parquet_file(&payments, false)?;
    for (language, query) in [
        (
            "cypher",
            "MATCH (a:Account) WHERE a.account_id = 9007199254740993 RETURN a",
        ),
        ("gremlin", "g.V('Account[1,9007199254740993]')"),
    ] {
        let result: Value = server
            .query(Uuid::new_v4(), language, query)
            .await?
            .error_for_status()?
            .json()
            .await?;
        assert_eq!(
            result["graph"]["elements"][0]["properties"]["status"], "source",
            "{query}: {result}"
        );
    }
    let vertex_path: Value = server
        .query(
            Uuid::new_v4(),
            "gremlin",
            "g.V().hasLabel('Account').out('Payment').path()",
        )
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(
        vertex_path["graph"]["elements"].as_array().unwrap().len(),
        2
    );
    assert_eq!(vertex_path["graph"]["steps"].as_array().unwrap().len(), 1);
    let bounded: Value = server
        .query(
            Uuid::new_v4(),
            "cypher",
            "MATCH (a:Account) RETURN a LIMIT 300",
        )
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(bounded["rows"].as_array().unwrap().len(), 300);
    assert_eq!(bounded["graph"]["elements"].as_array().unwrap().len(), 240);
    assert_eq!(bounded["graph"]["truncated"], true);
    assert_eq!(bounded["truncated"], false);
    let partitioned: Value = server
        .query(
            Uuid::new_v4(),
            "cypher",
            "USING {logicalPartition: {tenant_id: 1}} MATCH (a:Account) \
             WHERE a.account_id = 9007199254740993 RETURN a",
        )
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(
        partitioned["graph_options"]["logical_partition"],
        json!([{ "column": "tenant_id", "op": "eq", "value": 1 }])
    );
    let explore = |revision: Value, neighbors: bool, element: Value, graph_options: Value| {
        server.request(Method::POST, "models/payments/explore").json(&json!({
            "model_revision": revision, "neighbors": neighbors, "element": element, "graph_options": graph_options,
        }))
    };
    let account = json!({ "kind": "node", "label": "Account", "key": ["1", "9007199254740993"] });
    let merchant = json!({ "kind": "node", "label": "Merchant", "key": ["1", "7"] });
    for target in [&account, &merchant] {
        let reply = explore(
            rev1.clone(),
            true,
            target.clone(),
            partitioned["graph_options"].clone(),
        )
        .send()
        .await?;
        let code = reply.status();
        let result: Value = reply.json().await?;
        assert!(code.is_success(), "{code} {result}");
        let elements = result["graph"]["elements"].as_array().unwrap();
        assert_eq!(elements.len(), 3, "{result}");
        assert_eq!(result["selected"]["label"], target["label"]);
        assert_eq!(
            elements
                .iter()
                .find(|item| item["label"] == "Payment")
                .unwrap()["properties"]["amount"],
            "12.250"
        );
    }
    let filtered: Value = explore(
        rev1.clone(),
        true,
        account.clone(),
        json!({
            "logical_partition": [{ "column": "tenant_id", "op": "eq", "value": 2 }],
        }),
    )
    .send()
    .await?
    .error_for_status()?
    .json()
    .await?;
    assert!(filtered["selected"].is_null(), "{filtered}");
    assert_eq!(filtered["graph"]["elements"], json!([]));
    assert_eq!(
        explore(json!("stale"), true, account, json!({}))
            .send()
            .await?
            .status(),
        StatusCode::CONFLICT
    );
    assert_eq!(
        explore(
            rev1.clone(),
            false,
            json!({ "kind": "node", "label": "Account", "key": ["1"] }),
            json!({})
        )
        .send()
        .await?
        .status(),
        StatusCode::UNPROCESSABLE_ENTITY
    );
    let result: Value = server
        .query(
            Uuid::new_v4(),
            "sql",
            "SELECT account_id FROM \"Account\" ORDER BY account_id",
        )
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(result["rows"].as_array().unwrap().len(), 500);
    assert_eq!(result["truncated"], true);
    let export = server.execute("export", Uuid::new_v4(), "sql",
        "SELECT account_id, CAST(12.25 AS DECIMAL(12,3)) AS amount FROM \"Account\" ORDER BY account_id", &rev1, None)
        .await?.error_for_status()?;
    let ticket = export.json::<Value>().await?["ticket"]
        .as_str()
        .context("export ticket")?
        .to_owned();
    let second_export: Value = server
        .execute(
            "export",
            Uuid::new_v4(),
            "cypher",
            "RETURN 42 AS value",
            &rev1,
            None,
        )
        .await?
        .error_for_status()?
        .json()
        .await?;
    let second_ticket = second_export["ticket"]
        .as_str()
        .context("second export ticket")?;
    let download = format!("downloads/{ticket}");
    let export = server
        .request(Method::GET, &download)
        .send()
        .await?
        .error_for_status()?;
    assert_eq!(
        export.headers()["x-graphlake-model-revision"],
        rev1.as_str().unwrap()
    );
    let bytes = export.bytes().await?;
    let second_result = server
        .request(Method::GET, &format!("downloads/{second_ticket}"))
        .send()
        .await?
        .error_for_status()?
        .bytes()
        .await?;
    assert_eq!(
        graphlake::arrow::ipc::reader::StreamReader::try_new(second_result.as_ref(), None)?
            .next()
            .context("second export batch")??
            .num_rows(),
        1
    );
    assert_eq!(
        server
            .request(Method::GET, &download)
            .send()
            .await?
            .status(),
        StatusCode::NOT_FOUND
    );
    let batches = graphlake::arrow::ipc::reader::StreamReader::try_new(bytes.as_ref(), None)?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    assert_eq!(
        batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
        601
    );
    let last = batches.last().unwrap();
    assert_eq!(
        last.column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(last.num_rows() - 1),
        9_007_199_254_740_993
    );
    assert_eq!(
        last.column(1)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap()
            .value(0),
        12_250
    );
    let response = server
        .query(
            Uuid::new_v4(),
            "sql",
            "SELECT repeat('x', 10000) AS wide FROM \"Account\"",
        )
        .await?;
    let bytes = response.error_for_status()?.bytes().await?;
    assert!(bytes.len() <= 1024 * 1024);
    let result: Value = serde_json::from_slice(&bytes)?;
    assert_eq!(result["truncated"], true);
    assert!(result["rows"].as_array().unwrap().len() < 500);
    let result: Value = server
        .query(Uuid::new_v4(), "gremlin", "g.V().limit(1).count()")
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(result["rows"], json!([["1"]]));
    let exported = server
        .query(
            Uuid::new_v4(),
            "cypher",
            "EXPORT TO 'local:blocked.csv' MATCH (n) RETURN n",
        )
        .await?;
    assert_eq!(exported.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert!(!root.path().join("blocked.csv").exists());

    let broken = server
        .validate(
            "payments",
            &yaml.replace("payments.parquet", "wrong.parquet"),
        )
        .await?;
    assert_eq!(broken["status"], "failed");
    assert!(
        broken["error"].as_str().unwrap().contains("to_merchant_id"),
        "{broken}"
    );
    let offline = yaml.replace("GRAPHLAKE_ADMIN_TEST_PG_DSN", "GRAPHLAKE_ADMIN_OFFLINE");
    assert_eq!(
        server.validate("offline", &offline).await?["status"],
        "failed"
    );
    assert_eq!(
        server
            .request(Method::POST, "models")
            .json(&json!({ "id": "offline", "yaml": offline }))
            .send()
            .await?
            .status(),
        StatusCode::CREATED
    );

    let independent = "graph: Independent
sources:
  lake: {type: parquet, tables: {finance.payments: [payments.parquet]}}
nodes:
  Independent: {source: lake.finance.payments, key: [payment_id]}
edges: {}
";
    pg.batch_execute(&format!(
        "BEGIN; LOCK TABLE {schema}.accounts IN ACCESS EXCLUSIVE MODE"
    ))
    .await?;
    let request_id = Uuid::new_v4();
    let (blocked, cancelled) =
        tokio::join!(
            server.query(request_id, "sql", "SELECT * FROM \"Account\""),
            async {
                let mut waiting = false;
                let (watch, driver) = tokio_postgres::connect(&dsn, NoTls).await?;
                let watch_driver = tokio::spawn(driver);
                for _ in 0..40 {
                    waiting = watch.query_one(
                    "SELECT EXISTS(SELECT 1 FROM pg_locks l JOIN pg_class c ON c.oid=l.relation
                      JOIN pg_namespace n ON n.oid=c.relnamespace
                      WHERE n.nspname=$1 AND c.relname='accounts' AND NOT l.granted)",
                    &[&schema],
                ).await?.get(0);
                    if waiting {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                watch_driver.abort();
                ensure!(
                    waiting,
                    "no PostgreSQL session was blocked by the fixture lock"
                );
                assert_eq!(
                    server.validate("independent", independent).await?["status"],
                    "passed"
                );
                assert_eq!(
                    server.query(request_id, "sql", "SELECT 1").await?.status(),
                    StatusCode::CONFLICT
                );
                for _ in 0..40 {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    let result = server
                        .request(Method::DELETE, &format!("queries/{request_id}"))
                        .send()
                        .await?;
                    if result.status() == StatusCode::NO_CONTENT {
                        return Ok(());
                    }
                }
                anyhow::bail!("active query was not cancellable")
            }
        );
    let blocked = blocked?;
    let status = blocked.status();
    let body = blocked.text().await?;
    cancelled.with_context(|| format!("query returned {status}: {body}"))?;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    let started = std::time::Instant::now();
    assert_eq!(
        server
            .execute(
                "query",
                Uuid::new_v4(),
                "sql",
                "SELECT * FROM \"Account\"",
                &rev1,
                Some(80)
            )
            .await?
            .status(),
        StatusCode::GATEWAY_TIMEOUT
    );
    assert!(
        started.elapsed() < Duration::from_millis(1600),
        "request ignored the 80 ms override"
    );
    assert_eq!(
        server
            .execute("query", Uuid::new_v4(), "sql", "SELECT 1", &rev1, Some(0))
            .await?
            .status(),
        StatusCode::UNPROCESSABLE_ENTITY
    );
    pg.batch_execute("ROLLBACK").await?;

    let broken_yaml = yaml.replace("payments.parquet", "wrong.parquet");
    let draft: Value = server
        .request(Method::PUT, "models/payments")
        .header("If-Match", version.clone())
        .json(&json!({ "yaml": broken_yaml }))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(draft["active_revision"], rev1);
    assert_eq!(
        server
            .request(Method::POST, "models/payments/publish")
            .json(&json!({ "revision": draft["revision"], "active_revision": rev1 }))
            .send()
            .await?
            .status(),
        StatusCode::UNPROCESSABLE_ENTITY
    );
    let result: Value = server
        .query(
            Uuid::new_v4(),
            "cypher",
            "MATCH (:Account)-[e:Payment]->(:Merchant) RETURN e.amount",
        )
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(result["rows"], json!([["12.250"]]));
    assert_eq!(result["model_revision"], rev1);
    let changed = yaml.replace("支付关系图", "外部修改");
    fs::write(root.path().join("payments.yaml"), &changed)?;
    assert_eq!(
        server
            .request(Method::PUT, "models/payments")
            .header("If-Match", version)
            .json(&json!({ "yaml": yaml }))
            .send()
            .await?
            .status(),
        StatusCode::PRECONDITION_FAILED
    );
    let updated: Value = server
        .request(Method::GET, "models/payments")
        .send()
        .await?
        .json()
        .await?;
    assert_eq!(updated["status"], "unchecked");
    assert_eq!(updated["yaml"], changed);

    assert_eq!(updated["active_revision"], rev1);
    let rev2 = updated["revision"].clone();
    let next: Value = server
        .request(Method::POST, "models/payments/publish")
        .json(&json!({ "revision": rev2, "active_revision": rev1 }))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(next["active_revision"], rev2);
    let stale_schema = server
        .request(
            Method::GET,
            &format!("models/payments/schema/{}", rev1.as_str().unwrap()),
        )
        .send()
        .await?;
    assert_eq!(stale_schema.status(), StatusCode::CONFLICT);
    let topology: Value = server
        .request(
            Method::GET,
            &format!("models/payments/schema/{}", rev2.as_str().unwrap()),
        )
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(topology["graph"], "外部修改");
    assert_eq!(
        topology["nodes"]["Account"]["key"],
        json!(["tenant_id", "account_id"])
    );
    assert_eq!(topology["edges"]["Payment"]["from"]["node"], "Account");
    assert!(topology.get("sources").is_none());

    let stale: Value = server
        .execute("query", Uuid::new_v4(), "sql", "SELECT 1", &rev1, None)
        .await?
        .json()
        .await?;
    assert_eq!(stale["code"], "active_revision_changed");
    let rollback: Value = server
        .request(Method::POST, "models/payments/rollback")
        .json(&json!({ "revision": rev1, "active_revision": rev2 }))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(rollback["yaml"], changed);
    assert_eq!(rollback["active_revision"], rev1);
    let history: Value = server
        .request(Method::GET, "models/payments/history")
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(history["versions"][0]["action"], "rollback");

    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(
            root.path().join("payments.yaml"),
            root.path().join("linked.yaml"),
        )?;
        assert_eq!(
            server
                .request(Method::GET, "models/linked")
                .send()
                .await?
                .status(),
            StatusCode::FORBIDDEN
        );
    }
    let traversal = server
        .request(Method::GET, "models/%2e%2e%2fpayments")
        .send()
        .await?;
    ensure!(
        !traversal.status().is_success(),
        "path traversal was accepted"
    );

    let probe_payload: Value = server
        .request(Method::GET, "models/payments/readiness")
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(probe_payload["model_revision"], rev1);
    assert_eq!(probe_payload["sources"], json!({"ledger":true,"lake":true}));
    let payment_file = root.path().join("payments.parquet");
    let offline_file = root.path().join("payments.offline");
    fs::rename(&payment_file, &offline_file)?;
    fs::copy(root.path().join("wrong.parquet"), &payment_file)?;
    let drift = server
        .request(Method::GET, "models/payments/readiness")
        .send()
        .await?;
    assert_eq!(drift.status(), StatusCode::SERVICE_UNAVAILABLE);
    let drift = drift.json::<Value>().await?;
    assert_eq!(drift["status"], "mapping_invalid", "{drift}");
    assert_eq!(drift["location"]["label"], "Payment", "{drift}");
    fs::remove_file(&payment_file)?;
    fs::rename(&offline_file, &payment_file)?;
    fs::rename(&payment_file, &offline_file)?;
    let offline_probe = server
        .request(Method::GET, "models/payments/readiness")
        .send()
        .await?;
    assert_eq!(offline_probe.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        offline_probe.json::<Value>().await?["sources"],
        json!({"ledger":true,"lake":false})
    );
    fs::rename(&offline_file, &payment_file)?;
    assert_eq!(
        server
            .request(Method::GET, "models/payments/readiness")
            .send()
            .await?
            .status(),
        StatusCode::OK
    );

    let raced = server
        .request(Method::POST, "models/payments/unpublish")
        .json(&json!({"active_revision": rev2}))
        .send()
        .await?;
    assert_eq!(raced.status(), StatusCode::CONFLICT);
    let offline: Value = server
        .request(Method::POST, "models/payments/unpublish")
        .json(&json!({"active_revision": rev1}))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert!(offline["active_revision"].is_null());
    assert_eq!(offline["yaml"], changed);
    for operation in ["query", "export"] {
        let rejected = server
            .execute(operation, Uuid::new_v4(), "sql", "SELECT 1", &rev1, None)
            .await?;
        assert_eq!(rejected.status(), StatusCode::CONFLICT);
        assert_eq!(rejected.json::<Value>().await?["code"], "unpublished");
    }

    drop(server);
    let mut restarted = Server::start(root.path(), &dsn, 0)?;
    restarted.login().await?;
    let loaded: Value = restarted
        .request(Method::GET, "models/payments")
        .send()
        .await?
        .json()
        .await?;
    assert_eq!(loaded["yaml"], changed);
    assert!(loaded["active_revision"].is_null());
    let archived: Value = restarted
        .request(
            Method::GET,
            &format!("models/payments/history/{}", rev1.as_str().unwrap()),
        )
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(archived["yaml"], yaml);
    let reactivated: Value = restarted
        .request(Method::POST, "models/payments/rollback")
        .json(&json!({"revision": rev1, "active_revision": null}))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(reactivated["active_revision"], rev1);
    let active_archive = root.path().join(format!(
        ".graphlake-payments.{}.yaml",
        rev1.as_str().unwrap()
    ));
    fs::write(&active_archive, "damaged archive")?;
    let blocked = restarted
        .query(Uuid::new_v4(), "cypher", "RETURN 1")
        .await?;
    assert_eq!(blocked.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        blocked.json::<Value>().await?["code"],
        "model_archive_unavailable"
    );
    drop(restarted);
    let mut restarted = Server::start(root.path(), &dsn, 0)?;
    restarted.login().await?;
    assert!(restarted.model("payments").await?["archive_error"].is_string());
    let healthy = restarted.request(Method::POST, "models/legacy/query")
        .json(&json!({"request_id":Uuid::new_v4(),"model_revision":legacy["revision"],"language":"cypher","query":"RETURN 1 AS n"}))
        .send().await?.error_for_status()?.json::<Value>().await?;
    assert_eq!(healthy["rows"], json!([["1"]]));
    let restore = format!("models/payments/history/{}", rev1.as_str().unwrap());
    let mismatch = restarted
        .request(Method::PUT, &restore)
        .json(&json!({"yaml": changed}))
        .send()
        .await?;
    assert_eq!(mismatch.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(fs::read_to_string(&active_archive)?, "damaged archive");
    let recovered = restarted
        .request(Method::PUT, &restore)
        .json(&json!({"yaml": yaml}))
        .send()
        .await?
        .error_for_status()?
        .json::<Value>()
        .await?;
    assert_eq!(recovered["active_revision"], rev1);
    assert!(recovered["archive_error"].is_null());
    assert_eq!(fs::read_to_string(&active_archive)?, yaml);

    let payments = root.path().join("payments.parquet");
    let replacement = root.path().join("new-payments.parquet");
    let ids = (1001..=1121).rev().chain([8]).collect::<Vec<_>>();
    payments_file(&replacement, false, &ids)?;
    fs::rename(&replacement, &payments)?;
    let neighbor_page = |filter: Value, cursor: Value, versions: Value| {
        restarted.request(Method::POST, "models/payments/explore").json(&json!({
            "model_revision": rev1, "neighbors": true,
            "element": { "kind": "node", "label": "Account", "key": ["1", "9007199254740993"] },
            "graph_options": {}, "filter": filter, "cursor": cursor, "source_versions": versions,
        }))
    };
    pg.batch_execute(&format!(
        "CREATE TABLE {schema}.unrelated (id int); INSERT INTO {schema}.unrelated VALUES (1)"
    ))
    .await?;
    pg.batch_execute(&format!(
        "BEGIN; LOCK {schema}.accounts IN ACCESS EXCLUSIVE MODE"
    ))
    .await?;
    let (explored, lightweight) = tokio::join!(
        neighbor_page(json!({}), Value::Null, json!({})).send(),
        async {
            for _ in 0..100 {
                let waiting: bool = pg.query_one(
                    "SELECT EXISTS(SELECT FROM pg_locks WHERE relation = $1::text::regclass AND NOT granted)",
                    &[&format!("{schema}.accounts")],
                ).await?.get(0);
                if waiting {
                    let response = restarted.query(Uuid::new_v4(), "cypher", "RETURN 1").await;
                    pg.batch_execute("ROLLBACK").await?;
                    return response;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            pg.batch_execute("ROLLBACK").await?;
            anyhow::bail!("exploration did not reach the PostgreSQL lock");
        }
    );
    explored?.error_for_status()?;
    lightweight?.error_for_status()?;
    let filter = json!({ "direction": "out", "edge_label": "Payment", "properties": [
        { "column": "amount", "op": "ge", "value": "13.001" }
    ] });
    let mut cursor = Value::Null;
    let mut first_cursor = Value::Null;
    let mut versions = json!({});
    let mut observed = Vec::new();
    for expected_count in [50, 50, 21] {
        let response: Value = neighbor_page(filter.clone(), cursor, versions.clone())
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        cursor = response["next_cursor"].clone();
        if versions.as_object().unwrap().is_empty() {
            first_cursor = cursor.clone();
            versions = Value::Object(
                response["sources"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|source| {
                        (
                            source["source"].as_str().unwrap().to_owned(),
                            source["version"].clone(),
                        )
                    })
                    .collect(),
            );
            pg.execute(&format!("UPDATE {schema}.unrelated SET id = id + 1"), &[])
                .await?;
        }
        let elements = response["graph"]["elements"].as_array().unwrap();
        let edges = elements
            .iter()
            .filter(|item| item["kind"] == "edge")
            .collect::<Vec<_>>();
        assert_eq!(edges.len(), expected_count);
        for edge in edges {
            assert!(elements.iter().any(|item| item["id"] == edge["from"]));
            assert!(elements.iter().any(|item| item["id"] == edge["to"]));
            observed.push(edge["key"][1].as_str().unwrap().parse::<i64>()?);
        }
    }
    assert!(cursor.is_null());
    assert_eq!(observed, (1001..=1121).collect::<Vec<_>>());
    let search: Value = restarted
        .request(Method::POST, "models/payments/explore")
        .json(&json!({
            "model_revision": rev1, "neighbors": true,
            "element": { "kind": "node", "label": "Account", "key": ["1", "9007199254740993"] },
            "graph_options": {}, "filter": filter, "cursor": first_cursor,
            "search": { "query": "1121" },
        }))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(search["total"], 1);
    assert_eq!(search["matches"][0]["page"], 3);
    for hidden in [
        json!({"hidden_edges":["Payment"]}),
        json!({"hidden_nodes":["Merchant"]}),
    ] {
        let mut search = json!({"query":"1121"});
        search
            .as_object_mut()
            .unwrap()
            .extend(hidden.as_object().unwrap().clone());
        let filtered: Value = restarted
            .request(Method::POST, "models/payments/explore")
            .json(&json!({
                "model_revision": rev1, "neighbors": true,
                "element": { "kind": "node", "label": "Account", "key": ["1", "9007199254740993"] },
                "graph_options": {}, "filter": filter, "cursor": first_cursor,
                "search": search,
            }))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        assert_eq!(filtered["total"], 0);
    }
    let found: Value = neighbor_page(
        filter.clone(),
        search["matches"][0]["cursor"].clone(),
        versions.clone(),
    )
    .send()
    .await?
    .error_for_status()?
    .json()
    .await?;
    assert!(
        found["graph"]["elements"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["id"] == search["matches"][0]["id"])
    );
    let second: Value = neighbor_page(filter.clone(), first_cursor.clone(), versions.clone())
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(second["page_number"], 2);
    let first: Value = neighbor_page(
        filter.clone(),
        second["previous_cursor"].clone(),
        versions.clone(),
    )
    .send()
    .await?
    .error_for_status()?
    .json()
    .await?;
    assert_eq!(first["page_number"], 1);
    assert_eq!(first["next_cursor"], first_cursor);
    let incoming: Value = neighbor_page(json!({"direction":"in"}), Value::Null, versions.clone())
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(incoming["graph"]["elements"].as_array().unwrap().len(), 1);
    assert_eq!(
        neighbor_page(
            json!({"direction":"in"}),
            first_cursor.clone(),
            versions.clone()
        )
        .send()
        .await?
        .status(),
        StatusCode::UNPROCESSABLE_ENTITY
    );

    // The same key has moved to another account in the preferred mapping.
    // Filtering each branch by endpoint before resolving priority would show the stale edge.
    pg.batch_execute(&format!(
        "CREATE TABLE {schema}.preferred_payments
         (tenant_id BIGINT, payment_id BIGINT, from_account_id BIGINT, to_merchant_id BIGINT, amount NUMERIC(12,3));
         INSERT INTO {schema}.preferred_payments VALUES (1, 1001, 1, 7, 13.001)"
    )).await?;
    let preferred_yaml = yaml.replace("    source: lake.finance.payments", &format!(
        "    inputs:\n      - table: ledger.{schema}.preferred_payments\n      - table: lake.finance.payments\n    on_conflict: prefer_first"
    ));
    fs::write(root.path().join("preferred.yaml"), &preferred_yaml)?;
    let preferred = restarted.model("preferred").await?;
    let preferred_page =
        |filter: Value| {
            restarted.request(Method::POST, "models/preferred/explore").json(&json!({
            "model_revision": preferred["active_revision"], "neighbors": true,
            "element": { "kind": "node", "label": "Account", "key": ["1", "9007199254740993"] },
            "graph_options": {}, "filter": filter,
        }))
        };
    let preferred_result: Value = preferred_page(filter.clone())
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let preferred_keys: Vec<_> = preferred_result["graph"]["elements"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|item| item["kind"] == "edge")
        .map(|item| item["key"][1].as_str().unwrap())
        .collect();
    assert_eq!(
        preferred_keys,
        (1002..=1051).map(|key| key.to_string()).collect::<Vec<_>>()
    );

    payments_file(&replacement, false, &[1200])?;
    fs::rename(&replacement, &payments)?;
    for (cursor, sources) in [(first_cursor, json!({})), (Value::Null, versions)] {
        let reply = neighbor_page(filter.clone(), cursor, sources)
            .send()
            .await?;
        assert_eq!(reply.status(), StatusCode::CONFLICT);
        assert_eq!(
            reply.json::<Value>().await?["code"],
            "graph_sources_changed"
        );
    }
    let latest: Value = neighbor_page(filter.clone(), Value::Null, json!({}))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let relation = format!("ledger.{schema}.accounts");
    let ledger = json!({ relation.clone(): latest["sources"].as_array().unwrap().iter()
        .find(|source| source["source"] == relation).unwrap()["version"] });
    pg.execute(
        &format!(
            "UPDATE {schema}.accounts SET status = 'updated' WHERE account_id = 9007199254740993"
        ),
        &[],
    )
    .await?;
    assert_eq!(
        neighbor_page(filter, Value::Null, ledger)
            .send()
            .await?
            .status(),
        StatusCode::CONFLICT
    );
    let refreshed: Value = restarted
        .query(
            Uuid::new_v4(),
            "cypher",
            "MATCH (a:Account)-[e:Payment]->(:Merchant) RETURN a.status, e.payment_id",
        )
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(refreshed["rows"], json!([["updated", "1200"]]));

    let keyless_yaml = yaml.replace("    key: [tenant_id, payment_id]\n", "");
    fs::write(root.path().join("keyless.yaml"), keyless_yaml)?;
    payments_file(&replacement, false, &[8; 121])?;
    fs::rename(&replacement, &payments)?;
    let keyless = restarted.model("keyless").await?;
    let keyless_request =
        |cursor: Value, filter: Value| {
            restarted.request(Method::POST, "models/keyless/explore").json(&json!({
            "model_revision": keyless["active_revision"], "neighbors": true,
            "element": { "kind": "node", "label": "Account", "key": ["1", "9007199254740993"] },
            "graph_options": {}, "cursor": cursor, "filter": filter,
        }))
        };
    let keyless_page = |cursor| keyless_request(cursor, json!({}));
    let mut cursor = Value::Null;
    let mut first_page = Value::Null;
    let mut second_page = Value::Null;
    let mut edge_ids = std::collections::BTreeSet::new();
    for (index, expected) in [50, 50, 21].into_iter().enumerate() {
        let page: Value = keyless_page(cursor)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        if index == 0 {
            first_page = page.clone();
        } else if index == 1 {
            second_page = page.clone();
        }
        let edges = page["graph"]["elements"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|item| item["kind"] == "edge")
            .collect::<Vec<_>>();
        assert_eq!(edges.len(), expected);
        for edge in edges {
            assert_eq!(edge["properties"]["amount"], "12.250");
            assert!(edge_ids.insert(edge["id"].as_str().unwrap().to_owned()));
        }
        cursor = page["next_cursor"].clone();
    }
    assert!(cursor.is_null());
    let repeated: Value = keyless_page(first_page["page_cursor"].clone())
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(repeated["graph"], first_page["graph"]);
    let restarted_expansion: Value = keyless_page(Value::Null)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(restarted_expansion["graph"], first_page["graph"]);
    let evict = || async {
        for value in 0..44 {
            let reply = keyless_request(
                Value::Null,
                json!({"direction":"out", "edge_label":"Payment", "properties":[
                    {"column":"amount", "op":"ge", "value":format!("{value}.000")}
                ]}),
            )
            .send()
            .await?;
            if !reply.status().is_success() {
                anyhow::bail!("evicting neighborhood {value}: {}", reply.text().await?);
            }
        }
        Ok::<_, anyhow::Error>(())
    };
    evict().await?;
    let recovered: Value = keyless_page(first_page["next_cursor"].clone())
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(recovered["page_number"], 2);
    assert_eq!(recovered["graph"], second_page["graph"]);
    assert_eq!(recovered["next_cursor"], second_page["next_cursor"]);
    assert_eq!(recovered["page_cursor"], second_page["page_cursor"]);
    let recovered_first: Value = keyless_page(recovered["previous_cursor"].clone())
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(recovered_first["graph"], first_page["graph"]);
    evict().await?;
    pg.execute(
        &format!("UPDATE {schema}.accounts SET status = 'second version' WHERE account_id = 9007199254740993"),
        &[],
    ).await?;
    let changed = keyless_page(first_page["next_cursor"].clone())
        .send()
        .await?;
    assert_eq!(changed.status(), StatusCode::CONFLICT);
    assert_eq!(
        changed.json::<Value>().await?["code"],
        "graph_sources_changed"
    );
    pg.batch_execute(include_str!("../examples/postgres-relation-versions.sql"))
        .await?;
    for table in ["accounts", "merchants"] {
        pg.execute(
            "SELECT graphlake_tracking.watch($1::text::regclass)",
            &[&format!("{schema}.{table}")],
        )
        .await?;
    }
    let tracked: Value = keyless_page(Value::Null)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let accounts = format!("ledger.{schema}.accounts");
    assert!(tracked["sources"].as_array().unwrap().iter().any(|source| {
        source["source"] == accounts
            && source["version"]
                .as_str()
                .is_some_and(|version| version.starts_with("tracked:"))
    }));
    for _ in 0..3 {
        pg.execute(&format!("UPDATE {schema}.unrelated SET id = id + 1"), &[])
            .await?;
        let page: Value = keyless_page(tracked["next_cursor"].clone())
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        assert_eq!(page["page_number"], 2);
    }
    pg.batch_execute(&format!(
        "BEGIN; UPDATE {schema}.accounts SET status = 'rolled back'; ROLLBACK"
    ))
    .await?;
    keyless_page(tracked["next_cursor"].clone())
        .send()
        .await?
        .error_for_status()?;
    let (writer, driver) = tokio_postgres::connect(&dsn, NoTls).await?;
    let writer_connection = tokio::spawn(driver);
    writer
        .batch_execute(&format!(
            "BEGIN; UPDATE {schema}.accounts SET status = 'held write' WHERE account_id = 1"
        ))
        .await?;
    pg.batch_execute(&format!("SET statement_timeout = '750ms'; UPDATE {schema}.accounts SET status = 'first commit' WHERE account_id = 2; RESET statement_timeout")).await?;
    let stale = keyless_page(tracked["next_cursor"].clone()).send().await?;
    assert_eq!(stale.status(), StatusCode::CONFLICT);
    let tracked: Value = keyless_page(Value::Null)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    writer
        .batch_execute(&format!(
            "UPDATE {schema}.accounts SET status = 'held write' WHERE account_id = 2; COMMIT"
        ))
        .await?;
    writer_connection.abort();
    let stale = keyless_page(tracked["next_cursor"].clone()).send().await?;
    assert_eq!(stale.status(), StatusCode::CONFLICT);
    let tracked: Value = keyless_page(Value::Null)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    pg.execute(
        "SELECT graphlake_tracking.compact($1::text::regclass)",
        &[&format!("{schema}.accounts")],
    )
    .await?;
    keyless_page(tracked["next_cursor"].clone())
        .send()
        .await?
        .error_for_status()?;
    pg.batch_execute(&format!("SET session_replication_role = replica; UPDATE {schema}.accounts SET status = 'replica write' WHERE account_id = 9007199254740993; RESET session_replication_role")).await?;
    assert_eq!(
        keyless_page(tracked["next_cursor"].clone())
            .send()
            .await?
            .status(),
        StatusCode::CONFLICT
    );
    let tracked: Value = keyless_page(Value::Null)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    pg.batch_execute(&format!(
        "ALTER TABLE {schema}.accounts DISABLE TRIGGER graphlake_relation_version"
    ))
    .await?;
    assert_eq!(
        keyless_page(tracked["next_cursor"].clone())
            .send()
            .await?
            .status(),
        StatusCode::CONFLICT
    );
    for table in ["relation_changes", "relation_versions"] {
        pg.execute(&format!("DELETE FROM graphlake_tracking.{table} WHERE relation IN ($1::text::regclass, $2::text::regclass)"),
            &[&format!("{schema}.accounts"), &format!("{schema}.merchants")]).await?;
    }
    #[cfg(unix)]
    {
        pg.batch_execute(&format!(
            "BEGIN; LOCK TABLE {schema}.accounts IN ACCESS EXCLUSIVE MODE"
        ))
        .await?;
        let pid = restarted.child.id().try_into()?;
        let (query, terminated) = tokio::join!(
            restarted.query(Uuid::new_v4(), "sql", "SELECT * FROM \"Account\""),
            async {
                for _ in 0..50 {
                    let body = restarted
                        .http
                        .get(format!("{}/metrics", restarted.base))
                        .send()
                        .await?
                        .error_for_status()?
                        .text()
                        .await?;
                    if body
                        .lines()
                        .any(|line| line == "graphlake_active_queries 1")
                    {
                        let status = unsafe { libc::kill(pid, libc::SIGTERM) };
                        ensure!(status == 0, "sending SIGTERM failed");
                        return Ok(());
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                anyhow::bail!("blocked query did not occupy a service slot")
            }
        );
        terminated?;
        assert_eq!(query?.status(), StatusCode::CONFLICT);
        let stopped = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if let Some(status) = restarted.child.try_wait()? {
                    return Ok::<_, std::io::Error>(status);
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await??;
        assert!(stopped.success(), "{stopped}");
        pg.batch_execute("ROLLBACK").await?;
    }
    pg.batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
        .await?;
    connection.abort();
    Ok(())
}

#[tokio::test]
#[ignore = "requires a loopback etcd v3 gateway in GRAPHLAKE_TEST_ETCD"]
async fn cluster_queries_survive_coordinator_restart_and_worker_loss() -> Result<()> {
    let root = tempfile::tempdir()?;
    setup_assets(root.path())?;
    setup_credentials(root.path())?;
    parquet_file(&root.path().join("payments.parquet"), false)?;
    let extra = root
        .path()
        .join(".graphlake-local")
        .join(format!("{}.parquet", Uuid::new_v4()));
    let parts = extra.with_extension("parts");
    fs::create_dir_all(&parts)?;
    let base = Uuid::new_v4();
    let delta = Uuid::new_v4();
    payments_file(&parts.join(format!("{base}.parquet")), false, &[9, 9])?;
    payments_file(&parts.join(format!("{delta}.parquet")), false, &[9])?;
    fs::write(
        &extra,
        format!(
            "GLPQ1\n{}",
            json!({"generation": Uuid::new_v4(), "parts": [{"id": base, "rows": 2, "deleted": [0, 1]}, {"id": delta, "rows": 1}]})
        ),
    )?;
    fs::write(
        root.path().join("payments.yaml"),
        r#"
graph: payments
sources:
  lake: {type: parquet, tables: {finance.nodes: [payments.parquet], finance.payments: [payments.parquet, payments-extra.parquet]}}
nodes:
  Account: {source: lake.finance.nodes, key: [tenant_id, from_account_id]}
  Merchant: {source: lake.finance.nodes, key: [tenant_id, to_merchant_id]}
edges:
  Payment:
    source: lake.finance.payments
    key: [tenant_id, payment_id]
    from: {node: Account, columns: [tenant_id, from_account_id]}
    to: {node: Merchant, columns: [tenant_id, to_merchant_id]}
    properties: [amount, payment_id, score, channel]
"#.replace("payments-extra.parquet", extra.to_str().unwrap()),
    )?;
    let config = root.path().join("cluster.yaml");
    let keyless = fs::read_to_string(root.path().join("payments.yaml"))?
        .replace("    key: [tenant_id, payment_id]\n", "")
        .replace(
            "  Account: {source: lake.finance.nodes, key: [tenant_id, from_account_id]}",
            "  Account:\n    key: [tenant_id, from_account_id]\n    inputs: [{table: lake.finance.nodes}, {table: lake.finance.payments}]",
        );
    fs::write(root.path().join("keyless.yaml"), keyless)?;
    let temporal = RecordBatch::try_from_iter([
        ("id", Arc::new(Int64Array::from(vec![1, 2])) as _),
        ("src", Arc::new(Int64Array::from(vec![1, 2])) as _),
        ("dst", Arc::new(Int64Array::from(vec![2, 1])) as _),
        (
            "starts",
            Arc::new(graphlake::arrow::array::Date32Array::from(vec![0, 0])) as _,
        ),
        (
            "ends",
            Arc::new(graphlake::arrow::array::Date32Array::from(vec![
                Some(3),
                None,
            ])) as _,
        ),
        (
            "business_start",
            Arc::new(Int64Array::from(vec![0, 0])) as _,
        ),
        (
            "business_end",
            Arc::new(Int64Array::from(vec![Some(3), None])) as _,
        ),
    ])?;
    let mut writer = ArrowWriter::try_new(
        fs::File::create(root.path().join("temporal.parquet"))?,
        temporal.schema(),
        None,
    )?;
    writer.write(&temporal)?;
    writer.close()?;
    let temporal_model = r#"
graph: temporal
sources:
  store: {type: parquet, tables: {snapshots: [temporal.parquet]}}
nodes:
  Account: {source: store.snapshots, key: [id]}
edges:
  Link:
    source: store.snapshots
    key: [id]
    from: {node: Account, columns: [src]}
    to: {node: Account, columns: [dst]}
    scd2: {valid_from: starts, valid_to: ends}
"#;
    fs::write(root.path().join("temporal.yaml"), temporal_model)?;
    fs::write(
        root.path().join("business.yaml"),
        temporal_model.replace(
            "valid_from: starts, valid_to: ends",
            "valid_from: business_start, valid_to: business_end",
        ),
    )?;
    let store = root.path().join("store");
    fs::create_dir(&store)?;
    let etcd = std::env::var("GRAPHLAKE_TEST_ETCD")?;
    let name = format!("test-{}", Uuid::new_v4().simple());
    let memory_mib =
        std::env::var("GRAPHLAKE_TEST_CLUSTER_MEMORY_MIB").unwrap_or_else(|_| "32".into());
    let config_text = |workers: &[&str]| {
        format!(
            "name: {name}\nmetadata: {etcd}\nstorage: {}\ntoken_env: GRAPHLAKE_CLUSTER_TEST_TOKEN\nworkers: {}\ndistributed_partitions: 2\nmemory_mib: {memory_mib}\n",
            store.display(),
            serde_json::to_string(workers).unwrap()
        )
    };
    let listeners = (0..2)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0"))
        .collect::<std::io::Result<Vec<_>>>()?;
    let addresses = listeners
        .iter()
        .map(|listener| Ok(format!("http://{}", listener.local_addr()?)))
        .collect::<std::io::Result<Vec<_>>>()?;
    fs::write(
        &config,
        config_text(&addresses.iter().map(String::as_str).collect::<Vec<_>>()),
    )?;
    let mut workers = Vec::new();
    for listener in listeners {
        let port = listener.local_addr()?.port().to_string();
        drop(listener);
        let mut child = Command::new(env!("CARGO_BIN_EXE_graphlake-admin"))
            .args([
                "--cluster-config",
                config.to_str().unwrap(),
                "--port",
                &port,
                "worker",
                "--source-dir",
            ])
            .arg(root.path())
            .env("GRAPHLAKE_CLUSTER_TEST_TOKEN", PASSWORD.repeat(2))
            .stderr(Stdio::piped())
            .spawn()?;
        let mut ready = String::new();
        BufReader::new(child.stderr.take().unwrap()).read_line(&mut ready)?;
        let address = ready
            .trim()
            .strip_prefix("GraphLake worker ")
            .context("worker startup failed")?;
        workers.push(Server {
            child,
            base: address.into(),
            http: Client::new(),
            cookie: None,
        });
    }
    let arguments = ["--cluster-config", config.to_str().unwrap()];
    let mut coordinator = Server::start_with(root.path(), "", 0, &arguments)?;
    coordinator.login().await?;
    let readiness = || coordinator.request(Method::GET, "models/payments/readiness");
    let ready = readiness().send().await?;
    assert_eq!(ready.status(), StatusCode::OK, "{}", ready.text().await?);
    let query = "MATCH (a:Account)-[r:Payment]->(b:Merchant) RETURN a,r,b ORDER BY r.payment_id";
    let first_id = Uuid::new_v4();
    let first: Value = coordinator
        .query(first_id, "cypher", query)
        .await?
        .error_for_status()?
        .json()
        .await?;
    let second: Value = coordinator
        .query(Uuid::new_v4(), "cypher", query)
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_ne!(first["execution_worker"], second["execution_worker"]);
    assert_eq!(first["rows"], second["rows"]);
    assert_eq!(first["graph"]["elements"].as_array().unwrap().len(), 4);
    let outer = r#"SELECT p.payment_id, a.from_account_id FROM "Payment" p
        LEFT JOIN "Account" a ON a.from_account_id < p.payment_id ORDER BY p.payment_id"#;
    let preserved: Value = coordinator
        .query(Uuid::new_v4(), "sql", outer)
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(preserved["rows"], json!([["8", null], ["9", null]]));
    let aggregate = r#"SELECT count(*) AS n, sum(amount) AS total FROM "Payment""#;
    let distributed: Value = coordinator
        .query(Uuid::new_v4(), "sql", aggregate)
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(distributed["rows"], json!([["2", "24.259"]]));
    assert_eq!(
        distributed["distributed"]["workers"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert!(
        distributed["distributed"]["remote_partitions"]
            .as_u64()
            .unwrap()
            > 0
    );
    for sql in [
        r#"SELECT a.from_account_id AS id FROM "Account" a
            LEFT JOIN "Payment" p ON a.from_account_id = p.from_account_id AND p.payment_id < 0"#,
        r#"SELECT a.from_account_id AS id FROM "Account" a
            LEFT ANTI JOIN "Payment" p ON a.from_account_id = p.from_account_id AND p.payment_id < 0"#,
    ] {
        let result: Value = coordinator
            .query(Uuid::new_v4(), "sql", sql)
            .await?
            .error_for_status()?
            .json()
            .await?;
        assert_eq!(result["rows"], json!([["9007199254740993"]]), "{sql}");
    }
    let csr = |kind: Value, generation: &Value, revision: &Value| {
        json!({
            "request_id": Uuid::new_v4(),
            "model_revision": revision,
            "language": "cypher",
            "csr": {"generation": generation, "operation": kind}
        })
    };
    let revision = coordinator.model("payments").await?["active_revision"].clone();
    let publish = |server: &Server| {
        server
            .request(Method::POST, "models/payments/csr")
            .json(&json!({"model_revision": revision}))
    };
    let keyed_csr: Value = publish(&coordinator)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let gremlin = "g.V().hasLabel('Account').out('Payment').count()";
    let live: Value = coordinator
        .query(Uuid::new_v4(), "gremlin", gremlin)
        .await?
        .error_for_status()?
        .json()
        .await?;
    let snapshot: Value = coordinator
        .request(Method::POST, "models/payments/query")
        .json(&json!({
            "request_id": Uuid::new_v4(), "language":"gremlin", "query":gremlin,
            "model_revision":revision,"csr_generation":keyed_csr["generation"],
        }))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(snapshot["rows"], live["rows"]);
    assert_eq!(snapshot["csr_generation"], keyed_csr["generation"]);
    let temporal_revision = coordinator.model("temporal").await?["active_revision"].clone();
    let temporal_query = |generation: Value| {
        coordinator
            .request(Method::POST, "models/temporal/query")
            .json(&json!({
                "request_id": Uuid::new_v4(), "language":"cypher",
                "query":"MATCH (:Account)-[e:Link]->(:Account) RETURN e.id AS id ORDER BY id",
                "model_revision":temporal_revision, "csr_generation":generation,
            }))
    };
    for (scope, expected) in [
        (json!({}), None),
        (
            json!({"snapshot_time":"1970-01-02"}),
            Some(json!([["1"], ["2"]])),
        ),
    ] {
        let saved: Value = coordinator
            .request(Method::POST, "models/temporal/csr")
            .json(&json!({"model_revision":temporal_revision,"graph_options":scope}))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        let result = temporal_query(saved["generation"].clone()).send().await?;
        if let Some(expected) = expected {
            assert_eq!(
                result.error_for_status()?.json::<Value>().await?["rows"],
                expected
            );
        } else {
            assert_eq!(result.status(), StatusCode::CONFLICT);
            assert_eq!(
                result.json::<Value>().await?["code"],
                "csr_current_validity"
            );
        }
    }
    let live_temporal: Value = temporal_query(Value::Null)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(live_temporal["rows"], json!([["2"]]));
    let business_revision = coordinator.model("business").await?["active_revision"].clone();
    let business: Value = coordinator
        .request(Method::POST, "models/business/csr")
        .json(&json!({"model_revision":business_revision}))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let result: Value = coordinator
        .request(Method::POST, "models/business/query")
        .json(&json!({
            "request_id": Uuid::new_v4(), "language":"cypher",
            "query":"MATCH (:Account)-[e:Link]->(:Account) RETURN e.id AS id",
            "model_revision":business_revision, "csr_generation":business["generation"],
        }))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(result["rows"], json!([["2"]]));
    for cypher in [
        "MATCH (m:Merchant) WHERE m.tenant_id = 1 AND m.to_merchant_id = 7 WITH m CALL (m) { MATCH (m)-[:Payment*1..2]-(a:Account) RETURN a.from_account_id AS account } RETURN account ORDER BY account",
        "MATCH (m:Merchant) WHERE m.tenant_id = 1 AND m.to_merchant_id = 7 WITH m CALL (m) { MATCH (m)-[*1..2]-(a:Account) RETURN a.from_account_id AS account } RETURN account ORDER BY account",
        "MATCH p=(a:Account {tenant_id:1,from_account_id:9007199254740993})-[:Payment*1..2]->(m:Merchant) RETURN length(p) AS hops ORDER BY hops",
        "MATCH (a:Account {tenant_id:1,from_account_id:9007199254740993})-[:Payment*1..2]->(m:Merchant) RETURN count(*) AS hits",
        "MATCH p=(a:Account {tenant_id:1,from_account_id:9007199254740993})-[:Payment*0..0]-(b:Account {tenant_id:1,from_account_id:9007199254740993}) RETURN length(p) AS hops, [e IN relationships(p) | id(e)] AS edges, [n IN nodes(p) | id(n)] AS vertices",
        "MATCH p=(a:Account {tenant_id:1,from_account_id:9007199254740993})-[r:Payment*1..2]-(m:Merchant) RETURN length(p) AS hops, [e IN relationships(p) | e.amount] AS amounts ORDER BY amounts",
        "MATCH p=(a:Account {tenant_id:1,from_account_id:9007199254740993})-[:Payment*0..2]-(b:Account {tenant_id:1,from_account_id:9007199254740993}) RETURN length(p) AS hops ORDER BY hops",
        "MATCH p=allShortestPaths((a:Account {tenant_id:1,from_account_id:9007199254740993})-[r:Payment*1..2]->(m:Merchant {tenant_id:1,to_merchant_id:7})) RETURN length(p) AS hops, [e IN relationships(p) | e.amount] AS amounts ORDER BY amounts",
        "MATCH p=allShortestPaths((a:Account {tenant_id:1,from_account_id:9007199254740993})-[:Payment*1..2 {amount: 12.25}]->(m:Merchant {tenant_id:1,to_merchant_id:7})) RETURN [e IN relationships(p) | id(e)] AS ids, [n IN nodes(p) | n.tenant_id] AS tenants",
        "MATCH p=allShortestPaths((a:Account {tenant_id:1,from_account_id:9007199254740993})-[:Payment*1..2 {score: 0.0, channel: 'web-long-字符串'}]->(m:Merchant {tenant_id:1,to_merchant_id:7})) UNWIND relationships(p) AS e RETURN id(e), e.score, e.channel",
        "MATCH p=(a:Account {tenant_id:1,from_account_id:9007199254740993})-[:Payment*1..2]->(m:Merchant) UNWIND relationships(p) AS e RETURN id(e) AS rid, e.amount AS amount ORDER BY id(e) LIMIT 1",
        "MATCH p=allShortestPaths((a:Account {tenant_id:1,from_account_id:9007199254740993})-[*1..2]->(m:Merchant {tenant_id:1,to_merchant_id:7})) UNWIND relationships(p) AS e RETURN elementId(e) AS rid, e.channel AS channel ORDER BY rid DESC SKIP 1 LIMIT 1",
        "MATCH p=(a:Account {tenant_id:1,from_account_id:9007199254740993})-[:Payment*1..2]->(m:Merchant) UNWIND relationships(p) AS e WITH e, e.amount AS amount ORDER BY amount DESC, id(e) SKIP 1 LIMIT 1 WHERE amount > 12.0 RETURN id(e) AS rid, amount, e.channel AS channel",
        "MATCH p=allShortestPaths((a:Account {tenant_id:1,from_account_id:9007199254740993})-[*1..2]->(m:Merchant {tenant_id:1,to_merchant_id:7})) UNWIND relationships(p) AS e WITH e AS r, e.channel AS channel RETURN id(r) AS rid, r.amount AS amount, channel ORDER BY amount, rid DESC LIMIT 1",
        "MATCH p=(a:Account {tenant_id:1,from_account_id:9007199254740993})-[:Payment*1..2]-(m:Merchant) UNWIND relationships(p) AS e RETURN DISTINCT id(e) AS rid, e.amount AS amount, e.channel AS channel ORDER BY amount, rid DESC SKIP 1 LIMIT 1",
        "MATCH p=(a:Account {tenant_id:1,from_account_id:9007199254740993})-[:Payment*1..2]-(m:Merchant) UNWIND relationships(p) AS e WITH e AS r, count(*) AS hits, sum(e.amount) AS total, e.channel AS channel ORDER BY total DESC, id(r) LIMIT 1 WHERE hits > 0 RETURN id(r) AS rid, total, channel, r.score AS score",
        "MATCH p=allShortestPaths((a:Account {tenant_id:1,from_account_id:9007199254740993})-[*1..2]->(m:Merchant {tenant_id:1,to_merchant_id:7})) UNWIND relationships(p) AS e WITH DISTINCT e RETURN id(e) AS rid, count(*) AS hits, e.channel AS channel ORDER BY hits DESC, rid LIMIT 2",
        "MATCH p=(a:Account {tenant_id:1,from_account_id:9007199254740993})-[:Payment*1..2]-(m:Merchant) UNWIND relationships(p) AS e RETURN e.amount AS amount, count(DISTINCT e) AS hits ORDER BY amount, hits DESC LIMIT 2",
        "MATCH p=allShortestPaths((a:Account {tenant_id:1,from_account_id:9007199254740993})-[:Payment*1..2 {amount: -1}]->(m:Merchant {tenant_id:1,to_merchant_id:7})) RETURN length(p) AS hops",
        "MATCH p=allShortestPaths((a:Account {tenant_id:1,from_account_id:9007199254740993})-[*1..2 {amount: '12.250'}]->(m:Merchant {tenant_id:1,to_merchant_id:7})) RETURN length(p) AS hops",
        "MATCH p=shortestPath((a:Account {tenant_id:1,from_account_id:9007199254740993})-[*1..2]->(m:Merchant)) RETURN length(p) AS hops",
    ] {
        let live: Value = coordinator
            .query(Uuid::new_v4(), "cypher", cypher)
            .await?
            .error_for_status()?
            .json()
            .await?;
        let response = coordinator
            .request(Method::POST, "models/payments/query")
            .json(&json!({
                "request_id": Uuid::new_v4(), "language":"cypher", "query":cypher,
                "model_revision":revision,"csr_generation":keyed_csr["generation"]
            }))
            .send()
            .await?;
        assert!(
            response.status().is_success(),
            "{cypher}: {}",
            response.text().await?
        );
        let result: Value = response.json().await?;
        assert_eq!(result["rows"], live["rows"], "{cypher}");
        if cypher.contains("*0..0") {
            assert_eq!(result["csr_stats"]["rounds"], 0);
        } else if !cypher.contains("hortestPath") {
            assert_eq!(result["csr_stats"]["remote_partitions"], 0, "{result}");
            assert_eq!(result["csr_stats"]["exchange_bytes"], 0, "{result}");
        }
        if cypher.contains("{amount:") {
            assert!(
                result["csr_stats"]["filtered_arcs"].as_u64().unwrap() > 0,
                "{result}"
            );
        }
        if cypher.contains("ORDER BY amount,")
            || cypher.contains("ORDER BY amount DESC,")
            || cypher.contains("ORDER BY total DESC,")
        {
            assert!(
                result["csr_stats"]["sort_column_bytes"]
                    .as_u64()
                    .unwrap_or(0)
                    > 0,
                "{result}"
            );
        }
        if cypher.contains("{amount: 12.25}") {
            assert!(
                result["csr_stats"]["filter_column_pages"]
                    .as_u64()
                    .unwrap_or_default()
                    > 0,
                "{result}"
            );
        }
        if cypher.contains("channel: 'web-long-字符串'") {
            assert!(
                result["csr_stats"]["filter_column_pages"]
                    .as_u64()
                    .unwrap_or_default()
                    >= 2,
                "{result}"
            );
        }
        if cypher.contains("{amount: -1}") {
            assert!(
                result["csr_stats"]["skipped_property_blocks"]
                    .as_u64()
                    .unwrap_or_default()
                    > 0,
                "{result}"
            );
        }
    }
    let empty_page: Value = coordinator
        .request(Method::POST, "models/payments/query")
        .json(&json!({
            "request_id": Uuid::new_v4(), "language":"cypher",
            "query": "MATCH p=(:Account)-[:Payment*1..2]->(:Merchant) UNWIND relationships(p) AS e RETURN e.amount ORDER BY id(e) LIMIT 0",
            "model_revision":revision,"csr_generation":keyed_csr["generation"]
        }))
        .send().await?.error_for_status()?.json().await?;
    assert_eq!(empty_page["rows"], json!([]));
    assert_eq!(empty_page["csr_stats"]["remote_partitions"], 0);
    for operation in [
        json!({"kind":"pagerank","damping":0.85,"tolerance":0.000000001,"max_iterations":4}),
        json!({"kind":"wcc","max_iterations":4}),
    ] {
        let response: Value = coordinator
            .request(Method::POST, "models/payments/query")
            .json(&csr(operation, &keyed_csr["generation"], &revision))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        assert_eq!(response["rows"].as_array().unwrap().len(), 2);
        assert!(response["csr_stats"]["remote_partitions"].as_u64().unwrap() > 0);
        assert!(
            response["csr_stats"]["state_exchange_bytes"]
                .as_u64()
                .unwrap()
                > 0
        );
    }
    let keyless_revision = coordinator.model("keyless").await?["active_revision"].clone();
    let first_csr: Value = coordinator
        .request(Method::POST, "models/keyless/csr")
        .json(&json!({"model_revision":keyless_revision}))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let internal = coordinator
        .http
        .post(format!("{}/internal/v1/jobs", workers[0].base))
        .send()
        .await?;
    assert_eq!(internal.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        coordinator
            .guest(Method::GET, &format!("queries/{first_id}"))
            .send()
            .await?
            .status(),
        StatusCode::UNAUTHORIZED
    );
    drop(coordinator);
    let mut coordinator = Server::start_with(root.path(), "", 0, &arguments)?;
    coordinator.login().await?;
    let resumed: Value = coordinator
        .request(Method::GET, &format!("queries/{first_id}"))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(first, resumed);
    let arrow = coordinator
        .request(
            Method::GET,
            &format!("models/payments/results/{}", first_id),
        )
        .send()
        .await?
        .error_for_status()?
        .bytes()
        .await?;
    assert!(!arrow.is_empty());
    workers.remove(0);
    let degraded = coordinator
        .request(Method::GET, "models/payments/readiness")
        .send()
        .await?;
    assert_eq!(degraded.status(), StatusCode::SERVICE_UNAVAILABLE);
    let degraded = degraded.json::<Value>().await?;
    assert!(degraded["workers"][0]["probe"].is_null(), "{degraded}");
    assert_eq!(degraded["workers"][1]["probe"]["status"], "ready");
    let third: Value = coordinator
        .query(Uuid::new_v4(), "cypher", query)
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(third["execution_worker"], second["execution_worker"]);
    assert_eq!(third["graph"], first["graph"]);
    assert_eq!(first_csr["vertices"], 2);
    assert_eq!(first_csr["edges"], 2);
    let op = json!({
        "kind": "all_shortest",
        "start": {"label":"Account", "key":["1", "9007199254740993"]},
        "target": {"label":"Merchant", "key":["1", "7"]},
        "max_hops": 1, "max_paths": 2
    });
    let response: Value = coordinator
        .request(Method::POST, "models/keyless/query")
        .json(&csr(
            op.clone(),
            &first_csr["generation"],
            &keyless_revision,
        ))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(response["csr_generation"], first_csr["generation"]);
    let rows = response["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 2);
    let keys = rows
        .iter()
        .map(|row| -> Result<Value> {
            let edges: Value = serde_json::from_str(row[2].as_str().context("missing edges")?)?;
            Ok(edges[0]["identity"]["key"].clone())
        })
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(keys, [json!(["1"]), json!(["2"])]);
    assert!(response["csr_stats"]["retries"].as_u64().unwrap() > 0);
    let keyless_paths: Value = coordinator
        .request(Method::POST, "models/keyless/query")
        .json(&json!({
            "request_id": Uuid::new_v4(),
            "model_revision": keyless_revision,
            "language": "cypher",
            "csr_generation": first_csr["generation"],
            "query": "MATCH p=allShortestPaths((a:Account {tenant_id:1,from_account_id:9007199254740993})-[:Payment*1..2]->(m:Merchant {tenant_id:1,to_merchant_id:7})) UNWIND relationships(p) AS edge RETURN count(DISTINCT id(edge)) AS edges, sum(edge.amount) AS amount"
        }))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(keyless_paths["rows"], json!([["2", "24.259"]]));
    let explore_keyless = |element: Value, neighbors: bool| {
        coordinator
            .request(Method::POST, "models/keyless/explore")
            .json(&json!({
                "model_revision": keyless_revision, "csr_generation": first_csr["generation"],
                "graph_options": {}, "neighbors": neighbors, "element": element,
            }))
    };
    let neighbors: Value = explore_keyless(
        json!({"kind": "node", "label": "Account", "key": ["1", "9007199254740993"]}),
        true,
    )
    .send()
    .await?
    .error_for_status()?
    .json()
    .await?;
    let edges = neighbors["graph"]["elements"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|element| element["kind"] == "edge")
        .collect::<Vec<_>>();
    assert_eq!(edges.len(), 2);
    assert_ne!(edges[0]["key"], edges[1]["key"]);
    for edge in edges {
        let detail: Value = explore_keyless(
            json!({"kind": "edge", "label": "Payment", "key": edge["key"]}),
            false,
        )
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
        assert_eq!(detail["selected"]["id"], edge["id"]);
        assert_eq!(detail["selected"]["properties"], edge["properties"]);
        assert_eq!(detail["selected"]["transient"], true);
    }
    let refreshed: Value = publish(&coordinator)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_ne!(keyed_csr["generation"], refreshed["generation"]);
    let explore =
        |cursor: Option<&Value>| {
            coordinator.request(Method::POST, "models/payments/explore").json(&json!({
            "model_revision": revision, "csr_generation": keyed_csr["generation"],
            "graph_options": {}, "neighbors": true,
            "element": {"kind": "node", "label": "Account", "key": ["1", "9007199254740993"]},
            "filter": {"direction": "out", "edge_label": "Payment"},
            "cursor": cursor
        }))
        };
    let old_graph: Value = explore(None)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(old_graph["selected"]["label"], "Account");
    assert!(
        old_graph["graph"]["elements"]
            .as_array()
            .unwrap()
            .iter()
            .any(|element| element["kind"] == "edge" && element["label"] == "Payment")
    );
    let replay_graph: Value = explore(Some(&old_graph["page_cursor"]))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(old_graph["graph"], replay_graph["graph"]);
    let missing = coordinator
        .request(Method::POST, "models/payments/explore")
        .json(&json!({
            "model_revision": revision, "csr_generation": Uuid::new_v4(),
            "graph_options": {}, "neighbors": false,
            "element": {"kind": "node", "label": "Account", "key": ["1", "9007199254740993"]}
        }))
        .send()
        .await?;
    assert_eq!(missing.status(), StatusCode::CONFLICT);
    let missing: Value = missing.json().await?;
    assert_eq!(missing["code"], "csr_snapshot_expired");
    assert_eq!(
        coordinator
            .request(Method::POST, "models/payments/query")
            .json(&csr(op, &keyed_csr["generation"], &revision))
            .send()
            .await?
            .status(),
        StatusCode::CONFLICT
    );
    let replay: Value = coordinator
        .query(Uuid::new_v4(), "sql", aggregate)
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(replay["rows"], distributed["rows"]);
    assert!(replay["distributed"]["partition_retries"].as_u64().unwrap() > 0);
    let pinned_cypher = || {
        json!({
            "request_id": Uuid::new_v4(),
            "model_revision": revision,
            "language": "cypher",
            "csr_generation": refreshed["generation"],
            "query": "MATCH p=allShortestPaths((a:Account {tenant_id:1,from_account_id:9007199254740993})-[:Payment*1..2]->(m:Merchant)) RETURN [e IN relationships(p) | id(e)] AS ids ORDER BY ids"
        })
    };
    let before: Value = coordinator
        .request(Method::POST, "models/payments/query")
        .json(&pinned_cypher())
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(before["rows"].as_array().unwrap().len(), 2);
    payments_file(&extra, false, &[8])?;
    let after: Value = coordinator
        .request(Method::POST, "models/payments/query")
        .json(&pinned_cypher())
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(after["rows"], before["rows"]);
    assert_eq!(after["sources"], before["sources"]);
    let after_source_change: Value = explore(Some(&old_graph["page_cursor"]))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(old_graph["graph"], after_source_change["graph"]);
    let configuration_errors = || async {
        let metrics = coordinator
            .http
            .get(format!("{}/metrics", coordinator.base))
            .send()
            .await?
            .error_for_status()?
            .text()
            .await?;
        let count = metrics
            .lines()
            .find_map(|line| {
                line.strip_prefix("graphlake_query_errors_total{category=\"configuration\"} ")
            })
            .context("configuration counter")?;
        anyhow::Ok(count.parse::<u64>()?)
    };
    let count = configuration_errors().await?;
    let invalid = coordinator
        .query(Uuid::new_v4(), "cypher", "RETURN (")
        .await?;
    assert_eq!(invalid.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(configuration_errors().await?, count + 1);
    let conflict = coordinator
        .query(
            Uuid::new_v4(),
            "sql",
            r#"SELECT payment_id FROM "Payment" LIMIT 1"#,
        )
        .await?;
    assert_eq!(conflict.status(), StatusCode::UNPROCESSABLE_ENTITY);
    Ok(())
}

#[tokio::test]
async fn text_sqlite_and_duckdb_sources_browse_publish_query_and_restart() -> Result<()> {
    let root = tempfile::tempdir()?;
    setup_assets(root.path())?;
    setup_credentials(root.path())?;
    fs::write(root.path().join("accounts.csv"), "id,name\n1,Alice\n")?;
    fs::write(
        root.path().join("notes.jsonl"),
        "{\"id\":1,\"note\":\"imported\"}\n",
    )?;
    rusqlite::Connection::open(root.path().join("local.db"))?.execute_batch(
        "CREATE TABLE flags (id INTEGER, enabled BOOLEAN); INSERT INTO flags VALUES (1,1);",
    )?;
    duckdb::Connection::open(root.path().join("analytics.duckdb"))?.execute_batch("CREATE TABLE amounts (id BIGINT, amount DECIMAL(12,2)); INSERT INTO amounts VALUES (1,12.25)")?;
    let yaml = r#"graph: integration
sources:
  csv:
    type: csv
    tables:
      accounts:
        files: [accounts.csv]
        columns: [{name: id, data_type: Int64}, {name: name, data_type: Utf8}]
  json:
    type: jsonl
    tables:
      notes:
        files: [notes.jsonl]
        columns: [{name: id, data_type: Int64}, {name: note, data_type: Utf8}]
  db: {type: sqlite, path: local.db}
  analytics: {type: duckdb, path: analytics.duckdb}
nodes:
  Account: {source: csv.accounts, key: [id], properties: [name]}
  Note: {source: json.notes, key: [id], properties: [note]}
  Flag: {source: db.flags, key: [id], properties: [enabled]}
  Amount: {source: analytics.main.amounts, key: [id], properties: [amount]}
"#;
    let mut server = Server::start(root.path(), "", 0)?;
    server.login().await?;
    let created: Value = server
        .request(Method::POST, "models")
        .json(&json!({"id":"payments","yaml":yaml}))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let no_bucket = server
        .request(Method::POST, "models/payments/browse")
        .json(&json!({
            "yaml": "graph: object-browser\nsources:\n  files:\n    type: object_storage\n    storage: {provider: s3, bucket: ''}\n    format: parquet\n    tables: {}\nnodes: {}\n",
            "source": "files",
            "object_prefix": "exports/"
        }))
        .send()
        .await?;
    assert_eq!(no_bucket.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        no_bucket.json::<Value>().await?["code"],
        "object_list_failed"
    );
    for (source, table, column, kind, schema) in [
        ("csv", "accounts", "name", "Utf8", None),
        ("json", "notes", "note", "Utf8", None),
        ("db", "flags", "enabled", "Boolean", None),
        (
            "analytics",
            "amounts",
            "amount",
            "Decimal128(12, 2)",
            Some("main"),
        ),
    ] {
        let listing: Value = server
            .request(Method::POST, "models/payments/browse")
            .json(&json!({"yaml":yaml,"source":source,"schema":schema}))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        assert_eq!(listing["tables"], json!([table]));
        let fields: Value = server
            .request(Method::POST, "models/payments/browse")
            .json(&json!({"yaml":yaml,"source":source,"table":table,"schema":schema}))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        assert!(
            fields["columns"]
                .as_array()
                .unwrap()
                .iter()
                .any(|field| field["name"] == column && field["data_type"] == kind),
            "{fields}"
        );
    }
    let validation = server.validate("payments", yaml).await?;
    assert_eq!(validation["status"], "passed", "{validation}");
    server
        .request(Method::POST, "models/payments/publish")
        .json(&json!({"revision":created["revision"],"active_revision":null}))
        .send()
        .await?
        .error_for_status()?;
    for restart in [false, true] {
        if restart {
            drop(server);
            server = Server::start(root.path(), "", 0)?;
            server.login().await?;
        }
        let result: Value = server.query(Uuid::new_v4(), "sql",
            "SELECT a.name, n.note, f.enabled, d.amount FROM \"Account\" a JOIN \"Note\" n ON a.id=n.id JOIN \"Flag\" f ON f.id=a.id JOIN \"Amount\" d ON d.id=a.id"
        ).await?.error_for_status()?.json().await?;
        assert_eq!(
            result["rows"],
            json!([["Alice", "imported", "true", "12.25"]]),
            "{result}"
        );
        let readiness: Value = server
            .request(Method::GET, "models/payments/readiness")
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        assert_eq!(
            readiness["sources"],
            json!({"csv":true,"json":true,"db":true,"analytics":true})
        );
    }
    fs::write(root.path().join("accounts.csv"), "name,id\nAlice,1\n")?;
    assert_eq!(
        server
            .request(Method::GET, "models/payments/readiness")
            .send()
            .await?
            .status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    Ok(())
}
