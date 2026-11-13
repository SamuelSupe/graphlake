#![cfg(feature = "admin")]

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
use graphlake::arrow::array::{Decimal128Array, Int32Array, Int64Array, RecordBatch, StringArray};
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
        let mut child = Command::new(env!("CARGO_BIN_EXE_graphlake-admin"))
            .arg("--port")
            .arg(port.to_string())
            .args(["--timeout-ms", "2500", "--catalog-dir"])
            .arg(root)
            .arg("--assets-dir")
            .arg(root.join("assets"))
            .env("GRAPHLAKE_ADMIN_TEST_PG_DSN", dsn)
            .env(
                "GRAPHLAKE_ADMIN_OFFLINE",
                "postgresql://reader@127.0.0.1:1/fixture?sslmode=disable",
            )
            .stderr(Stdio::piped())
            .spawn()?;
        let mut message = String::new();
        BufReader::new(child.stderr.take().unwrap()).read_line(&mut message)?;
        let Some(base) = message.trim().strip_prefix("GraphLake admin ") else {
            let _ = child.kill();
            let _ = child.wait();
            anyhow::bail!("admin server failed to start: {message}");
        };
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

fn setup_assets(root: &Path) -> Result<()> {
    fs::create_dir(root.join("assets"))?;
    fs::write(
        root.join("assets/index.html"),
        "<!doctype html><title>GraphLake</title>",
    )?;
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
    let response = account(Method::GET, "models")
        .send()
        .await?
        .error_for_status()?;
    assert_eq!(response.json::<Value>().await?, json!([]));
    let dropped_in = include_str!("../examples/finance.yaml");
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

    for (username, password) in [("missing", PASSWORD), ("admin", "incorrect test password")]
        .into_iter()
        .cycle()
        .take(5)
    {
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
    assert_eq!(loaded["active_revision"], rev1);

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
