use super::*;
use futures::TryStreamExt;
use graphlake::{Catalog, Engine, Language, QueryOptions};
use parquet::{
    arrow::arrow_reader::{ArrowReaderOptions, ParquetRecordBatchReaderBuilder},
    file::{metadata::PageIndexPolicy, reader::ChunkReader},
};
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;

const BASE: &str = "models/payments/local-tables";

#[tokio::test]
#[ignore = "requires the disposable etcd fixture"]
async fn revocation_reconciliation_and_failed_csr_refresh_are_visible() -> Result<()> {
    let root = tempfile::tempdir()?;
    let dir = root.path();
    setup_assets(dir)?;
    setup_credentials(dir)?;
    let nodes = dir.join("nodes.db");
    rusqlite::Connection::open(&nodes)?.execute_batch("CREATE TABLE users(id INTEGER PRIMARY KEY) STRICT; INSERT INTO users VALUES(1),(2),(3),(4),(5);")?;
    let db = rusqlite::Connection::open(dir.join("grants.db"))?;
    db.execute_batch("CREATE TABLE grants(id INTEGER PRIMARY KEY, src INTEGER, dst INTEGER, version INTEGER NOT NULL, deleted INTEGER NOT NULL) STRICT;
        INSERT INTO grants VALUES(1,1,2,110,0),(2,1,3,130,0);")?;
    let yaml = "graph: authorization\nsources:\n  nodes: {type: sqlite, path: nodes.db}\n  source: {type: sqlite, path: grants.db}\nnodes:\n  User: {source: nodes.users, key: [id]}\nedges:\n  Grant: {source: source.grants, key: [id], from: {node: User, columns: [src]}, to: {node: User, columns: [dst]}}\n";
    fs::write(dir.join("payments.yaml"), yaml)?;
    fs::create_dir(dir.join("store"))?;
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let port = listener.local_addr()?.port().to_string();
    let config = dir.join("cluster.yaml");
    fs::write(
        &config,
        format!(
            "name: s4-{}\nmetadata: {}\nstorage: {}\ntoken_env: GRAPHLAKE_CLUSTER_TEST_TOKEN\nworkers: ['http://127.0.0.1:{port}']\ndistributed_partitions: 2\n",
            Uuid::new_v4().simple(),
            std::env::var("GRAPHLAKE_TEST_ETCD")?,
            dir.join("store").display()
        ),
    )?;
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
        .arg(dir)
        .env("GRAPHLAKE_CLUSTER_TEST_TOKEN", PASSWORD.repeat(2))
        .stderr(Stdio::piped())
        .spawn()?;
    let mut ready = String::new();
    BufReader::new(child.stderr.take().unwrap()).read_line(&mut ready)?;
    let _worker = Server {
        child,
        base: ready
            .trim()
            .strip_prefix("GraphLake worker ")
            .context("worker startup")?
            .into(),
        http: Client::new(),
        cookie: None,
    };
    let mut server =
        Server::start_with(dir, "", 0, &["--cluster-config", config.to_str().unwrap()])?;
    server.login().await?;
    let draft = server.model("payments").await?;
    let replica: Value = server.request(Method::POST, BASE).json(&json!({
        "model_revision": draft["revision"], "name":"grants", "source_table":"source.grants",
        "columns":["id","src","dst","version","deleted"], "key_columns":["id"], "watermark_column":"version",
        "deleted_column":"deleted", "lookback":10, "mode":"incremental"
    })).send().await?.error_for_status()?.json().await?;
    let first = load(&server, "grants", "full").await?;
    assert_eq!(first["total_rows"], 2, "{first}");
    let yaml = format!(
        "freshness: {{auto_refresh_csr: true}}\n{}",
        yaml.replace(
            "{type: sqlite, path: grants.db}",
            &format!(
                "{{type: parquet, tables: {{grants: ['{}']}}}}",
                replica["path"].as_str().unwrap()
            )
        )
    );
    let updated: Value = server
        .request(Method::PUT, "models/payments")
        .header(
            "If-Match",
            format!("\"{}\"", draft["revision"].as_str().unwrap()),
        )
        .json(&json!({"yaml": yaml}))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let published: Value = server
        .request(Method::POST, "models/payments/publish")
        .json(&json!({"revision":updated["revision"],"active_revision":draft["active_revision"]}))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let revision = published["active_revision"].clone();
    async fn wait_health(
        server: &Server,
        predicate: impl Fn(&Value) -> bool,
        seconds: u64,
    ) -> Result<Value> {
        tokio::time::timeout(Duration::from_secs(seconds), async {
            loop {
                let health: Value = server
                    .request(Method::GET, "models/payments/freshness")
                    .send()
                    .await?
                    .error_for_status()?
                    .json()
                    .await?;
                if predicate(&health) {
                    return Ok(health);
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await?
    }
    let head = wait_health(&server, |h| h["csr"]["current"] == true, 15).await?;
    let path = |generation: &Value, strict| {
        server.request(Method::POST, "models/payments/query").json(&json!({
        "request_id": Uuid::new_v4(), "model_revision": revision,
        "freshness": {"require_current_csr": strict},
        "csr": {"generation": generation, "edge_types":["Grant"], "operation": {
            "kind":"shortest", "start":{"label":"User","key":["1"]}, "target":{"label":"User","key":["2"]}, "max_hops":1
        }}
    }))
    };
    let generation = &head["csr"]["generation"];
    let before: Value = path(generation, true)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(before["rows"].as_array().unwrap().len(), 1, "{before}");
    let config = table(&server, "grants").await?;
    server.request(Method::PUT, &format!("{BASE}/grants"))
        .json(&json!({"revision":config["revision"], "mode":"incremental", "interval_seconds":null,"reconcile_interval_seconds":30,"timeout_seconds":10}))
        .send().await?.error_for_status()?;
    let started = std::time::Instant::now();
    let offline = dir.join("offline.db");
    fs::rename(&nodes, &offline)?;
    db.execute_batch(
        "UPDATE grants SET deleted=1,version=131 WHERE id=1;
        INSERT INTO grants VALUES(3,1,4,125,0),(4,1,5,90,0); DELETE FROM grants WHERE id=2;",
    )?;
    let delta = load(&server, "grants", "incremental").await?;
    assert_eq!(delta["total_rows"], 2, "{delta}");
    let state = wait_health(&server, |h| h["refresh"]["status"] == "failed", 15).await?;
    assert_eq!(state["csr"]["generation"], *generation);
    assert_eq!(state["csr"]["current"], false);
    let rejected = path(generation, true).send().await?;
    assert_eq!(rejected.status(), StatusCode::CONFLICT);
    assert_eq!(
        rejected.json::<Value>().await?["code"],
        "freshness_required"
    );
    let stale: Value = path(generation, false)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(stale["rows"], before["rows"]);
    let expired = server.request(Method::POST, "models/payments/query").json(&json!({
        "request_id": Uuid::new_v4(), "model_revision": revision, "language":"sql", "query":"SELECT id FROM \"Grant\"",
        "freshness":{"max_reconciliation_age_seconds":1}
    })).send().await?;
    assert_eq!(expired.status(), StatusCode::CONFLICT);
    fs::rename(&offline, &nodes)?;
    let live: Value = server
        .query(
            Uuid::new_v4(),
            "sql",
            "SELECT id FROM \"Grant\" ORDER BY id",
        )
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(live["rows"], json!([["2"], ["3"]]));
    eprintln!(
        "revocation local_visible_ms={} refresh_failure_visible_ms={}",
        delta["finished_at_ms"].as_u64().unwrap() - delta["started_at_ms"].as_u64().unwrap(),
        started.elapsed().as_millis()
    );
    let full = tokio::time::timeout(Duration::from_secs(35), async {
        loop {
            let current = table(&server, "grants").await?;
            let run = &current["history"][0];
            if run["trigger"] == "reconcile" && run["status"] != "running" {
                return Ok::<_, anyhow::Error>(run.clone());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await??;
    assert_eq!(full["total_rows"], 2, "{full}");
    let live: Value = server
        .query(
            Uuid::new_v4(),
            "sql",
            "SELECT id FROM \"Grant\" ORDER BY id",
        )
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(live["rows"], json!([["3"], ["4"]]));
    eprintln!(
        "revocation reconciliation_visible_ms={}",
        started.elapsed().as_millis()
    );
    let next = wait_health(
        &server,
        |h| h["csr"]["current"] == true && h["csr"]["generation"] != *generation,
        45,
    )
    .await?;
    let after: Value = path(&next["csr"]["generation"], true)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(after["rows"], json!([]), "{after}");
    eprintln!(
        "revocation strict_csr_visible_ms={}",
        started.elapsed().as_millis()
    );
    Ok(())
}

fn parquet_rows(path: &Path, rows: &[(i64, &str, i64)]) -> Result<()> {
    let batch = RecordBatch::try_from_iter(vec![
        (
            "id",
            Arc::new(Int64Array::from(
                rows.iter().map(|v| v.0).collect::<Vec<_>>(),
            )) as graphlake::arrow::array::ArrayRef,
        ),
        (
            "name",
            Arc::new(StringArray::from(
                rows.iter().map(|v| v.1).collect::<Vec<_>>(),
            )) as _,
        ),
        (
            "version",
            Arc::new(Int64Array::from(
                rows.iter().map(|v| v.2).collect::<Vec<_>>(),
            )) as _,
        ),
    ])?;
    let stage = tempfile::NamedTempFile::new_in(path.parent().unwrap())?;
    let mut writer = ArrowWriter::try_new(stage.as_file().try_clone()?, batch.schema(), None)?;
    writer.write(&batch)?;
    writer.close()?;
    stage.persist(path)?;
    Ok(())
}

fn yaml(source: &str, reference: &str) -> String {
    format!(
        "graph: local_test\nsources:\n  input: {source}\nnodes:\n  Person:\n    source: {reference}\n    key: [id]\n    properties: [name, version]\n"
    )
}

async fn create(server: &Server, name: &str, source_table: &str) -> Result<Value> {
    let model = server.model("payments").await?;
    let response = server.request(Method::POST, BASE).json(&json!({
        "model_revision": model["revision"], "name": name, "source_table": source_table,
        "columns": ["id", "name", "version"], "key_columns": ["id"], "watermark_column": "version",
        "mode": "incremental", "interval_seconds": null, "timeout_seconds": 10
    })).send().await?;
    let status = response.status();
    let body = response.json::<Value>().await?;
    ensure!(
        status == StatusCode::CREATED,
        "create failed {status}: {body}"
    );
    Ok(body)
}

async fn table(server: &Server, name: &str) -> Result<Value> {
    let tables: Vec<Value> = server
        .request(Method::GET, BASE)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    tables
        .into_iter()
        .find(|t| t["name"] == name)
        .context("missing local table")
}

async fn completed(server: &Server, name: &str, run: &str) -> Result<Value> {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let table = table(server, name).await?;
            if table["history"][0]["id"] == run && table["history"][0]["status"] != "running" {
                return Ok::<_, anyhow::Error>(table["history"][0].clone());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await?
}

async fn load(server: &Server, name: &str, mode: &str) -> Result<Value> {
    let response = server
        .request(Method::POST, &format!("{BASE}/{name}/load"))
        .json(&json!({"mode": mode}))
        .send()
        .await?;
    let status = response.status();
    let run: Value = response.json().await?;
    ensure!(
        status == StatusCode::ACCEPTED,
        "load failed {status}: {run}"
    );
    completed(server, name, run["id"].as_str().unwrap()).await
}

async fn rows(root: &Path, path: &str) -> Result<Vec<(i64, String, i64)>> {
    let model_path = root.join("verify.yaml");
    fs::write(
        &model_path,
        yaml(
            &format!("{{type: parquet, tables: {{items: ['{path}']}}}}"),
            "input.items",
        ),
    )?;
    let engine = Engine::new(Catalog::load(&model_path)?)?;
    let result = engine
        .query(
            Language::Sql,
            "SELECT id, name, version FROM \"Person\" ORDER BY id",
            QueryOptions::default(),
        )
        .await?;
    let batches = result.batches.try_collect::<Vec<_>>().await?;
    let mut values = Vec::new();
    for batch in batches {
        let id = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let name = batch.column(1);
        let version = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            values.push((
                id.value(row),
                graphlake::arrow::util::display::array_value_to_string(name, row)?,
                version.value(row),
            ));
        }
    }
    Ok(values)
}

#[tokio::test]
async fn local_tables_atomic_incremental_restart_and_failure() -> Result<()> {
    let root = tempfile::tempdir()?;
    setup_assets(root.path())?;
    setup_credentials(root.path())?;
    let source = root.path().join("source.parquet");
    parquet_rows(&source, &[(1, "one", 10), (2, "two", 20)])?;
    fs::write(
        root.path().join("payments.yaml"),
        yaml(
            "{type: parquet, tables: {items: [source.parquet]}}",
            "input.items",
        ),
    )?;
    let mut server = Server::start(root.path(), "", 0)?;
    assert_eq!(
        server.guest(Method::GET, BASE).send().await?.status(),
        StatusCode::UNAUTHORIZED
    );
    server.login().await?;
    let definition = create(&server, "people", "input.items").await?;
    assert_eq!(definition["ready"], false);
    server.request(Method::PUT, "users/sync_reader").json(&json!({"service": true, "token": "local-table-reader-token-for-tests", "disabled": false})).send().await?.error_for_status()?;
    server
        .request(Method::PUT, "roles/sync_readers")
        .json(&json!({"members": ["sync_reader"], "grants": {"payments": ["query", "modeler"]}}))
        .send()
        .await?
        .error_for_status()?;
    for (method, endpoint) in [
        (Method::GET, BASE.to_owned()),
        (Method::POST, format!("{BASE}/people/load")),
    ] {
        assert_eq!(
            server
                .guest(method, &endpoint)
                .basic_auth(
                    "svc/sync_reader",
                    Some("local-table-reader-token-for-tests")
                )
                .json(&json!({"mode":"full"}))
                .send()
                .await?
                .status(),
            StatusCode::FORBIDDEN
        );
    }

    let path = definition["path"].as_str().unwrap();
    let first = load(&server, "people", "incremental").await?;
    assert_eq!(first["status"], "succeeded", "{first}");
    assert_eq!(first["total_rows"], 2);
    let file_url = url::Url::from_file_path(path).unwrap();
    let alias = server
        .request(Method::POST, "models")
        .json(&json!({
            "id": "alias",
            "yaml": yaml(
                &format!("{{type: parquet, tables: {{items: ['{file_url}']}}}}"),
                "input.items",
            )
        }))
        .send()
        .await?
        .error_for_status()?
        .json::<Value>()
        .await?;
    let alias_load = server
        .request(Method::POST, "models/alias/local-tables")
        .json(&json!({
            "model_revision": alias["revision"], "name": "chain", "source_table": "input.items",
            "columns": ["id", "name", "version"], "mode": "full"
        }))
        .send()
        .await?;
    assert_eq!(alias_load.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(alias_load.json::<Value>().await?["code"], "local_source");
    parquet_rows(
        &source,
        &[(1, "updated", 30), (2, "two", 20), (3, "boundary", 20)],
    )?;
    let second = load(&server, "people", "incremental").await?;
    assert_eq!(second["status"], "succeeded", "{second}");
    assert_eq!(second["total_rows"], 3);
    assert_eq!(
        rows(root.path(), path).await?,
        vec![
            (1, "updated".into(), 30),
            (2, "two".into(), 20),
            (3, "boundary".into(), 20)
        ]
    );
    let committed = fs::read(path)?;
    parquet_rows(&source, &[(1, "bad", 40), (1, "duplicate", 40)])?;
    let failed = load(&server, "people", "incremental").await?;
    assert_eq!(failed["status"], "failed");
    assert!(failed["error"].as_str().unwrap().contains("重复主键"));
    assert_eq!(fs::read(path)?, committed);
    assert_eq!(load(&server, "people", "full").await?["status"], "failed");
    assert_eq!(fs::read(path)?, committed);
    drop(server);
    let mut server = Server::start(root.path(), "", 0)?;
    server.login().await?;
    assert_eq!(
        table(&server, "people").await?["history"][0]["status"],
        "failed"
    );
    parquet_rows(&source, &[(1, "after restart", 40), (4, "new", 35)])?;
    let retry = load(&server, "people", "incremental").await?;
    assert_eq!(retry["status"], "succeeded", "{retry}");
    assert_eq!(retry["loaded_rows"], 2);
    assert_eq!(retry["total_rows"], 4);
    #[cfg(unix)]
    let snapshot = fs::File::open(path)?;
    let repeated = load(&server, "people", "incremental").await?;
    assert_eq!(repeated["status"], "succeeded", "{repeated}");
    assert_eq!(repeated["total_rows"], 4);
    assert_eq!(repeated["loaded_rows"], 1);
    parquet_rows(&source, &[])?;
    let no_rows = load(&server, "people", "incremental").await?;
    assert_eq!(no_rows["status"], "succeeded", "{no_rows}");
    assert_eq!(no_rows["total_rows"], 4);
    assert_eq!(no_rows["loaded_rows"], 0);
    #[cfg(unix)]
    assert_eq!(fs::metadata(path)?.ino(), snapshot.metadata()?.ino());
    // The same primary key and watermark do not make a changed payload a no-op.
    parquet_rows(&source, &[(1, "same watermark", 40), (4, "new", 35)])?;
    let boundary_update = load(&server, "people", "incremental").await?;
    assert_eq!(boundary_update["status"], "succeeded", "{boundary_update}");
    assert_eq!(
        rows(root.path(), path).await?[0],
        (1, "same watermark".into(), 40)
    );
    let full = load(&server, "people", "full").await?;
    assert_eq!(full["total_rows"], 2);
    fs::remove_file(source)?;
    assert_eq!(
        rows(root.path(), path).await?,
        vec![(1, "same watermark".into(), 40), (4, "new".into(), 35)]
    );
    assert_eq!(load(&server, "people", "full").await?["status"], "failed");
    assert_eq!(rows(root.path(), path).await?.len(), 2);
    parquet_rows(&root.path().join("source.parquet"), &[])?;
    assert_eq!(load(&server, "people", "full").await?["total_rows"], 0);
    parquet_rows(
        &root.path().join("source.parquet"),
        &[(5, "after empty", 1)],
    )?;
    assert_eq!(
        load(&server, "people", "incremental").await?["total_rows"],
        1
    );
    Ok(())
}

#[tokio::test]
async fn local_tables_incremental_reuses_row_groups_and_preserves_the_footer() -> Result<()> {
    let root = tempfile::tempdir()?;
    setup_assets(root.path())?;
    setup_credentials(root.path())?;
    let source = root.path().join("source.parquet");
    let last = 8192 * 5 + 7;
    parquet_rows(
        &source,
        &(1..=last)
            .map(|id| (id, "original", id))
            .collect::<Vec<_>>(),
    )?;
    fs::write(
        root.path().join("payments.yaml"),
        yaml(
            "{type: parquet, tables: {items: [source.parquet]}}",
            "input.items",
        ),
    )?;
    let mut server = Server::start(root.path(), "", 0)?;
    server.login().await?;
    let definition = create(&server, "groups", "input.items").await?;
    let path = definition["path"].as_str().unwrap();
    let initial = load(&server, "groups", "full").await?;
    assert_eq!(initial["status"], "succeeded", "{initial}");
    assert_eq!(initial["total_rows"], last);

    let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Required);
    let original = fs::File::open(path)?;
    let old = ParquetRecordBatchReaderBuilder::try_new_with_options(
        original.try_clone()?,
        options.clone(),
    )?;
    assert_eq!(old.metadata().num_row_groups(), 6);
    let id = 8192 + 3;
    parquet_rows(
        &source,
        &[
            (id, "updated", last + 1),
            (last, "original", last),
            (last + 1, "inserted", last + 1),
        ],
    )?;
    let updated = load(&server, "groups", "incremental").await?;
    assert_eq!(updated["status"], "succeeded", "{updated}");
    assert_eq!(updated["total_rows"], last + 1);

    let output = fs::File::open(path)?;
    let new = ParquetRecordBatchReaderBuilder::try_new_with_options(output.try_clone()?, options)?;
    // The unchanged third group is kept after the second group moves. Its
    // encoded bytes and the rebased page index both have to survive the move.
    let before = old.metadata().row_group(2);
    let after = new.metadata().row_group(1);
    for (before, after) in before.columns().iter().zip(after.columns()) {
        let (old_offset, old_size) = before.byte_range();
        let (new_offset, new_size) = after.byte_range();
        assert_ne!(old_offset, new_offset);
        assert_eq!(
            original.get_bytes(old_offset, old_size as usize)?,
            output.get_bytes(new_offset, new_size as usize)?
        );
    }
    let old_page = &old.metadata().offset_index().unwrap()[2][0].page_locations()[0];
    let new_page = &new.metadata().offset_index().unwrap()[1][0].page_locations()[0];
    assert_eq!(
        old_page.offset - before.column(0).data_page_offset(),
        new_page.offset - after.column(0).data_page_offset()
    );

    // A later incremental update must use the new footer's watermark and only
    // replace matching rows even if the row groups now overlap in key range.
    drop(server);
    let mut server = Server::start(root.path(), "", 0)?;
    server.login().await?;
    parquet_rows(
        &source,
        &[
            (last + 1, "inserted", last + 1),
            (id, "updated again", last + 2),
        ],
    )?;
    let next = load(&server, "groups", "incremental").await?;
    assert_eq!(next["status"], "succeeded", "{next}");
    assert_eq!(next["loaded_rows"], 2);
    assert_eq!(next["total_rows"], last + 1);
    let result = rows(root.path(), path).await?;
    assert_eq!(result.len(), (last + 1) as usize);
    assert!(
        result
            .iter()
            .enumerate()
            .all(|(i, row)| row.0 == i as i64 + 1)
    );
    assert_eq!(
        result[(id - 1) as usize],
        (id, "updated again".into(), last + 2)
    );
    assert_eq!(
        result.last(),
        Some(&(last + 1, "inserted".into(), last + 1))
    );
    Ok(())
}

#[tokio::test]
async fn local_tables_scattered_overlay_reads_recovers_and_compacts() -> Result<()> {
    let root = tempfile::tempdir()?;
    setup_assets(root.path())?;
    setup_credentials(root.path())?;
    let source = root.path().join("source.parquet");
    let last = 8192 * 10 + 5;
    parquet_rows(
        &source,
        &(1..=last)
            .map(|id| (id, "original", id))
            .collect::<Vec<_>>(),
    )?;
    fs::write(
        root.path().join("payments.yaml"),
        yaml(
            "{type: parquet, tables: {items: [source.parquet]}}",
            "input.items",
        ),
    )?;
    let mut server = Server::start(root.path(), "", 0)?;
    server.login().await?;
    let definition = create(&server, "overlay", "input.items").await?;
    let path = definition["path"].as_str().unwrap();
    assert_eq!(load(&server, "overlay", "full").await?["total_rows"], last);
    let base = fs::File::open(path)?;

    let changed = (0..10)
        .map(|group| (group * 8192 + 1, "changed", last + 1))
        .chain([(last, "original", last), (last + 1, "new", last + 1)])
        .collect::<Vec<_>>();
    parquet_rows(&source, &changed)?;
    let run = load(&server, "overlay", "incremental").await?;
    assert_eq!(run["status"], "succeeded", "{run}");
    assert_eq!(run["total_rows"], last + 1);
    let committed = fs::read(path)?;
    assert!(committed.starts_with(b"GLPQ1\n"));
    let first_manifest: Value = serde_json::from_slice(&committed[6..])?;
    let base_id = first_manifest["parts"][0]["id"].as_str().unwrap();
    let parts = Path::new(path).with_extension("parts");
    let base_path = parts.join(format!("{base_id}.parquet"));
    let files = fs::read_dir(&parts)?.collect::<std::io::Result<Vec<_>>>()?;
    assert_eq!(files.len(), 2);
    #[cfg(unix)]
    assert!(
        files
            .iter()
            .any(|file| file.metadata().unwrap().ino() == base.metadata().unwrap().ino())
    );

    let result = rows(root.path(), path).await?;
    assert_eq!(result.len(), (last + 1) as usize);
    assert!(
        result
            .iter()
            .enumerate()
            .all(|(i, row)| row.0 == i as i64 + 1)
    );
    for &(id, value, version) in &changed {
        assert_eq!(result[(id - 1) as usize], (id, value.into(), version));
    }
    let engine = Engine::new(Catalog::load(root.path().join("verify.yaml"))?)?;
    let point = || async {
        let result = engine
            .query(
                Language::Sql,
                "SELECT version FROM \"Person\" WHERE id = 1",
                QueryOptions::default(),
            )
            .await?;
        let values = result
            .batches
            .try_collect::<Vec<_>>()
            .await?
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect::<Vec<_>>();
        Ok::<_, anyhow::Error>((result.versions, values))
    };
    let (first_versions, versions) = point().await?;
    assert_eq!(versions, [last + 1]);
    let counts = engine
        .query(
            Language::Sql,
            "SELECT COUNT(*) AS n FROM \"Person\"",
            QueryOptions::default(),
        )
        .await?
        .batches
        .try_collect::<Vec<_>>()
        .await?;
    assert_eq!(
        counts[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        last + 1
    );

    drop(server);
    let orphan = parts.join(format!("{}.parquet", Uuid::new_v4()));
    fs::write(&orphan, b"uncommitted")?;
    let mut server = Server::start(root.path(), "", 0)?;
    server.login().await?;
    assert!(!orphan.exists());
    assert_eq!(fs::read(path)?, committed);
    let duplicate = [(1, "invalid", last + 2), (1, "duplicate", last + 2)];
    parquet_rows(&source, &duplicate)?;
    assert_eq!(
        load(&server, "overlay", "incremental").await?["status"],
        "failed"
    );
    assert_eq!(fs::read(path)?, committed);

    // Repeated writes of key 1 mask its older delta-file positions. Compaction
    // streams the surviving delta rows, including inserts absent from the base.
    for version in last + 2..=last + 16 {
        parquet_rows(&source, &[(1, "again", version)])?;
        let run = load(&server, "overlay", "incremental").await?;
        assert_eq!(run["status"], "succeeded", "{run}");
        assert_eq!(run["total_rows"], last + 1);
    }
    let compacted = fs::read(path)?;
    assert!(compacted.starts_with(b"GLPQ1\n"));
    let compacted: Value = serde_json::from_slice(&compacted[6..])?;
    assert_eq!(compacted["parts"].as_array().unwrap().len(), 2);
    assert_eq!(compacted["parts"][0], first_manifest["parts"][0]);
    assert_eq!(compacted["parts"][1]["rows"], changed.len());
    assert_eq!(fs::read_dir(&parts)?.count(), 2);
    #[cfg(unix)]
    assert_eq!(fs::metadata(&base_path)?.ino(), base.metadata()?.ino());

    // The consolidated part also carries the latest committed watermark.
    drop(server);
    let mut server = Server::start(root.path(), "", 0)?;
    server.login().await?;
    parquet_rows(
        &source,
        &[
            (1, "again", last + 16),
            (last + 2, "next", last + 16),
            (last + 3, "old", last + 15),
        ],
    )?;
    let replay = load(&server, "overlay", "incremental").await?;
    assert_eq!(replay["status"], "succeeded", "{replay}");
    assert_eq!(replay["loaded_rows"], 2);
    let (latest_versions, versions) = point().await?;
    assert_eq!(versions, [last + 16]);
    assert_ne!(latest_versions[0].version, first_versions[0].version);
    let result = rows(root.path(), path).await?;
    assert_eq!(result.len(), (last + 2) as usize);
    assert_eq!(result[0], (1, "again".into(), last + 16));
    assert_eq!(result[(last) as usize], (last + 1, "new".into(), last + 1));
    assert_eq!(result.last(), Some(&(last + 2, "next".into(), last + 16)));
    Ok(())
}

#[tokio::test]
async fn local_tables_reclaims_only_masked_base_partitions() -> Result<()> {
    let root = tempfile::tempdir()?;
    setup_assets(root.path())?;
    setup_credentials(root.path())?;
    let source = root.path().join("source.parquet");
    parquet_rows(
        &source,
        &[(65_536, "updated", 100_001), (65_537, "updated", 100_001)],
    )?;
    fs::write(
        root.path().join("payments.yaml"),
        yaml(
            "{type: parquet, tables: {items: [source.parquet]}}",
            "input.items",
        ),
    )?;
    let mut server = Server::start(root.path(), "", 0)?;
    server.login().await?;
    let definition = create(&server, "partitions", "input.items").await?;
    let path = definition["path"].as_str().unwrap();
    let parts = Path::new(path).with_extension("parts");
    fs::create_dir(&parts)?;
    let hot = Uuid::new_v4();
    let cold = Uuid::new_v4();
    let delta = Uuid::new_v4();
    let hot_file = parts.join(format!("{hot}.parquet"));
    let cold_file = parts.join(format!("{cold}.parquet"));
    parquet_rows(
        &hot_file,
        &(1..=70_000)
            .map(|id| (id, "original", id))
            .collect::<Vec<_>>(),
    )?;
    parquet_rows(&cold_file, &[(70_001, "cold", 70_001)])?;
    parquet_rows(
        &parts.join(format!("{delta}.parquet")),
        &(1..=65_535)
            .map(|id| (id, "delta", 100_000))
            .collect::<Vec<_>>(),
    )?;
    let old = json!({"generation": Uuid::new_v4(), "base_parts": 2, "parts": [
        {"id":hot,"rows":70_000,"deleted":(0..65_535).collect::<Vec<_>>()},
        {"id":cold,"rows":1}, {"id":delta,"rows":65_535},
    ]});
    fs::write(path, format!("GLPQ1\n{old}"))?;
    let cold_bytes = fs::read(&cold_file)?;
    let legacy = rows(root.path(), path).await?;
    assert_eq!(legacy.len(), 70_001);
    assert_eq!(legacy[0], (1, "delta".into(), 100_000));

    let updated = load(&server, "partitions", "incremental").await?;
    assert_eq!(updated["status"], "succeeded", "{updated}");
    assert_eq!(updated["total_rows"], 70_001);
    let bytes = fs::read(path)?;
    let manifest: Value = serde_json::from_slice(&bytes[6..])?;
    assert_eq!(manifest["base_parts"], 2);
    assert_ne!(manifest["parts"][0]["id"], old["parts"][0]["id"]);
    assert_eq!(manifest["parts"][0]["rows"], 4_463);
    assert_eq!(manifest["parts"][1]["id"], old["parts"][1]["id"]);
    assert_eq!(manifest["parts"][1]["key_range"], json!([70_001, 70_001]));
    assert_eq!(fs::read(&cold_file)?, cold_bytes);
    assert!(!hot_file.exists());

    let engine = Engine::new(Catalog::load(root.path().join("verify.yaml"))?)?;
    let query = |sql: &'static str| async {
        let result = engine
            .query(Language::Sql, sql, QueryOptions::default())
            .await?;
        let batches = result.batches.try_collect::<Vec<_>>().await?;
        Ok::<_, anyhow::Error>(batches.iter().map(RecordBatch::num_rows).sum::<usize>())
    };
    let hidden = cold_file.with_extension("hidden");
    fs::rename(&cold_file, &hidden)?;
    let hit = query("SELECT name FROM \"Person\" WHERE 65537 = id").await;
    let required = query("SELECT name FROM \"Person\" WHERE id = 70001").await;
    fs::rename(&hidden, &cold_file)?;
    assert_eq!(
        hit.context("commuted point lookup should skip the cold file")?,
        1
    );
    assert!(required.is_err());

    let hidden = parts.with_extension("hidden");
    fs::rename(&parts, &hidden)?;
    let miss = query("SELECT name FROM \"Person\" WHERE id = -1").await;
    fs::rename(&hidden, &parts)?;
    assert_eq!(
        miss.context("out-of-range lookup should skip all files")?,
        0
    );
    assert_eq!(
        query("SELECT name FROM \"Person\" WHERE id = -1 OR id = 70001").await?,
        1
    );

    drop(server);
    let mut server = Server::start(root.path(), "", 0)?;
    server.login().await?;
    let replay = load(&server, "partitions", "incremental").await?;
    assert_eq!(replay["status"], "succeeded", "{replay}");
    assert_eq!(fs::read(path)?, bytes);
    let result = rows(root.path(), path).await?;
    assert_eq!(result.len(), 70_001);
    assert!(
        result
            .iter()
            .enumerate()
            .all(|(i, row)| row.0 == i as i64 + 1)
    );
    assert_eq!(result[65_534], (65_535, "delta".into(), 100_000));
    assert_eq!(result[65_536], (65_537, "updated".into(), 100_001));
    assert_eq!(result.last(), Some(&(70_001, "cold".into(), 70_001)));

    fs::remove_file(&cold_file)?;
    assert_eq!(
        load(&server, "partitions", "incremental").await?["status"],
        "failed"
    );
    assert_eq!(fs::read(path)?, bytes);
    let repaired = load(&server, "partitions", "full").await?;
    assert_eq!(repaired["status"], "succeeded", "{repaired}");
    assert_eq!(
        rows(root.path(), path).await?,
        vec![
            (65_536, "updated".into(), 100_001),
            (65_537, "updated".into(), 100_001)
        ]
    );
    Ok(())
}

#[tokio::test]
async fn local_table_result_persistence_retries_without_shutting_down_http() -> Result<()> {
    let root = tempfile::tempdir()?;
    setup_assets(root.path())?;
    setup_credentials(root.path())?;
    let db = rusqlite::Connection::open(root.path().join("source.db"))?;
    db.execute_batch("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL, version INTEGER NOT NULL); INSERT INTO items VALUES (1, 'one', 10);")?;
    fs::write(
        root.path().join("payments.yaml"),
        yaml("{type: sqlite, path: source.db}", "input.items"),
    )?;
    let mut server = Server::start(root.path(), "", 0)?;
    server.login().await?;
    create(&server, "fault", "input.items").await?;

    db.execute_batch("BEGIN EXCLUSIVE")?;
    let run: Value = server
        .request(Method::POST, &format!("{BASE}/fault/load"))
        .json(&json!({"mode": "full"}))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let state = root.path().join(".graphlake-local/state.json");
    let saved = state.with_extension("original");
    fs::rename(&state, &saved)?;
    fs::create_dir(&state)?;
    tokio::time::sleep(Duration::from_millis(180)).await;
    db.execute_batch("COMMIT")?;
    let done = completed(&server, "fault", run["id"].as_str().unwrap()).await?;
    assert_eq!(done["status"], "succeeded");
    server
        .http
        .get(format!("{}/healthz", server.base))
        .send()
        .await?
        .error_for_status()?;

    fs::remove_dir(&state)?;
    fs::rename(saved, &state)?;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let persisted: Value = serde_json::from_slice(&fs::read(&state)?)?;
            if persisted["payments/fault"]["history"][0]["status"] == "succeeded" {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await??;
    Ok(())
}

#[tokio::test]
async fn local_tables_schedule_persists_and_pauses() -> Result<()> {
    let root = tempfile::tempdir()?;
    setup_assets(root.path())?;
    setup_credentials(root.path())?;
    let source = root.path().join("source.parquet");
    parquet_rows(&source, &[(1, "one", 10)])?;
    fs::write(
        root.path().join("payments.yaml"),
        yaml(
            "{type: parquet, tables: {items: [source.parquet]}}",
            "input.items",
        ),
    )?;
    let mut server = Server::start(root.path(), "", 0)?;
    server.login().await?;
    let definition = create(&server, "scheduled", "input.items").await?;
    let endpoint = format!("{BASE}/scheduled");
    let settings = json!({"revision": definition["revision"], "mode": "incremental", "interval_seconds": 30, "timeout_seconds": 10});
    let mut invalid = settings.clone();
    invalid["interval_seconds"] = json!(1);
    let invalid_response = server
        .request(Method::PUT, &endpoint)
        .json(&invalid)
        .send()
        .await?;
    let invalid_status = invalid_response.status();
    assert_eq!(
        invalid_status,
        StatusCode::UNPROCESSABLE_ENTITY,
        "{}",
        invalid_response.text().await?
    );
    let scheduled: Value = server
        .request(Method::PUT, &endpoint)
        .json(&settings)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(
        server
            .request(Method::PUT, &endpoint)
            .json(&settings)
            .send()
            .await?
            .status(),
        StatusCode::CONFLICT
    );
    drop(server);
    let mut server = Server::start(root.path(), "", 0)?;
    server.login().await?;
    assert_eq!(
        table(&server, "scheduled").await?["next_run_at_ms"],
        scheduled["next_run_at_ms"]
    );
    let run = tokio::time::timeout(Duration::from_secs(35), async {
        loop {
            let table = table(&server, "scheduled").await?;
            if table["history"][0]["status"] == "succeeded" {
                return Ok::<_, anyhow::Error>(table);
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await??;
    assert_eq!(run["history"][0]["trigger"], "scheduled");
    assert_eq!(run["history"][0]["total_rows"], 1);
    let paused: Value = server.request(Method::PUT, &endpoint).json(&json!({
        "revision": run["revision"], "mode": "incremental", "interval_seconds": null, "timeout_seconds": 10
    })).send().await?.error_for_status()?.json().await?;
    assert_eq!(paused["next_run_at_ms"], Value::Null);
    drop(server);
    let mut server = Server::start(root.path(), "", 0)?;
    server.login().await?;
    assert_eq!(
        table(&server, "scheduled").await?["interval_seconds"],
        Value::Null
    );
    parquet_rows(&source, &[(1, "changed", 20)])?;
    tokio::time::sleep(Duration::from_secs(31)).await;
    assert_eq!(
        table(&server, "scheduled").await?["history"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires writable PostgreSQL in GRAPHLAKE_ADMIN_TEST_PG_DSN"]
async fn local_tables_postgres_upsert_and_cancel() -> Result<()> {
    let dsn = std::env::var("GRAPHLAKE_ADMIN_TEST_PG_DSN")?;
    let (pg, driver) = tokio_postgres::connect(&dsn, NoTls).await?;
    let connection = tokio::spawn(driver);
    let schema = format!("local_sync_{}", Uuid::new_v4().simple());
    pg.batch_execute(&format!("CREATE SCHEMA {schema}; CREATE TABLE {schema}.items(id bigint PRIMARY KEY, name text NOT NULL, version bigint NOT NULL); INSERT INTO {schema}.items VALUES (1, 'one', 10), (2, 'two', 20)")).await?;
    let root = tempfile::tempdir()?;
    setup_assets(root.path())?;
    setup_credentials(root.path())?;
    fs::write(
        root.path().join("payments.yaml"),
        yaml(
            "{type: postgres, dsn_env: GRAPHLAKE_ADMIN_TEST_PG_DSN}",
            &format!("input.{schema}.items"),
        ),
    )?;
    let mut server = Server::start(root.path(), &dsn, 0)?;
    server.login().await?;
    let definition = create(&server, "pg", &format!("input.{schema}.items")).await?;
    let full = load(&server, "pg", "full").await?;
    assert_eq!(full["status"], "succeeded", "{full}");
    pg.batch_execute(&format!("UPDATE {schema}.items SET name='changed', version=30 WHERE id=1; INSERT INTO {schema}.items VALUES(3,'new',30)")).await?;
    let update = load(&server, "pg", "incremental").await?;
    assert_eq!(update["status"], "succeeded", "{update}");
    assert_eq!(update["total_rows"], 3);
    pg.batch_execute(&format!(
        "BEGIN; LOCK {schema}.items IN ACCESS EXCLUSIVE MODE"
    ))
    .await?;
    let response: Value = server
        .request(Method::POST, &format!("{BASE}/pg/load"))
        .json(&json!({"mode":"full"}))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(
        server
            .request(Method::POST, &format!("{BASE}/pg/load"))
            .json(&json!({"mode":"full"}))
            .send()
            .await?
            .status(),
        StatusCode::CONFLICT
    );
    server
        .request(Method::POST, &format!("{BASE}/pg/cancel"))
        .send()
        .await?
        .error_for_status()?;
    let cancelled = completed(&server, "pg", response["id"].as_str().unwrap()).await?;
    assert_eq!(cancelled["status"], "cancelled", "{cancelled}");
    assert_eq!(
        rows(root.path(), definition["path"].as_str().unwrap())
            .await?
            .len(),
        3
    );
    pg.batch_execute("ROLLBACK").await?;
    pg.batch_execute(&format!(
        "BEGIN; LOCK {schema}.items IN ACCESS EXCLUSIVE MODE"
    ))
    .await?;
    let interrupted: Value = server
        .request(Method::POST, &format!("{BASE}/pg/load"))
        .json(&json!({"mode":"incremental"}))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    drop(server);
    pg.batch_execute("ROLLBACK").await?;
    let mut server = Server::start(root.path(), &dsn, 0)?;
    server.login().await?;
    let recovered = table(&server, "pg").await?;
    assert_eq!(recovered["history"][0]["id"], interrupted["id"]);
    assert_eq!(recovered["history"][0]["status"], "interrupted");
    assert_eq!(load(&server, "pg", "incremental").await?["total_rows"], 3);
    pg.batch_execute(&format!("CREATE TABLE {schema}.moments(id bigint PRIMARY KEY, name text, version timestamptz NOT NULL); INSERT INTO {schema}.moments VALUES (1,'first','2026-11-15 00:00:00+00')")).await?;
    create(&server, "moments", &format!("input.{schema}.moments")).await?;
    assert_eq!(
        load(&server, "moments", "incremental").await?["total_rows"],
        1
    );
    pg.batch_execute(&format!("UPDATE {schema}.moments SET name=NULL, version='2026-11-15 00:00:01+00'; INSERT INTO {schema}.moments VALUES (2,'boundary','2026-11-15 08:00:00+08')")).await?;
    let timestamps = load(&server, "moments", "incremental").await?;
    assert_eq!(timestamps["status"], "succeeded", "{timestamps}");
    assert_eq!(timestamps["total_rows"], 2);
    assert_eq!(timestamps["loaded_rows"], 2);
    assert_eq!(timestamps["source_reads"]["postgres_rows"], 2);
    let current = table(&server, "moments").await?;
    let timestamp_path = current["path"].as_str().unwrap();
    #[cfg(unix)]
    let original = fs::File::open(timestamp_path)?;
    let timestamp_replay = load(&server, "moments", "incremental").await?;
    assert_eq!(
        timestamp_replay["status"], "succeeded",
        "{timestamp_replay}"
    );
    assert_eq!(timestamp_replay["loaded_rows"], 1);
    assert_eq!(timestamp_replay["source_reads"]["postgres_rows"], 1);
    assert_eq!(timestamp_replay["total_rows"], 2);
    #[cfg(unix)]
    assert_eq!(
        fs::metadata(timestamp_path)?.ino(),
        original.metadata()?.ino()
    );
    let before = fs::read(timestamp_path)?;
    pg.batch_execute(&format!(
        "ALTER TABLE {schema}.moments ALTER COLUMN name TYPE bigint USING id"
    ))
    .await?;
    assert_eq!(load(&server, "moments", "full").await?["status"], "failed");
    assert_eq!(fs::read(timestamp_path)?, before);
    pg.batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
        .await?;
    connection.abort();
    Ok(())
}

#[tokio::test]
#[ignore = "requires a disposable MySQL 8.4 instance in GRAPHLAKE_TEST_MYSQL"]
async fn mysql_local_table_reads_only_timestamp_boundary_and_changes() -> Result<()> {
    use mysql_async::{Conn, Opts, prelude::Queryable};

    let mut mysql = Conn::new(Opts::from_url(&std::env::var("GRAPHLAKE_TEST_MYSQL")?)?).await?;
    mysql.query_drop("SET SESSION time_zone='+00:00'").await?;
    let name = format!("graphlake_{}", Uuid::new_v4().simple());
    mysql
        .query_drop(format!(
            "CREATE DATABASE {name} CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci"
        ))
        .await?;
    let test = async {
        mysql.query_drop(format!(
            "CREATE TABLE {name}.accounts (
                id BIGINT PRIMARY KEY, name VARCHAR(64),
                version TIMESTAMP(6) NOT NULL, KEY (version)
             ) ENGINE=InnoDB"
        )).await?;
        mysql.query_drop(format!(
            "INSERT INTO {name}.accounts
             WITH RECURSIVE ids(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM ids WHERE n<500)
             SELECT n, CASE n WHEN 1 THEN 'Blocked' WHEN 2 THEN 'blocked' ELSE 'active' END,
                    TIMESTAMP('2026-11-15 00:00:00.000000') + INTERVAL n MICROSECOND FROM ids"
        )).await?;
        let root = tempfile::tempdir()?;
        setup_assets(root.path())?;
        setup_credentials(root.path())?;
        let reference = format!("input.{name}.accounts");
        let mapping = yaml("{type: mysql, dsn_env: GRAPHLAKE_TEST_MYSQL, tls: false}", &reference);
        fs::write(root.path().join("payments.yaml"), &mapping)?;
        let mut server = Server::start(root.path(), "", 0)?;
        server.login().await?;
        let copy = create(&server, "accounts", &reference).await?;
        let full = load(&server, "accounts", "full").await?;
        assert_eq!(full["source_reads"]["mysql_rows"], 500, "{full}");
        let direct = Engine::new(Catalog::load(&root.path().join("payments.yaml"))?)?;
        let filtered = direct.query(
            Language::Sql,
            r#"SELECT id FROM "Person" WHERE name='blocked' ORDER BY id LIMIT 1"#,
            Default::default(),
        ).await?;
        let mut filtered = filtered;
        let batches = (&mut filtered.batches).try_collect::<Vec<_>>().await?;
        assert_eq!(filtered.metrics().reads.mysql_rows, 1);
        assert_eq!(batches[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap().value(0), 2);

        mysql.query_drop(format!("UPDATE {name}.accounts SET name='updated', version='2026-11-15 00:00:00.000500' WHERE id=1")).await?;
        mysql.query_drop(format!("INSERT INTO {name}.accounts VALUES (501, 'new', '2026-11-15 00:00:00.000501')")).await?;
        let incremental = load(&server, "accounts", "incremental").await?;
        assert_eq!(incremental["status"], "succeeded", "{incremental}");
        assert_eq!(incremental["loaded_rows"], 3);
        assert_eq!(incremental["source_reads"]["mysql_rows"], 3);
        assert_eq!(incremental["total_rows"], 501);
        let replay = load(&server, "accounts", "incremental").await?;
        assert_eq!(replay["source_reads"]["mysql_rows"], 1);
        assert_eq!(replay["total_rows"], 501);

        let copy_path = copy["path"].as_str().context("missing snapshot path")?;
        let replica = root.path().join("replica.yaml");
        fs::write(&replica, yaml(&format!("{{type: parquet, tables: {{accounts: ['{copy_path}']}}}}"), "input.accounts"))?;
        let replica = Engine::new(Catalog::load(&replica)?)?;
        let snapshot_sql = r#"SELECT id, name, version FROM "Person" ORDER BY id"#;
        let expected = direct.query(Language::Sql, snapshot_sql, Default::default()).await?;
        let actual = replica.query(Language::Sql, snapshot_sql, Default::default()).await?;
        let expected = expected.batches.try_collect::<Vec<_>>().await?;
        let actual = actual.batches.try_collect::<Vec<_>>().await?;
        let expected = graphlake::arrow::compute::concat_batches(&expected[0].schema(), &expected)?;
        let actual = graphlake::arrow::compute::concat_batches(&actual[0].schema(), &actual)?;
        for (replicated, source) in actual.columns().iter().zip(expected.columns()) {
            let replicated = graphlake::arrow::compute::cast(replicated, source.data_type())?;
            assert_eq!(&replicated, source);
        }
        Ok::<(), anyhow::Error>(())
    }.await;
    mysql.query_drop(format!("DROP DATABASE {name}")).await?;
    mysql.disconnect().await?;
    test
}

#[tokio::test]
async fn local_tables_indexed_keys_compact_keep_pinned_readers_and_recover() -> Result<()> {
    let root = tempfile::tempdir()?;
    setup_assets(root.path())?;
    setup_credentials(root.path())?;
    let db = rusqlite::Connection::open(root.path().join("source.db"))?;
    db.execute_batch(
        "CREATE TABLE accounts (tenant INTEGER NOT NULL, id TEXT NOT NULL, name TEXT NOT NULL, version INTEGER NOT NULL,
          PRIMARY KEY(tenant,id)) STRICT;
         WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<4000)
         INSERT INTO accounts SELECT i%4, printf('00000000-0000-4000-8000-%012x',i), 'original', i FROM n;"
    )?;
    fs::write(
        root.path().join("payments.yaml"),
        "graph: accounts\nsources:\n  input: {type: sqlite, path: source.db}\nnodes:\n  Person: {source: input.accounts, key: [tenant, id], properties: [name, version]}\n",
    )?;
    let mut server = Server::start(root.path(), "", 0)?;
    server.login().await?;
    let revision = server.model("payments").await?["revision"].clone();
    for (name, keys) in [
        ("uuid", json!(["id"])),
        ("composite", json!(["tenant", "id"])),
    ] {
        let view = server.request(Method::POST, BASE).json(&json!({
            "model_revision": revision, "name": name, "source_table":"input.accounts",
            "columns":["tenant","id","name","version"], "key_columns": keys, "watermark_column":"version",
            "mode":"incremental", "interval_seconds":null, "timeout_seconds":30
        })).send().await?.error_for_status()?.json::<Value>().await?;
        let path = Path::new(
            view["path"]
                .as_str()
                .context("missing indexed snapshot path")?,
        );
        let loaded = load(&server, name, "full").await?;
        assert_eq!(loaded["status"], "succeeded", "{loaded}");
        assert_eq!(loaded["total_rows"], 4000);
        let before = fs::read(path)?;
        assert!(before.starts_with(b"GLPQ2\n"));
        let base: Value = serde_json::from_slice(&before[6..])?;
        let base_id = base["parts"][0]["id"].clone();
        let redundant = load(&server, name, "incremental").await?;
        assert_eq!(redundant["loaded_rows"], 1);
        assert!(redundant["committed_generation"].is_null());
        assert_eq!(fs::read(path)?, before);

        if name == "uuid" {
            db.execute(
                "UPDATE accounts SET name='wide-delta' WHERE version BETWEEN 1000 AND 2200",
                [],
            )?;
            db.execute(
                "UPDATE accounts SET version=version+5000 WHERE name='wide-delta'",
                [],
            )?;
            let loaded = load(&server, name, "incremental").await?;
            assert_eq!(loaded["status"], "succeeded", "{loaded}");
            assert!(loaded["loaded_rows"].as_u64().unwrap() > 1024);
            let manifest: Value = serde_json::from_slice(&fs::read(path)?[6..])?;
            assert_eq!(manifest["parts"][0]["id"], base_id);
            // Leave the remainder of this test on a fresh checkpoint.
            db.execute(
                "UPDATE accounts SET version=version-5000 WHERE name='wide-delta'",
                [],
            )?;
            load(&server, name, "full").await?;
        }
        let snapshot: Value = serde_json::from_slice(&fs::read(path)?[6..])?;
        let base_id = snapshot["parts"][0]["id"].clone();

        let mapping = root.path().join(format!("{name}-replica.yaml"));
        fs::write(
            &mapping,
            format!(
                "graph: replica\nsources:\n  copy: {{type: parquet, tables: {{items: ['{}']}}}}\nnodes:\n  Person: {{source: copy.items, key: [tenant,id], properties: [name,version]}}\n",
                path.display()
            ),
        )?;
        let engine = Engine::new(Catalog::load(&mapping)?)?;
        let query =
            r#"SELECT name, version FROM "Person" WHERE id='00000000-0000-4000-8000-000000000001'"#;
        let shift = if name == "uuid" { 4000 } else { 4100 };
        let mut held = None;
        let mut held_part = None;
        for round in 1..=18 {
            let version = shift + round;
            db.execute("UPDATE accounts SET name=?1, version=?2 WHERE id='00000000-0000-4000-8000-000000000001'",
                rusqlite::params![format!("change-{version}"), version])?;
            let completed = load(&server, name, "incremental").await?;
            assert_eq!(completed["status"], "succeeded", "{completed}");
            assert_eq!(completed["total_rows"], 4000);
            let manifest: Value = serde_json::from_slice(&fs::read(path)?[6..])?;
            assert_eq!(manifest["parts"][0]["id"], base_id);
            assert!(manifest["parts"].as_array().unwrap().len() <= 16);
            if round == 1 {
                held = Some(
                    engine
                        .query(Language::Sql, query, Default::default())
                        .await?,
                );
                let delta = manifest["parts"][1]["id"].as_str().unwrap();
                held_part = Some(
                    path.with_extension("parts")
                        .join(format!("{delta}.parquet")),
                );
            } else if round == 16 {
                assert_eq!(manifest["parts"].as_array().unwrap().len(), 2);
                assert!(held_part.as_ref().unwrap().is_file());
            }
        }
        let mut old = held.take().unwrap();
        let old = old
            .batches
            .try_next()
            .await?
            .context("old indexed query missing row")?;
        let completed = load(&server, name, "incremental").await?;
        assert_eq!(completed["status"], "succeeded", "{completed}");
        assert_eq!(
            old.column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            shift + 1
        );
        drop(server);
        server = Server::start(root.path(), "", 0)?;
        server.login().await?;
        db.execute("UPDATE accounts SET name='restarted', version=?1 WHERE id='00000000-0000-4000-8000-000000000001'", [shift + 19])?;
        let run = load(&server, name, "incremental").await?;
        assert_eq!(run["total_rows"], 4000, "{run}");
        assert!(run["committed_generation"].is_string());
        let mut current = engine
            .query(Language::Sql, query, Default::default())
            .await?;
        let current = current
            .batches
            .try_next()
            .await?
            .context("new indexed query missing row")?;
        assert_eq!(
            current
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            shift + 19
        );
    }
    Ok(())
}
