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
    let parts = Path::new(path).with_extension("parts");
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

    // Repeated writes of key 1 must mask its older delta-file positions. At
    // the part limit a full merge compacts the live rows and reclaims old files.
    for version in last + 2..=last + 16 {
        parquet_rows(&source, &[(1, "again", version)])?;
        let run = load(&server, "overlay", "incremental").await?;
        assert_eq!(run["status"], "succeeded", "{run}");
        assert_eq!(run["total_rows"], last + 1);
    }
    assert!(fs::read(path)?.starts_with(b"PAR1"));
    assert_eq!(fs::read_dir(parts)?.count(), 0);
    let result = rows(root.path(), path).await?;
    assert_eq!(result.len(), (last + 1) as usize);
    assert_eq!(result[0], (1, "again".into(), last + 16));
    assert_eq!(result.last(), Some(&(last + 1, "new".into(), last + 1)));
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
