use std::time::Duration;

use anyhow::{Result, ensure};
use futures::TryStreamExt;
use graphlake::{
    Catalog, Direction, Engine, Language, QueryOptions, QueryResult, ScalarValue, TraversalKind,
    TraversalStrategy,
};
use object_store::{ObjectStoreExt, aws::AmazonS3Builder, path::Path};

use super::{
    TWO_HOPS, accounts, catalog, expected_two_hops, parquet, rows, transfers, traversal_request,
};

#[tokio::test]
#[ignore = "requires GraphLake test PostgreSQL and S3 endpoints"]
async fn postgres_snapshot_and_parquet_s3_join() -> Result<()> {
    let dsn = std::env::var("GRAPHLAKE_TEST_POSTGRES")?;
    let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls).await?;
    let driver = tokio::spawn(connection);
    let schema = format!("graphlake_test_{}", std::process::id());
    client
        .batch_execute(&format!(
            "CREATE SCHEMA {schema};
         CREATE TABLE {schema}.accounts (
            tenant integer, id bigint, status text, balance numeric(38,2),
            PRIMARY KEY (tenant, id)
         );
         INSERT INTO {schema}.accounts VALUES
            (1, 1, 'active', 123456789012345678901234567890123456.78),
            (1, 2, 'active', -0.12), (1, 3, 'locked', NULL), (2, 2, 'other', 0);"
        ))
        .await?;
    let bucket = std::env::var("GRAPHLAKE_TEST_BUCKET")?;
    let store = AmazonS3Builder::from_env()
        .with_bucket_name(&bucket)
        .build()?;
    let file = Path::from(format!("{schema}/transfers.parquet"));
    store.put(&file, parquet(&transfers()?)?.into()).await?;
    let check = async {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("graph.yaml");
        std::fs::write(&path, catalog(
            &format!("crm.{schema}.accounts"),
            &format!("  crm:\n    type: postgres\n    dsn_env: GRAPHLAKE_TEST_POSTGRES\n  lake:\n    type: parquet\n    tables:\n      transfers: [s3://{bucket}/{file}]"),
            "key: [tenant, id]",
        ).replace("properties: [status]", "properties: [status, balance]"))?;
        let engine = Engine::new(Catalog::load(&path)?)?;
        let opts = QueryOptions { batch_size: 2, ..Default::default() };
        let mut snapshot = engine.query(Language::Sql,
            "SELECT status, CAST(balance AS VARCHAR) AS balance FROM \"Account\" WHERE tenant = 1 AND id = 1",
            opts.clone()).await?;
        client.batch_execute(&format!("UPDATE {schema}.accounts SET status = 'changed' WHERE tenant=1 AND id=1")).await?;
        assert_eq!(rows(&mut snapshot).await?, vec![serde_json::json!({
            "status": "active", "balance": "123456789012345678901234567890123456.78",
        })]);
        assert_eq!(snapshot.metrics().reads.postgres_rows, 1);

        let mut joined = engine.query(Language::Cypher, TWO_HOPS, opts.clone()).await?;
        assert_eq!(rows(&mut joined).await?, expected_two_hops());
        assert_eq!(joined.versions.len(), 2);
        assert!(joined.metrics().reads.object_bytes > 0);
        assert!(joined.metrics().reads.postgres_rows > 0);

        client.batch_execute(&format!(
            "CREATE VIEW {schema}.slow_accounts AS
             SELECT 1::int AS tenant, 1::bigint AS id, 'slow'::text AS status,
                    NULL::numeric(38,2) AS balance FROM pg_sleep(30)"
        )).await?;
        let slow = std::fs::read_to_string(&path)?.replace(
            &format!("crm.{schema}.accounts"), &format!("crm.{schema}.slow_accounts"),
        );
        std::fs::write(&path, slow)?;
        let slow = Engine::new(Catalog::load(&path)?)?;
        let slow_opts = QueryOptions { cancellation: Default::default(), ..opts.clone() };
        let cancellation = slow_opts.cancellation.clone();
        let mut waiting = slow.query(Language::Sql,
            "SELECT * FROM \"Account\"", slow_opts).await?;
        let reading = tokio::spawn(async move {
            let result = waiting.batches.try_next().await;
            (result, waiting)
        });
        let started = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let active: bool = client.query_one(
                    "SELECT EXISTS (SELECT FROM pg_stat_activity
                     WHERE application_name = 'graphlake-validation' AND wait_event = 'PgSleep')",
                    &[],
                ).await?.try_get(0)?;
                if active { return Ok::<(), anyhow::Error>(()); }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }).await;
        cancellation.cancel();
        let (result, _held) = tokio::time::timeout(Duration::from_secs(3), reading).await??;
        started??;
        assert!(result.unwrap_err().to_string().contains("cancelled"));

        let mut abandoned = engine.query(Language::Sql,
            "SELECT * FROM \"Account\"", opts).await?;
        ensure!(abandoned.batches.try_next().await?.is_some(), "expected first PostgreSQL batch");
        drop(abandoned);
        let settled = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let open: i64 = client.query_one(
                    "SELECT count(*) FROM pg_stat_activity
                     WHERE application_name = 'graphlake-validation' AND pid <> pg_backend_pid()",
                    &[],
                ).await?.try_get(0)?;
                if open == 0 {
                    return Ok::<(), anyhow::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }).await;
        settled??;
        check_frontier(&client, &schema, dir.path()).await?;
        Ok::<(), anyhow::Error>(())
    }.await;
    let cleanup = client
        .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
        .await;
    let remove = store.delete(&file).await;
    drop(client);
    driver.await??;
    check?;
    cleanup?;
    remove?;
    Ok(())
}

async fn check_frontier(
    client: &tokio_postgres::Client,
    schema: &str,
    dir: &std::path::Path,
) -> Result<()> {
    client.batch_execute(&format!(
        "CREATE TABLE {schema}.transfers (tenant int, id bigint, src bigint, dst bigint, amount bigint, PRIMARY KEY(tenant,id));
         INSERT INTO {schema}.transfers VALUES
           (1,10,1,2,10),(1,11,1,2,20),(1,12,2,1,30),(1,13,2,3,40),
           (1,14,1,1,50),(1,15,1,9,60),(2,10,2,2,70);
         INSERT INTO {schema}.accounts SELECT 3,id,'active',0 FROM generate_series(1,500) id;
         INSERT INTO {schema}.transfers SELECT 3,id,1,id,1 FROM generate_series(2,500) id;
         INSERT INTO {schema}.transfers SELECT 4,id,id,id+1,1 FROM generate_series(1,2000) id;
         CREATE INDEX ON {schema}.transfers (tenant,src);
         CREATE INDEX ON {schema}.transfers (tenant,dst);
         ANALYZE {schema}.accounts;
         ANALYZE {schema}.transfers;"
    )).await?;
    let mapping = catalog(
        &format!("crm.{schema}.accounts"),
        "  crm:
    type: postgres
    dsn_env: GRAPHLAKE_TEST_POSTGRES",
        "key: [tenant, id]",
    )
    .replace("lake.transfers", &format!("crm.{schema}.transfers"));
    let path = dir.join("frontier.yaml");
    std::fs::write(&path, mapping)?;
    let engine =
        Engine::new(Catalog::load(&path)?)?.with_cache(dir.join("remote-csr"), "validation")?;
    let opts = QueryOptions {
        batch_size: 32,
        ..Default::default()
    };
    let request = traversal_request(Direction::Outgoing, TraversalKind::Paths { min_hops: 1 });
    let mut scan = engine
        .traverse_with_strategy(request.clone(), opts.clone(), TraversalStrategy::Scan)
        .await?;
    let mut expected = rows(&mut scan).await?;
    expected.sort_by_key(ToString::to_string);
    let mut batch = engine
        .traverse_with_strategy(request.clone(), opts.clone(), TraversalStrategy::Batch)
        .await?;
    let mut actual = rows(&mut batch).await?;
    actual.sort_by_key(ToString::to_string);
    assert_eq!(actual, expected);
    assert_eq!(scan.versions[0].version, batch.versions[0].version);
    assert!(batch.metrics().reads.frontier_requests > 0);
    assert!(batch.metrics().reads.postgres_rows < scan.metrics().reads.postgres_rows / 10);

    let mut cypher = engine
        .query(
            Language::Cypher,
            "MATCH p=(a:Account)-[:Transfer*1..2]->(b:Account)
         WHERE a.tenant=1 AND a.id=1 RETURN length(p) AS hops, count(*) AS paths ORDER BY hops",
            opts.clone(),
        )
        .await?;
    assert_eq!(
        rows(&mut cypher).await?,
        vec![
            serde_json::json!({"hops": 1, "paths": 3}),
            serde_json::json!({"hops": 2, "paths": 6}),
        ]
    );
    assert!(cypher.metrics().reads.frontier_requests > 0);

    let mut broad = traversal_request(Direction::Outgoing, TraversalKind::Reachable);
    broad.start[0] = ScalarValue::Int32(Some(3));
    let mut auto = engine.traverse(broad, opts.clone()).await?;
    assert_eq!(rows(&mut auto).await?.len(), 500);
    assert_eq!(auto.metrics().reads.frontier_scan_fallbacks, 1);

    let refresh = engine.refresh("Transfer", opts.clone()).await?;
    let mut cached = engine.traverse(request, opts).await?;
    let mut result = rows(&mut cached).await?;
    result.sort_by_key(ToString::to_string);
    assert_eq!(result, expected);
    assert_eq!(cached.metrics().reads.requests, 0);
    eprintln!(
        "phase2 scan={:?} batch={:?} refresh={refresh:?} cached={:?}",
        scan.metrics(),
        batch.metrics(),
        cached.metrics()
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires a REST Iceberg fixture from tests/iceberg_fixture.py"]
async fn iceberg_rest_snapshot_applies_position_and_equality_deletes() -> Result<()> {
    #[derive(serde::Deserialize)]
    struct Fixture {
        uri: String,
        warehouse: String,
        table: String,
        previous: i64,
        current: i64,
    }
    let fixture: Fixture = serde_json::from_slice(&std::fs::read(std::env::var(
        "GRAPHLAKE_TEST_ICEBERG_FIXTURE",
    )?)?)?;
    let dir = tempfile::tempdir()?;
    std::fs::write(dir.path().join("accounts.parquet"), parquet(&accounts()?)?)?;
    let path = dir.path().join("graph.yaml");
    let sources = format!(
        "  accounts:
    type: parquet
    tables:
      accounts: [accounts.parquet]
  lake:
    type: iceberg
    uri: {}
    warehouse: {}",
        serde_json::to_string(&fixture.uri)?,
        serde_json::to_string(&fixture.warehouse)?,
    );
    let config = |sources: &str| {
        catalog("accounts.accounts", sources, "key: [tenant, id]")
            .replace("lake.transfers", &format!("lake.{}", fixture.table))
    };
    std::fs::write(&path, config(&sources))?;
    let live = Engine::new(Catalog::load(&path)?)?;
    std::fs::write(
        &path,
        config(&format!(
            "{sources}
    snapshots:
      {}: {}",
            fixture.table, fixture.previous
        )),
    )?;
    let historical = Engine::new(Catalog::load(&path)?)?;
    let opts = QueryOptions::default();
    let query = "SELECT tenant, id FROM \"Transfer\" ORDER BY tenant, id";
    let mut latest = live.query(Language::Sql, query, opts.clone()).await?;
    let mut old = historical.query(Language::Sql, query, opts.clone()).await?;
    let latest_rows = rows(&mut latest).await?;
    let old_rows = rows(&mut old).await?;
    assert_eq!(old_rows.len(), 6);
    assert_eq!(
        latest_rows,
        vec![
            serde_json::json!({"tenant": 1, "id": 12}),
            serde_json::json!({"tenant": 1, "id": 13}),
            serde_json::json!({"tenant": 1, "id": 14}),
            serde_json::json!({"tenant": 2, "id": 10}),
        ]
    );
    let snapshot = |result: &QueryResult| {
        result
            .versions
            .iter()
            .find(|version| version.consistency == "Iceberg snapshot")
            .unwrap()
            .version
            .clone()
    };
    assert_eq!(snapshot(&latest), fixture.current.to_string());
    assert_eq!(snapshot(&old), fixture.previous.to_string());
    assert!(latest.metrics().reads.iceberg_rows > 0);
    let mut partitioned = opts.clone();
    partitioned
        .graph
        .logical_partition
        .push(graphlake::Predicate {
            column: "tenant".into(),
            op: graphlake::Comparison::Eq,
            value: 2.into(),
        });
    let mut filtered = live.query(Language::Sql, query, partitioned).await?;
    assert_eq!(
        rows(&mut filtered).await?,
        vec![serde_json::json!({"tenant": 2, "id": 10})]
    );
    assert!(filtered.metrics().reads.iceberg_rows < latest.metrics().reads.iceberg_rows);
    let mut result = historical
        .query(Language::Cypher, TWO_HOPS, opts.clone())
        .await?;
    assert_eq!(rows(&mut result).await?, expected_two_hops());
    let mut ranged = live
        .query(
            Language::Cypher,
            "MATCH (a:Account {tenant: 1, id: 1})-[:Transfer*1..2]->(b:Account)
         RETURN b.id AS destination",
            opts,
        )
        .await?;
    assert_eq!(
        rows(&mut ranged).await?,
        vec![serde_json::json!({"destination": 1})]
    );
    Ok(())
}

#[tokio::test]
async fn text_sources_join_across_files_and_keep_materialized_rows() -> Result<()> {
    let dir = tempfile::tempdir()?;
    std::fs::write(
        dir.path().join("accounts.csv"),
        "tenant,id,status\n1,1,active\n1,2,active\n1,3,locked\n2,2,other\n",
    )?;
    let transfers = [
        (1, 10, 1, 2, 10),
        (1, 11, 1, 2, 20),
        (1, 12, 2, 1, 30),
        (1, 13, 2, 3, 40),
        (1, 14, 1, 1, 50),
        (1, 15, 1, 9, 60),
        (2, 10, 2, 2, 70),
    ]
    .into_iter()
    .map(|(tenant, id, src, dst, amount)| {
        serde_json::json!({"tenant":tenant,"id":id,"src":src,"dst":dst,"amount":amount}).to_string()
    })
    .collect::<Vec<_>>();
    std::fs::write(dir.path().join("a.jsonl"), transfers[..3].join("\n"))?;
    std::fs::write(dir.path().join("b.jsonl"), transfers[3..].join("\n"))?;
    let path = dir.path().join("graph.yaml");
    std::fs::write(
        &path,
        catalog(
            "crm.accounts",
            r#"  crm:
    type: csv
    tables:
      accounts:
        files: [accounts.csv]
        columns:
          - {name: tenant, data_type: Int32}
          - {name: id, data_type: Int64}
          - {name: status, data_type: Utf8}
  lake:
    type: jsonl
    tables:
      transfers:
        files: [a.jsonl, b.jsonl]
        columns:
          - {name: tenant, data_type: Int32}
          - {name: id, data_type: Int64}
          - {name: src, data_type: Int64}
          - {name: dst, data_type: Int64}
          - {name: amount, data_type: Int64}"#,
            "key: [tenant, id]",
        ),
    )?;
    let engine = Engine::new(Catalog::load(&path)?)?;
    let mut result = engine
        .query(
            Language::Cypher,
            TWO_HOPS,
            QueryOptions {
                batch_size: 2,
                ..Default::default()
            },
        )
        .await?;
    std::fs::write(
        dir.path().join("accounts.csv"),
        "tenant,id,status\n1,1,changed\n",
    )?;
    assert_eq!(rows(&mut result).await?, expected_two_hops());
    assert_eq!(result.versions.len(), 3);
    assert!(result.metrics().reads.object_bytes > 0);
    Ok(())
}

#[tokio::test]
async fn text_sources_preserve_types_and_reject_invalid_records() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("graph.yaml");
    let long = format!("{},\n你好", "x".repeat(70_000));
    for (kind, valid, invalid) in [
        ("csv", format!("id,name,amount,enabled\n9007199254740993,\"{long}\",12345678901234567890.12,true\n2,,0.01,\n"), "name,id,amount,enabled\nwrong,1,0.01,true\n".to_owned()),
        ("jsonl", [serde_json::json!({"id":9007199254740993_i64,"name":long,"amount":"12345678901234567890.12","enabled":true}).to_string(),
            r#"{"id":2,"amount":"0.01"}"#.into()].join("\n"), r#"{"id":1,"name":"ok","amount":"0.01"}
{"id":"bad","amount":"0.02"}"#.into()),
    ] {
        std::fs::write(dir.path().join("data"), valid)?;
        std::fs::write(&path, format!(r#"graph: text
sources:
  files:
    type: {kind}
    tables:
      records:
        files: [data]
        columns:
          - {{name: id, data_type: Int64, nullable: false}}
          - {{name: name, data_type: Utf8}}
          - {{name: amount, data_type: 'Decimal128(38, 2)'}}
          - {{name: enabled, data_type: Boolean}}
nodes:
  Record:
    source: files.records
    key: [id]
    properties: [name, amount, enabled]
"#))?;
        let engine = Engine::new(Catalog::load(&path)?)?;
        let query = "SELECT id, name, CAST(amount AS VARCHAR) AS amount, enabled FROM \"Record\" ORDER BY id DESC";
        let mut result = engine.query(Language::Sql, query, QueryOptions { batch_size: 1, ..Default::default() }).await?;
        assert_eq!(rows(&mut result).await?, vec![
            serde_json::json!({"id":9007199254740993_i64,"name":long,"amount":"12345678901234567890.12","enabled":true}),
            serde_json::json!({"id":2,"amount":"0.01"}),
        ], "{kind}");
        std::fs::write(dir.path().join("data"), invalid)?;
        assert!(engine.query(Language::Sql, query, Default::default()).await.is_err(), "{kind}");
        std::fs::write(dir.path().join("data"), if kind == "csv" { "id,name,amount,enabled\n" } else { "" })?;
        let mut empty = engine.query(Language::Sql, query, Default::default()).await?;
        assert!(rows(&mut empty).await?.is_empty());
    }
    Ok(())
}

#[tokio::test]
async fn text_sources_spill_and_enforce_decoder_memory() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("graph.yaml");
    std::fs::write(
        &path,
        r#"graph: bounded
sources:
  files:
    type: csv
    tables:
      records:
        files: [data.csv]
        columns: [{name: id, data_type: Int64}, {name: payload, data_type: Utf8}]
nodes:
  Record: {source: files.records, key: [id], properties: [payload]}
"#,
    )?;
    let mut data = "id,payload\n".to_owned();
    for id in 0..20_000 {
        data.push_str(&format!("{id},{}\n", "x".repeat(500)));
    }
    std::fs::write(dir.path().join("data.csv"), &data)?;
    let engine = Engine::new(Catalog::load(&path)?)?;
    let options = QueryOptions {
        memory_bytes: 8 * 1024 * 1024,
        batch_size: 128,
        ..Default::default()
    };
    let mut result = engine
        .query(
            Language::Sql,
            "SELECT count(*) AS n, sum(length(payload)) AS bytes FROM \"Record\"",
            options.clone(),
        )
        .await?;
    assert_eq!(
        rows(&mut result).await?,
        vec![serde_json::json!({"n":20000,"bytes":10000000})]
    );
    assert!(result.metrics().reads.snapshot_spilled_bytes > 0);
    drop(result);
    std::fs::write(
        dir.path().join("data.csv"),
        format!("id,payload\n1,{}\n", "x".repeat(8 * 1024 * 1024)),
    )?;
    assert!(
        engine
            .query(Language::Sql, "SELECT payload FROM \"Record\"", options)
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn sqlite_sources_query_typed_rows_and_do_not_create_files() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let db = dir.path().join("source.db");
    let connection = rusqlite::Connection::open(&db)?;
    connection.execute_batch("PRAGMA journal_mode=WAL;
        CREATE TABLE accounts (tenant INT, id BIGINT, status TEXT, enabled BOOLEAN, bytes BLOB);
        INSERT INTO accounts VALUES (1,1,'active',1,x'00ff'), (1,2,'active',0,NULL), (1,3,'locked',NULL,x''), (2,2,'other',1,x'12');
        CREATE TABLE transfers (tenant INT, id BIGINT, src BIGINT, dst BIGINT, amount BIGINT);
        INSERT INTO transfers VALUES (1,10,1,2,10),(1,11,1,2,20),(1,12,2,1,30),(1,13,2,3,40),(1,14,1,1,50),(1,15,1,9,60),(2,10,2,2,70);")?;
    let path = dir.path().join("graph.yaml");
    std::fs::write(
        &path,
        catalog(
            "lake.accounts",
            "  lake: {type: sqlite, path: source.db}",
            "key: [tenant, id]",
        )
        .replace(
            "properties: [status]",
            "properties: [status, enabled, bytes]",
        ),
    )?;
    let engine = Engine::new(Catalog::load(&path)?)?;
    let mut options = QueryOptions {
        batch_size: 2,
        ..Default::default()
    };
    options.graph.require_consistent_snapshot = true;
    let mut result = engine.query(Language::Cypher, TWO_HOPS, options).await?;
    assert_eq!(rows(&mut result).await?, expected_two_hops());
    assert_eq!(result.versions.len(), 1);
    assert_eq!(result.versions[0].consistency, "SQLite read transaction");
    assert!(result.metrics().reads.sqlite_rows > 0);
    let mut result = engine
        .query(
            Language::Sql,
            "SELECT enabled, bytes FROM \"Account\" WHERE tenant=1 AND id=1",
            Default::default(),
        )
        .await?;
    let result = rows(&mut result).await?;
    assert_eq!(result[0]["enabled"], true);
    assert!(!result[0]["bytes"].is_null());
    connection.execute_batch("UPDATE accounts SET enabled=2 WHERE id=1")?;
    assert!(
        engine
            .query(
                Language::Sql,
                "SELECT enabled FROM \"Account\"",
                Default::default()
            )
            .await
            .is_err()
    );
    let yaml = std::fs::read_to_string(&path)?.replace("source.db", "missing.db");
    std::fs::write(&path, yaml)?;
    let missing = Engine::new(Catalog::load(&path)?)?;
    assert!(
        missing
            .query(
                Language::Sql,
                "SELECT * FROM \"Account\"",
                Default::default()
            )
            .await
            .is_err()
    );
    assert!(!dir.path().join("missing.db").exists());
    Ok(())
}

#[tokio::test]
#[ignore = "requires a writable GraphLake test S3 bucket"]
async fn text_sources_s3_keep_versions_and_read_new_objects_on_next_query() -> Result<()> {
    let bucket = std::env::var("GRAPHLAKE_TEST_BUCKET")?;
    let store = AmazonS3Builder::from_env()
        .with_bucket_name(&bucket)
        .build()?;
    let object = Path::from(format!(
        "text-integration/{}/records.jsonl",
        uuid::Uuid::new_v4()
    ));
    store
        .put(
            &object,
            b"{\"id\":1,\"value\":\"before\"}\n".to_vec().into(),
        )
        .await?;
    let result = async {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("graph.yaml");
        std::fs::write(
            &path,
            format!(
                r#"graph: s3
sources:
  files:
    type: jsonl
    tables:
      records:
        files: [s3://{bucket}/{object}]
        columns: [{{name: id, data_type: Int64}}, {{name: value, data_type: Utf8}}]
nodes:
  Record: {{source: files.records, key: [id], properties: [value]}}
"#
            ),
        )?;
        let engine = Engine::new(Catalog::load(path)?)?;
        let mut first = engine
            .query(
                Language::Sql,
                "SELECT value FROM \"Record\"",
                Default::default(),
            )
            .await?;
        store
            .put(&object, b"{\"id\":1,\"value\":\"after\"}\n".to_vec().into())
            .await?;
        assert_eq!(
            rows(&mut first).await?,
            vec![serde_json::json!({"value":"before"})]
        );
        let mut second = engine
            .query(
                Language::Sql,
                "SELECT value FROM \"Record\"",
                Default::default(),
            )
            .await?;
        assert_eq!(
            rows(&mut second).await?,
            vec![serde_json::json!({"value":"after"})]
        );
        assert_ne!(first.versions[0].version, second.versions[0].version);
        Ok::<(), anyhow::Error>(())
    }
    .await;
    store.delete(&object).await?;
    result
}

#[tokio::test]
#[ignore = "requires an S3-compatible test bucket and explicit GRAPHLAKE_TEST_STORAGE_* credentials"]
async fn object_storage_sources_use_connection_credentials_for_parquet_csv_and_jsonl() -> Result<()>
{
    let endpoint = std::env::var("GRAPHLAKE_TEST_STORAGE_ENDPOINT")?;
    let bucket = std::env::var("GRAPHLAKE_TEST_STORAGE_BUCKET")?;
    let store = AmazonS3Builder::new()
        .with_endpoint(&endpoint)
        .with_bucket_name(&bucket)
        .with_region("us-east-1")
        .with_allow_http(true)
        .with_access_key_id(std::env::var("GRAPHLAKE_TEST_STORAGE_KEY")?)
        .with_secret_access_key(std::env::var("GRAPHLAKE_TEST_STORAGE_SECRET")?)
        .build()?;
    let prefix = format!("sources/{}/", uuid::Uuid::new_v4());
    let files = [
        format!("{prefix}accounts.parquet"),
        format!("{prefix}accounts.csv"),
        format!("{prefix}accounts.jsonl"),
    ];
    store
        .put(
            &Path::from(files[0].clone()),
            super::parquet(&super::accounts()?)?.into(),
        )
        .await?;
    let mut csv = String::from("tenant,id,status\n1,1,active\n");
    let payload = "x".repeat(256);
    for id in 2..1002 {
        use std::fmt::Write;
        writeln!(csv, "1,{id},{payload}")?;
    }
    let csv_bytes = csv.len() as u64;
    store
        .put(&Path::from(files[1].clone()), csv.into_bytes().into())
        .await?;
    store
        .put(
            &Path::from(files[2].clone()),
            b"{\"tenant\":1,\"id\":1,\"status\":\"active\"}\n"
                .to_vec()
                .into(),
        )
        .await?;
    let result = async {
        for (index, format) in ["parquet", "csv", "jsonl"].into_iter().enumerate() {
            let schema = if format == "parquet" { "" } else { "        columns: [{name: tenant, data_type: Int64}, {name: id, data_type: Int64}, {name: status, data_type: Utf8}]\n" };
            let catalog: Catalog = serde_saphyr::from_str(&format!("graph: storage\nsources:\n  store:\n    type: object_storage\n    storage:\n      provider: minio\n      endpoint: '{endpoint}'\n      bucket: '{bucket}'\n      access_key_env: GRAPHLAKE_TEST_STORAGE_KEY\n      secret_key_env: GRAPHLAKE_TEST_STORAGE_SECRET\n    format: {format}\n    tables:\n      accounts:\n        files: ['{}']\n{schema}nodes:\n  Account: {{source: store.accounts, key: [tenant, id], properties: [status]}}\n", files[index]))?;
            let engine = Engine::new(catalog)?;
            let mut result = engine.query(Language::Sql, "SELECT status FROM \"Account\" WHERE tenant=1 AND id=1", Default::default()).await?;
            assert_eq!(rows(&mut result).await?, vec![serde_json::json!({"status":"active"})]);
            assert_eq!(result.versions.len(), 1);
            assert!(!result.versions[0].version.is_empty());
            if format == "csv" {
                let reads = result.metrics().reads;
                assert_eq!(reads.object_bytes, csv_bytes);
                assert!(reads.requests <= 3, "large CSV used {} range requests", reads.requests);
            }
        }
        Ok::<(), anyhow::Error>(())
    }.await;
    for file in files {
        store.delete(&Path::from(file)).await?;
    }
    result
}

#[tokio::test]
#[ignore = "requires a disposable PostgreSQL instance in GRAPHLAKE_TEST_POSTGRES"]
async fn postgres_pushdown_preserves_null_keys_and_filtered_order() -> Result<()> {
    let dsn = std::env::var("GRAPHLAKE_TEST_POSTGRES")?;
    let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls).await?;
    let connection = tokio::spawn(connection);
    let schema = format!("graphlake_shape_{}", uuid::Uuid::new_v4().simple());
    client
        .batch_execute(&format!(
            "CREATE SCHEMA {schema};
         CREATE TABLE {schema}.accounts (id BIGINT UNIQUE, amount INTEGER);
         INSERT INTO {schema}.accounts VALUES (NULL,999),(NULL,888),(1,8),(2,NULL),(3,-1);"
        ))
        .await?;
    let checked = async {
        let catalog: Catalog = serde_saphyr::from_str(&format!(
            "graph: pg\nsources:\n  db: {{type: postgres, dsn_env: GRAPHLAKE_TEST_POSTGRES}}\nnodes:\n  Account: {{source: db.{schema}.accounts, key: [id], properties: [amount]}}"
        ))?;
        let engine = Engine::new(catalog)?;
        for (sql, control, expected) in [
            (r#"SELECT id FROM "Account" WHERE amount IS NOT NULL ORDER BY id DESC LIMIT 1"#,
             r#"WITH a AS (SELECT * FROM "Account") SELECT id FROM a WHERE amount IS NOT NULL ORDER BY id DESC LIMIT 1"#,
             serde_json::json!({"id":3})),
            (r#"SELECT COUNT(*) AS n FROM "Account""#,
             r#"WITH a AS (SELECT * FROM "Account") SELECT COUNT(*) AS n FROM a"#,
             serde_json::json!({"n":3})),
            (r#"SELECT MAX(amount) AS n FROM "Account""#,
             r#"WITH a AS (SELECT * FROM "Account") SELECT MAX(amount) AS n FROM a"#,
             serde_json::json!({"n":8})),
        ] {
            let mut fast = engine.query(Language::Sql, sql, Default::default()).await?;
            let mut control = engine.query(Language::Sql, control, Default::default()).await?;
            let result = rows(&mut fast).await?;
            assert_eq!(result, vec![expected]);
            assert_eq!(result, rows(&mut control).await?);
            assert_eq!(fast.metrics().reads.postgres_rows, 1);
        }
        Ok::<_, anyhow::Error>(())
    }.await;
    client
        .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
        .await?;
    drop(client);
    connection.await??;
    checked
}
