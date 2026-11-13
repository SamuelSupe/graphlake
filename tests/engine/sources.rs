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
