use std::{collections::BTreeMap, time::Duration};

use anyhow::{Context, Result};
use futures::TryStreamExt;
use graphlake::{
    Catalog, Engine, Language, QueryOptions,
    arrow::array::{Array, BinaryArray, Decimal128Array, TimestampMicrosecondArray},
};

use super::{TWO_HOPS, expected_two_hops, rows};

const VERSION: &str = "2026-11-10T09:00:00.123456Z";

fn source(kind: &str, uri: &str) -> String {
    let settings = match kind {
        "snowflake" => "    warehouse: WH_TEST\n    role: READ_TEST",
        "trino" => "    user: graph-reader",
        _ => {
            "    billing_project: billing-test\n    location: US\n    maximum_bytes_billed: 1000000"
        }
    };
    format!(
        "    type: {kind}\n    uri: {uri}\n    token_env: GRAPHLAKE_TEST_WAREHOUSE_TOKEN\n{settings}"
    )
}

fn engine(mapping: &str) -> Result<Engine> {
    let catalog: Catalog = serde_saphyr::from_str(mapping)?;
    Engine::new(catalog)
}

#[tokio::test]
#[ignore = "requires the loopback warehouse API fixture"]
async fn warehouses_join_time_travel_pages_and_decode_exact_types() -> Result<()> {
    let uri = std::env::var("GRAPHLAKE_TEST_WAREHOUSE_URI")?;
    let snowflake = source("snowflake", &uri);
    let bigquery = source("bigquery", &uri);
    let engine = engine(&format!(
        "graph: finance
sources:
  sf:
{snowflake}
  bq:
{bigquery}
nodes:
  Account:
    source: bq.data-test.sales.accounts
    key: [tenant, id]
    properties: [status, balance, created]
edges:
  Transfer:
    source: sf.db.sales.transfers
    key: [tenant, id]
    from: {{node: Account, columns: [tenant, src]}}
    to: {{node: Account, columns: [tenant, dst]}}
    properties: [amount]"
    ))?;
    let options = QueryOptions {
        batch_size: 2,
        ..Default::default()
    };
    let mut explain = engine
        .query(
            Language::Cypher,
            &format!("EXPLAIN {TWO_HOPS}"),
            options.clone(),
        )
        .await?;
    assert!(!rows(&mut explain).await?.is_empty());
    assert_eq!(explain.metrics().reads.warehouse_rows, 0);
    let mut result = engine
        .query(Language::Cypher, TWO_HOPS, options.clone())
        .await?;
    assert_eq!(rows(&mut result).await?, expected_two_hops());
    assert_eq!(result.versions.len(), 2);
    assert!(
        result
            .versions
            .iter()
            .all(|version| version.version == VERSION)
    );
    let baseline = result.metrics().reads.warehouse_rows;
    let mut partitioned = options.clone();
    partitioned
        .graph
        .logical_partition
        .push(graphlake::Predicate {
            column: "tenant".into(),
            op: graphlake::Comparison::Eq,
            value: 1.into(),
        });
    let mut filtered = engine
        .query(Language::Cypher, TWO_HOPS, partitioned)
        .await?;
    assert_eq!(rows(&mut filtered).await?, expected_two_hops());
    assert_eq!(filtered.metrics().reads.warehouse_rows, baseline - 2);

    let mut result = engine
        .query(
            Language::Sql,
            r#"SELECT balance, created FROM "Account" WHERE tenant = 1 ORDER BY id LIMIT 2"#,
            options,
        )
        .await?;
    let batch = result.batches.try_next().await?.unwrap();
    let balances = batch
        .column(0)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    let timestamps = batch
        .column(1)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();
    assert_eq!(balances.value(0), 1525);
    assert_eq!(timestamps.value(0), 1794301200123456);
    let mut count = balances.null_count();
    while let Some(batch) = result.batches.try_next().await? {
        count += batch.column(0).null_count();
    }
    assert_eq!(count, 1);
    assert_eq!(result.metrics().reads.warehouse_rows, 3);
    Ok(())
}

#[tokio::test]
#[ignore = "requires the loopback warehouse API fixture"]
async fn warehouses_cancel_stalled_jobs_after_the_query_deadline() -> Result<()> {
    let uri = std::env::var("GRAPHLAKE_TEST_WAREHOUSE_URI")?;
    let endpoint = format!("{uri}/fixture/cancelled");
    let client = reqwest::Client::new();
    let cancelled = || async {
        client
            .get(&endpoint)
            .send()
            .await?
            .error_for_status()?
            .json::<BTreeMap<String, u64>>()
            .await
    };
    let before = cancelled().await?;
    for (kind, relation) in [
        ("snowflake", "db.sales"),
        ("bigquery", "data-test.sales"),
        ("trino", "lake.test"),
    ] {
        let settings = source(kind, &uri);
        let time_travel = if kind == "trino" {
            String::new()
        } else {
            format!("    as_of: '{VERSION}'\n")
        };
        let engine = engine(&format!(
            "graph: cancelled
sources:
  warehouse:
{settings}
{time_travel}nodes:
  Account:
    source: warehouse.{relation}.stalled
    key: [id]"
        ))?;
        let options = QueryOptions {
            timeout: Duration::from_secs(1),
            ..Default::default()
        };
        let Err(error) = engine
            .query(Language::Sql, r#"SELECT * FROM "Account""#, options)
            .await
        else {
            panic!("stalled {kind} query should time out");
        };
        assert!(error.to_string().contains("deadline"), "{error:#}");
    }
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let after = cancelled().await?;
            if ["bigquery", "snowflake", "trino"]
                .iter()
                .all(|kind| after.get(*kind).unwrap_or(&0) > before.get(*kind).unwrap_or(&0))
            {
                return Ok::<(), anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    })
    .await??;
    Ok(())
}

#[tokio::test]
#[ignore = "requires the loopback warehouse API fixture"]
async fn trino_checks_cursor_origin_and_decodes_typed_pages() -> Result<()> {
    let uri = std::env::var("GRAPHLAKE_TEST_WAREHOUSE_URI")?;
    let settings = source("trino", &uri);
    let catalog = |table| {
        format!(
            "graph: trino
sources:
  remote:
{settings}
nodes:
  Account:
    source: remote.lake.test.{table}
    key: [id]
    properties: [amount, recorded, payload]"
        )
    };
    let sql = r#"SELECT id, amount, recorded, payload FROM "Account" ORDER BY id"#;
    let mut result = engine(&catalog("accounts"))?
        .query(Language::Sql, sql, Default::default())
        .await?;
    assert_eq!(result.versions[0].consistency, "Trino statement");
    let records = rows(&mut result).await?;
    assert_eq!(records.len(), 2);
    assert_eq!(records[0]["id"], 9007199254740993_i64);
    assert_eq!(records[1]["id"], 9007199254740994_i64);
    assert_eq!(records[0]["amount"], 12.25);
    assert_eq!(records[0]["recorded"], "2026-01-01T19:04:05.123456789Z");
    assert_eq!(records[0]["recorded"], records[1]["recorded"]);
    assert_ne!(records[0]["payload"], records[1]["payload"]);
    let folded = engine(&catalog("dst_fold"))?;
    let mut cold = folded.query(Language::Sql, sql, Default::default()).await?;
    let expected = rows(&mut cold).await?;
    assert_eq!(expected[0]["recorded"], "2026-11-01T05:30:00.123456789Z");
    assert_eq!(expected[1]["recorded"], "2026-11-01T06:30:00.123456789Z");
    let mut warm = folded.query(Language::Sql, sql, Default::default()).await?;
    assert_eq!(rows(&mut warm).await?, expected);
    assert!(warm.metrics().reads.requests < cold.metrics().reads.requests);
    let Err(error) = engine(&catalog("foreign"))?
        .query(Language::Sql, sql, Default::default())
        .await
    else {
        panic!("foreign Trino cursor was accepted");
    };
    assert!(format!("{error:#}").contains("invalid Trino result cursor"));
    Ok(())
}

#[tokio::test]
#[ignore = "requires the loopback warehouse API fixture"]
async fn named_trino_connectors_qualify_the_catalog_and_preserve_results() -> Result<()> {
    let uri = std::env::var("GRAPHLAKE_TEST_WAREHOUSE_URI")?;
    for kind in [
        "mongodb",
        "elasticsearch",
        "redshift",
        "sqlserver",
        "oracle",
    ] {
        let table = if kind == "elasticsearch" {
            "accounts.2026.11"
        } else {
            "accounts"
        };
        let engine = engine(&format!(
            "graph: sources
sources:
  remote:
    type: {kind}
    uri: {uri}
    catalog: lake
    user: graph-reader
    token_env: GRAPHLAKE_TEST_WAREHOUSE_TOKEN
nodes:
  Account:
    source: remote.test.{table}
    key: [id]
    properties: [amount]"
        ))?;
        let mut result = engine
            .query(
                Language::Cypher,
                "MATCH (a:Account) RETURN a.id AS id, a.amount AS amount ORDER BY id",
                Default::default(),
            )
            .await?;
        let records = rows(&mut result).await?;
        assert_eq!(records.len(), 2, "{kind}");
        assert_eq!(records[0]["id"], 9007199254740993_i64);
        assert_eq!(records[0]["amount"], 12.25);
        assert!(records[1]["amount"].is_null());
    }
    let mongo = engine(&format!(
        "graph: documents
sources:
  remote:
    type: mongodb
    uri: {uri}
    catalog: lake
    user: graph-reader
    token_env: GRAPHLAKE_TEST_WAREHOUSE_TOKEN
nodes:
  Document:
    source: remote.test.accounts
    key: [_id]"
    ))?;
    let mut result = mongo
        .query(
            Language::Sql,
            r#"SELECT _id FROM "Document" ORDER BY _id"#,
            Default::default(),
        )
        .await?;
    let batch = result
        .batches
        .try_next()
        .await?
        .expect("expected MongoDB keys");
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .expect("binary ObjectId");
    assert_eq!(ids.value(0), &[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
    Ok(())
}

#[tokio::test]
#[ignore = "requires the Spanner emulator graphlake/test/finance fixture"]
async fn spanner_joins_tables_at_one_read_timestamp_and_pushes_integer_keys() -> Result<()> {
    let uri = std::env::var("GRAPHLAKE_TEST_SPANNER_URI")?;
    let mapping = format!(
        "graph: finance
sources:
  remote:
    type: spanner
    uri: {uri}
    database: projects/graphlake/instances/test/databases/finance
nodes:
  Account:
    source: remote.Accounts
    key: [id]
    properties: [status]
edges:
  Transfer:
    source: remote.Transfers
    key: [id]
    from: {{node: Account, columns: [src]}}
    to: {{node: Account, columns: [dst]}}
    properties: [amount, recorded]"
    );
    let mut options = QueryOptions::default();
    options.graph.require_consistent_snapshot = true;
    let precise = engine(&mapping)?;
    let error = precise
        .query(
            Language::Cypher,
            "MATCH (:Account)-[e:Transfer]->(:Account) RETURN e.recorded AS recorded",
            options.clone(),
        )
        .await
        .err()
        .context("an inexact timestamp must not be returned")?;
    let detail = format!("{error:#}");
    assert!(
        detail.contains("recorded") && detail.contains("microseconds"),
        "{detail}"
    );

    let engine = engine(&mapping.replace("[amount, recorded]", "[amount]"))?;
    let mut result = engine
        .query(
            Language::Cypher,
            "MATCH (a:Account)-[e:Transfer]->(b:Account) RETURN a.id AS id, b.status AS status, e.amount AS amount",
            options.clone(),
        )
        .await?;
    assert_eq!(result.versions.len(), 1);
    assert_eq!(
        result.versions[0].consistency,
        "Spanner read-only transaction"
    );
    chrono::DateTime::parse_from_rfc3339(&result.versions[0].version)?;
    let records = rows(&mut result).await?;
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["id"], 9007199254740993_i64);
    assert_eq!(records[0]["status"], "target");
    assert_eq!(records[0]["amount"], 12.250000001);

    let mut result = engine
        .query(
            Language::Cypher,
            "MATCH (a:Account) WHERE a.id = 9007199254740993 RETURN a.status AS status",
            options,
        )
        .await?;
    assert_eq!(rows(&mut result).await?[0]["status"], "source");
    assert_eq!(result.metrics().reads.warehouse_rows, 1);
    assert_eq!(result.metrics().reads.requests, 3);
    Ok(())
}

#[tokio::test]
#[ignore = "requires the loopback warehouse API fixture"]
async fn trino_pushes_key_filters_and_spills_large_snapshots_within_budget() -> Result<()> {
    let uri = std::env::var("GRAPHLAKE_TEST_WAREHOUSE_URI")?;
    let settings = source("trino", &uri);
    let engine = engine(&format!(
        "graph: trino
sources:
  remote:
{settings}
nodes:
  Account:
    source: remote.lake.test.large_accounts
    key: [id]
    properties: [name]"
    ))?;
    let options = QueryOptions {
        memory_bytes: 2 << 20,
        spill_bytes: 32 << 20,
        batch_size: 128,
        ..Default::default()
    };
    let sql = r#"SELECT count(*) AS n FROM "Account""#;
    let mut explain = engine
        .query(Language::Sql, &format!("EXPLAIN {sql}"), options.clone())
        .await?;
    assert!(!rows(&mut explain).await?.is_empty());
    assert_eq!(explain.metrics().reads.warehouse_rows, 0);
    let mut analyze = engine
        .query(
            Language::Sql,
            &format!("EXPLAIN ANALYZE {sql}"),
            options.clone(),
        )
        .await?;
    assert!(!rows(&mut analyze).await?.is_empty());
    assert_eq!(analyze.metrics().reads.warehouse_rows, 8192);
    // One statement submission and 64 pages, with no schema-only statement.
    assert_eq!(analyze.metrics().reads.requests, 65);
    for (language, query) in [
        (Language::Sql, r#"SELECT id FROM "Account" WHERE id = 42"#),
        (
            Language::Cypher,
            "MATCH (a:Account) WHERE a.id = 42 RETURN a.id AS id",
        ),
        (
            Language::Cypher,
            "MATCH (a:Account) WHERE a.id = 42 RETURN DISTINCT a.id AS id ORDER BY id",
        ),
    ] {
        let mut point = engine.query(language, query, options.clone()).await?;
        assert_eq!(rows(&mut point).await?, [serde_json::json!({"id":42})]);
        assert_eq!(point.metrics().reads.warehouse_rows, 1);
    }
    let mut nested = engine
        .query(
            Language::Sql,
            r#"SELECT id, (SELECT count(*) FROM "Account") AS n
           FROM (SELECT id FROM "Account" WHERE id = 42) AS filtered"#,
            options.clone(),
        )
        .await?;
    assert_eq!(
        rows(&mut nested).await?,
        [serde_json::json!({"id":42,"n":8192})]
    );
    assert_eq!(nested.metrics().reads.warehouse_rows, 8192);
    let wide_sql = r#"SELECT count(*) AS n, sum(length(name)) AS bytes FROM "Account""#;
    let mut result = engine
        .query(Language::Sql, wide_sql, options.clone())
        .await?;
    assert_eq!(
        rows(&mut result).await?,
        [serde_json::json!({"n":8192,"bytes":1572864})]
    );
    assert!(result.metrics().reads.snapshot_spilled_bytes > 0);
    let constrained = QueryOptions {
        spill_bytes: 1024,
        ..options
    };
    let Err(error) = engine.query(Language::Sql, wide_sql, constrained).await else {
        panic!("source spill exceeded its disk quota");
    };
    assert!(format!("{error:#}").contains("allowable limit"));
    Ok(())
}

#[tokio::test]
#[ignore = "requires ClickHouse with the readonly_reader fixture"]
async fn clickhouse_reads_arrow_with_a_locked_readonly_profile() -> Result<()> {
    let uri = std::env::var("GRAPHLAKE_TEST_CLICKHOUSE_URI")?;
    let engine = engine(&format!(
        "graph: clickhouse
sources:
  remote:
    type: clickhouse
    uri: {uri}
    user: readonly_reader
    password_env: GRAPHLAKE_TEST_CLICKHOUSE_PASSWORD
nodes:
  Account:
    source: remote.graphlake.nodes
    key: [id]
    properties: [amount, name, posted]
  StringKey:
    source: remote.graphlake.nodes
    key: [name]
    properties: [amount]"
    ))?;
    let mut result = engine
        .query(
            Language::Sql,
            r#"SELECT count(*) AS n, sum(amount) AS amount, min(name) AS name FROM "Account""#,
            QueryOptions {
                batch_size: 64,
                ..Default::default()
            },
        )
        .await?;
    assert_eq!(result.versions[0].consistency, "ClickHouse statement");
    assert_eq!(
        rows(&mut result).await?,
        [serde_json::json!({
            "n": 3000, "amount": 4499250.0, "name": "node-1"
        })]
    );
    let mut dates = engine
        .query(
            Language::Sql,
            r#"SELECT posted FROM "Account" WHERE id <= 2 ORDER BY id"#,
            Default::default(),
        )
        .await?;
    assert_eq!(
        dates.batches.schema().field(0).data_type(),
        &graphlake::arrow::datatypes::DataType::Date32
    );
    assert_eq!(
        rows(&mut dates).await?,
        [
            serde_json::json!({"posted": "2026-01-02"}),
            serde_json::json!({}),
        ]
    );
    for (value, count) in [("node-1", 1), (r#"测试''节点\path"#, 0)] {
        let mut filtered = engine
            .query(
                Language::Sql,
                &format!(r#"SELECT amount FROM "StringKey" WHERE name = '{value}'"#),
                Default::default(),
            )
            .await?;
        let mut control = engine.query(
            Language::Sql,
            &format!(r#"WITH full_scan AS (SELECT * FROM "StringKey") SELECT amount FROM full_scan WHERE name = '{value}'"#),
            Default::default(),
        ).await?;
        let expected = rows(&mut control).await?;
        assert_eq!(expected.len(), count);
        assert_eq!(rows(&mut filtered).await?, expected);
        assert_eq!(filtered.metrics().reads.warehouse_rows, count as u64);
        assert_eq!(control.metrics().reads.warehouse_rows, 3000);
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires the loopback warehouse API fixture"]
async fn databricks_preserves_pages_types_and_rejects_incomplete_results() -> Result<()> {
    let uri = std::env::var("GRAPHLAKE_TEST_WAREHOUSE_URI")?;
    for table in ["accounts", "foreign", "truncated", "incomplete"] {
        let engine = engine(&format!(
            "graph: databricks\nsources:\n  db: {{type: databricks, uri: '{uri}', token_env: GRAPHLAKE_TEST_WAREHOUSE_TOKEN, warehouse: WH_TEST, catalog: main}}\nnodes:\n  Account: {{source: db.sales.{table}, key: [tenant, id], properties: [balance, created]}}"
        ))?;
        let query = "SELECT balance, created FROM \"Account\" ORDER BY tenant, id";
        let result = engine
            .query(
                Language::Sql,
                query,
                QueryOptions {
                    batch_size: 2,
                    ..Default::default()
                },
            )
            .await;
        if table != "accounts" {
            assert!(result.is_err(), "{table} must not return partial results");
            continue;
        }
        let mut result = result?;
        let batch = result.batches.try_next().await?.unwrap();
        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap()
                .value(0),
            1525
        );
        assert_eq!(
            batch
                .column(1)
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap()
                .value(0),
            1794301200123456
        );
        let mut count = batch.num_rows();
        while let Some(batch) = result.batches.try_next().await? {
            count += batch.num_rows();
        }
        assert_eq!(count, 4);
        let mut point = engine
            .query(
                Language::Sql,
                "SELECT id FROM \"Account\" WHERE tenant = 1 AND id = 2",
                Default::default(),
            )
            .await?;
        assert_eq!(rows(&mut point).await?, vec![serde_json::json!({"id": 2})]);
        assert_eq!(point.metrics().reads.warehouse_rows, 1);
        let mut options = QueryOptions::default();
        options.graph.logical_partition.push(graphlake::Predicate {
            column: "balance".into(),
            op: graphlake::Comparison::IsNull,
            value: serde_json::Value::Null,
        });
        let mut null_balance = engine
            .query(Language::Sql, "SELECT id FROM \"Account\"", options)
            .await?;
        assert_eq!(
            rows(&mut null_balance).await?,
            vec![serde_json::json!({"id": 2})]
        );
        assert_eq!(null_balance.metrics().reads.warehouse_rows, 1);
        let mut explain = engine
            .query(
                Language::Sql,
                "EXPLAIN SELECT * FROM \"Account\"",
                Default::default(),
            )
            .await?;
        assert!(!rows(&mut explain).await?.is_empty());
        assert_eq!(explain.metrics().reads.warehouse_rows, 0);
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires the loopback warehouse API fixture"]
async fn databricks_cancels_on_query_timeout() -> Result<()> {
    let uri = std::env::var("GRAPHLAKE_TEST_WAREHOUSE_URI")?;
    let client = reqwest::Client::new();
    let before: BTreeMap<String, u64> = client
        .get(format!("{uri}/fixture/cancelled"))
        .send()
        .await?
        .json()
        .await?;
    let engine = engine(&format!(
        "graph: cancelled\nsources:\n  db: {{type: databricks, uri: '{uri}', token_env: GRAPHLAKE_TEST_WAREHOUSE_TOKEN, warehouse: WH_TEST, catalog: main}}\nnodes:\n  Account: {{source: db.sales.stalled, key: [id]}}"
    ))?;
    assert!(
        engine
            .query(
                Language::Sql,
                "SELECT * FROM \"Account\"",
                QueryOptions {
                    timeout: Duration::from_secs(1),
                    ..Default::default()
                }
            )
            .await
            .is_err()
    );
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let after: BTreeMap<String, u64> = client
                .get(format!("{uri}/fixture/cancelled"))
                .send()
                .await?
                .json()
                .await?;
            if after.get("databricks").unwrap_or(&0) > before.get("databricks").unwrap_or(&0) {
                return Ok::<(), anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    })
    .await??;
    Ok(())
}
