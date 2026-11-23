use std::{path::Path, sync::Arc};

use anyhow::{Context, Result};
use graphlake::{
    Catalog, Engine, GraphAlgorithm, Language, QueryOptions, TraversalStrategy,
    arrow::{
        array::{Int32Array, Int64Array, RecordBatch, StringArray},
        compute::concat_batches,
    },
};
use mysql_async::{Conn, Opts, prelude::Queryable};

use super::{TWO_HOPS, accounts, catalog, expected_two_hops, local_fixture, parquet, rows};

#[tokio::test]
async fn whole_graph_algorithms_agree_across_scan_and_csr() -> Result<()> {
    let (dir, engine) = local_fixture("key: [tenant, id]")?;
    let initial = accounts()?;
    let isolated = RecordBatch::try_from_iter([
        ("tenant", Arc::new(Int32Array::from(vec![2])) as _),
        ("id", Arc::new(Int64Array::from(vec![99])) as _),
        ("status", Arc::new(StringArray::from(vec!["isolated"])) as _),
    ])?;
    let vertices = concat_batches(&initial.schema(), &[initial.clone(), isolated])?;
    std::fs::write(dir.path().join("accounts.parquet"), parquet(&vertices)?)?;
    let engine = engine.with_cache(dir.path().join("cache"), "analytics")?;
    let options = QueryOptions {
        batch_size: 2,
        ..Default::default()
    };
    engine.refresh("Transfer", options.clone()).await?;

    for strategy in [TraversalStrategy::Scan, TraversalStrategy::Cached] {
        let mut result = engine
            .analyze_graph(
                "Transfer",
                GraphAlgorithm::PageRank {
                    damping: 0.85,
                    tolerance: 1e-11,
                    max_iterations: 1000,
                },
                options.clone(),
                strategy,
            )
            .await?;
        let mut values = rows(&mut result).await?;
        values.sort_by_key(|row| (row["tenant"].as_i64().unwrap(), row["id"].as_i64().unwrap()));
        assert_eq!(values.len(), 5);
        let expected = [
            0.19315122649,
            0.17394905778,
            0.13842504565,
            0.42997797398,
            0.06449669610,
        ];
        let ranks = values
            .iter()
            .map(|row| row["__graphlake_rank"].as_f64().unwrap())
            .collect::<Vec<_>>();
        for (actual, expected) in ranks.iter().zip(expected) {
            assert!((actual - expected).abs() < 1e-8, "{actual} != {expected}");
        }
        assert!((ranks.iter().sum::<f64>() - 1.0).abs() < 1e-10);
        let metrics = result.metrics();
        assert_eq!(metrics.algorithm.as_ref().unwrap().converged, Some(true));
        if strategy == TraversalStrategy::Cached {
            assert_eq!(metrics.reads.requests, 0);
            assert!(metrics.csr_generation.is_some());
        }

        let mut result = engine
            .analyze_graph(
                "Transfer",
                GraphAlgorithm::WeaklyConnectedComponents,
                options.clone(),
                strategy,
            )
            .await?;
        let values = rows(&mut result).await?;
        assert_eq!(values.len(), 5);
        assert_eq!(result.metrics().algorithm.unwrap().components, Some(3));
        for row in values {
            let representative = match row["tenant"].as_i64() {
                Some(1) => 1,
                _ => row["id"].as_i64().unwrap(),
            };
            assert_eq!(
                row["__graphlake_component"],
                serde_json::json!({"tenant": row["tenant"], "id": representative})
            );
        }
    }
    Ok(())
}

#[derive(serde::Deserialize)]
struct DeltaFixture {
    local: String,
    s3: String,
}

fn delta_fixture() -> Result<DeltaFixture> {
    Ok(serde_json::from_slice(&std::fs::read(std::env::var(
        "GRAPHLAKE_TEST_DELTA_FIXTURE",
    )?)?)?)
}

fn delta_mapping(path: &str, version: Option<u64>) -> Result<String> {
    let path = serde_json::to_string(path)?;
    let version = version
        .map(|version| format!("\n        version: {version}"))
        .unwrap_or_default();
    Ok(format!(
        "  lake:\n    type: delta\n    tables:\n      transfers:\n        path: {path}{version}"
    ))
}

#[tokio::test]
async fn delta_cached_snapshot_observes_commits_and_a_replaced_log() -> Result<()> {
    use serde_json::json;

    let dir = tempfile::tempdir()?;
    let table = dir.path().join("table");
    let log = table.join("_delta_log");
    std::fs::create_dir_all(&log)?;
    let schema = json!({
        "type":"struct", "fields":[{"name":"id","type":"long","nullable":false,"metadata":{}}],
    });
    let add = |id| -> Result<_> {
        let file = format!("part-{id}-{}.parquet", uuid::Uuid::new_v4());
        let batch =
            RecordBatch::try_from_iter([("id", Arc::new(Int64Array::from(vec![id])) as _)])?;
        let bytes = parquet(&batch)?;
        std::fs::write(table.join(&file), &bytes)?;
        Ok(json!({"add":{
            "path":file,"partitionValues":{},"size":bytes.len(),"modificationTime":0,"dataChange":true,
        }}))
    };
    let commit = |version, actions: &[serde_json::Value]| -> Result<()> {
        let file = log.join(format!("{version:020}.json"));
        let bytes = actions
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n");
        std::fs::write(file, format!("{bytes}\n"))?;
        Ok(())
    };
    let initialize = |ids: &[i64]| {
        let mut actions = vec![
            json!({"protocol":{"minReaderVersion":1,"minWriterVersion":2}}),
            json!({"metaData":{
                "id":uuid::Uuid::new_v4(),"format":{"provider":"parquet","options":{}},
                "schemaString":schema.to_string(),"partitionColumns":[],"configuration":{},
            }}),
        ];
        for id in ids {
            actions.push(add(*id)?);
        }
        commit(0, &actions)
    };
    initialize(&[1])?;
    let mapping = delta_mapping(table.to_str().unwrap(), None)?;
    let engine = Engine::new(serde_saphyr::from_str(&format!(
        "graph: delta\nsources:\n{mapping}\nnodes:\n  Node: {{source: lake.transfers, key: [id]}}"
    ))?)?;
    let read = || async {
        let mut result = engine
            .query(
                Language::Sql,
                r#"SELECT id FROM "Node" ORDER BY id"#,
                Default::default(),
            )
            .await?;
        rows(&mut result).await
    };
    assert_eq!(read().await?, [json!({"id":1})]);
    commit(1, &[add(2)?])?;
    assert_eq!(read().await?, [json!({"id":1}), json!({"id":2})]);
    std::fs::remove_dir_all(&log)?;
    std::fs::create_dir(&log)?;
    initialize(&[3])?;
    assert_eq!(read().await?, [json!({"id":3})]);
    assert_eq!(read().await?, [json!({"id":3})]);
    // Recreating the same numeric version invalidates the previous uniqueness proof.
    initialize(&[4, 4])?;
    let error = read()
        .await
        .expect_err("recreated table has duplicate keys");
    assert!(format!("{error:#}").contains("duplicate business key"));
    initialize(&[5])?;
    assert_eq!(read().await?, [json!({"id":5})]);
    Ok(())
}

#[tokio::test]
#[ignore = "requires a Delta fixture from tests/delta_fixture.py"]
async fn delta_time_travel_preserves_partitions_removals_and_deletion_vectors() -> Result<()> {
    let fixture = delta_fixture()?;
    let dir = tempfile::tempdir()?;
    std::fs::write(dir.path().join("accounts.parquet"), parquet(&accounts()?)?)?;
    let config = dir.path().join("graph.yaml");
    let load = |path: &str, version| -> Result<Engine> {
        let sources = format!(
            "  accounts:\n    type: parquet\n    tables:\n      accounts: [accounts.parquet]\n{}",
            delta_mapping(path, version)?
        );
        std::fs::write(
            &config,
            catalog("accounts.accounts", &sources, "key: [tenant, id]"),
        )?;
        Engine::new(Catalog::load(&config)?)
    };
    let opts = QueryOptions {
        batch_size: 1,
        ..Default::default()
    };
    for (path, version, expected_version, expected_ids) in [
        (
            fixture.local.as_str(),
            Some(0),
            "0",
            vec![10, 11, 12, 13, 14, 10],
        ),
        (
            fixture.local.as_str(),
            Some(1),
            "1",
            vec![10, 11, 12, 13, 14],
        ),
        (fixture.local.as_str(), Some(2), "2", vec![12, 13, 14]),
        (fixture.s3.as_str(), None, "2", vec![12, 13, 14]),
    ] {
        let engine = load(path, version)?;
        let mut result = engine
            .query(
                Language::Sql,
                "SELECT tenant, id FROM \"Transfer\" ORDER BY tenant, id",
                opts.clone(),
            )
            .await?;
        let ids = rows(&mut result)
            .await?
            .into_iter()
            .map(|row| row["id"].as_i64().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(ids, expected_ids);
        assert!(
            result
                .versions
                .iter()
                .any(|v| v.consistency == "Delta version" && v.version == expected_version)
        );
    }
    let error = load(&fixture.local, None)?
        .query(Language::Sql, "SELECT id FROM \"Transfer\"", opts.clone())
        .await
        .err()
        .context("expected unsupported Delta reader feature to fail")?;
    let detail = format!("{error:#}");
    assert!(
        detail.contains("graphlakeTestUnsupported") || detail.contains("Unsupported"),
        "{detail}"
    );
    let mut historic = load(&fixture.s3, Some(0))?
        .query(Language::Cypher, TWO_HOPS, opts)
        .await?;
    assert_eq!(rows(&mut historic).await?, expected_two_hops());
    Ok(())
}

#[tokio::test]
#[ignore = "requires an isolated MySQL 8.x instance and the Delta fixture"]
async fn mysql_snapshot_joins_delta_and_preserves_exact_types() -> Result<()> {
    let fixture = delta_fixture()?;
    let dsn = std::env::var("GRAPHLAKE_TEST_MYSQL")?;
    let mut client = Conn::new(Opts::from_url(&dsn)?).await?;
    let db = client
        .query_first::<String, _>("SELECT DATABASE()")
        .await?
        .context("MySQL test database required")?;
    let table = format!("graphlake_accounts_{}", std::process::id());
    client
        .query_drop(format!(
            "CREATE TABLE {table} (
            tenant INT, id BIGINT, status VARCHAR(32), balance DECIMAL(38,2),
            unsigned_value BIGINT UNSIGNED, created TIMESTAMP(6) NULL,
            PRIMARY KEY (tenant,id)
        ) ENGINE=InnoDB"
        ))
        .await?;
    let check = async {
        client.query_drop("SET SESSION time_zone = '+00:00'").await?;
        client.query_drop(format!(
            "INSERT INTO {table} VALUES
             (1,1,'active',123456789012345678901234567890123456.78,18446744073709551615,'2026-11-11 00:00:00.123456'),
             (1,2,'active',-0.12,0,NULL),(1,3,'locked',NULL,0,NULL),(2,2,'other',0,0,NULL)"
        )).await?;
        let dir = tempfile::tempdir()?;
        let engine = mysql_engine(dir.path(), &db, &table, &fixture.s3)?;
        let options = QueryOptions { batch_size: 2, ..Default::default() };
        let mut pinned = engine.query(Language::Sql,
            "SELECT status, CAST(balance AS VARCHAR) AS balance,
                    CAST(unsigned_value AS VARCHAR) AS unsigned_value, created
             FROM \"Account\" WHERE tenant = 1 AND id = 1", options.clone()).await?;
        client.query_drop(format!("UPDATE {table} SET status='changed' WHERE tenant=1 AND id=1")).await?;
        let data = rows(&mut pinned).await?;
        assert_eq!(data, vec![serde_json::json!({
            "status": "active", "balance": "123456789012345678901234567890123456.78",
            "unsigned_value": "18446744073709551615", "created": "2026-11-11T00:00:00.123456Z",
        })]);
        assert_eq!(pinned.metrics().reads.mysql_rows, 1);
        assert!(pinned.versions[0].consistency.contains("MySQL InnoDB"));
        let mut joined = engine.query(Language::Cypher, TWO_HOPS, options.clone()).await?;
        assert_eq!(rows(&mut joined).await?, expected_two_hops());
        assert_eq!(joined.versions.len(), 2);
        let limited = engine.query(Language::Sql, "SELECT id FROM \"Account\"",
            QueryOptions { memory_bytes: 16, ..options }).await;
        let limited = match limited {
            Ok(mut query) => rows(&mut query).await.map(|_| ()),
            Err(error) => Err(error),
        };
        let error = limited.err().context("expected the MySQL scan to exceed this memory budget")?;
        assert!(format!("{error:#}").contains("Resources exhausted"));
        Ok::<(), anyhow::Error>(())
    }.await;
    let cleanup = client.query_drop(format!("DROP TABLE {table}")).await;
    client.disconnect().await?;
    check?;
    cleanup?;
    Ok(())
}

fn mysql_engine(dir: &Path, db: &str, table: &str, delta: &str) -> Result<Engine> {
    let sources = format!(
        "  crm:\n    type: mysql\n    dsn_env: GRAPHLAKE_TEST_MYSQL\n    tls: false\n{}",
        delta_mapping(delta, Some(0))?
    );
    let config = dir.join("graph.yaml");
    let mapping = catalog(&format!("crm.{db}.{table}"), &sources, "key: [tenant, id]").replace(
        "properties: [status]",
        "properties: [status, balance, unsigned_value, created]",
    );
    std::fs::write(&config, mapping)?;
    Engine::new(Catalog::load(config)?)
}
