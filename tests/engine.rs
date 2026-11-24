#[path = "engine/direct_sources.rs"]
mod direct_sources;

#[path = "engine/query_model.rs"]
mod query_model;

#[path = "engine/phase3.rs"]
mod phase3;

#[path = "engine/sources.rs"]
mod sources;

#[path = "engine/warehouses.rs"]
mod warehouses;

#[path = "engine/projected.rs"]
mod projected;

use std::{sync::Arc, time::Duration};

use anyhow::{Result, ensure};
use futures::TryStreamExt;
use graphlake::{
    Catalog, Direction, Engine, Language, QueryOptions, QueryResult, ScalarValue, Traversal,
    TraversalKind, TraversalStrategy,
    arrow::array::{Int32Array, Int64Array, RecordBatch, StringArray},
};
use parquet::arrow::ArrowWriter;
use tempfile::TempDir;

fn parquet(batch: &RecordBatch) -> Result<Vec<u8>> {
    let mut writer = ArrowWriter::try_new(Vec::new(), batch.schema(), None)?;
    writer.write(batch)?;
    Ok(writer.into_inner()?)
}

fn accounts() -> Result<RecordBatch> {
    Ok(RecordBatch::try_from_iter([
        ("tenant", Arc::new(Int32Array::from(vec![1, 1, 1, 2])) as _),
        ("id", Arc::new(Int64Array::from(vec![1, 2, 3, 2])) as _),
        (
            "status",
            Arc::new(StringArray::from(vec![
                "active", "active", "locked", "other",
            ])) as _,
        ),
    ])?)
}

fn transfers() -> Result<RecordBatch> {
    Ok(RecordBatch::try_from_iter([
        (
            "tenant",
            Arc::new(Int32Array::from(vec![1, 1, 1, 1, 1, 1, 2])) as _,
        ),
        (
            "id",
            Arc::new(Int64Array::from(vec![10, 11, 12, 13, 14, 15, 10])) as _,
        ),
        (
            "src",
            Arc::new(Int64Array::from(vec![1, 1, 2, 2, 1, 1, 2])) as _,
        ),
        (
            "dst",
            Arc::new(Int64Array::from(vec![2, 2, 1, 3, 1, 9, 2])) as _,
        ),
        (
            "amount",
            Arc::new(Int64Array::from(vec![10, 20, 30, 40, 50, 60, 70])) as _,
        ),
    ])?)
}

fn catalog(accounts_source: &str, sources: &str, key: &str) -> String {
    format!(
        "graph: finance
sources:
{sources}
nodes:
  Account:
    source: {accounts_source}
    key: [tenant, id]
    properties: [status]
edges:
  Transfer:
    source: lake.transfers
    {key}
    from: {{node: Account, columns: [tenant, src]}}
    to: {{node: Account, columns: [tenant, dst]}}
    properties: [amount]
"
    )
}

fn local_fixture(key: &str) -> Result<(TempDir, Engine)> {
    let dir = tempfile::tempdir()?;
    std::fs::write(dir.path().join("accounts.parquet"), parquet(&accounts()?)?)?;
    std::fs::write(
        dir.path().join("transfers.parquet"),
        parquet(&transfers()?)?,
    )?;
    let path = dir.path().join("graph.yaml");
    std::fs::write(
        &path,
        catalog(
            "lake.accounts",
            "  lake:\n    type: parquet\n    tables:\n      accounts: [accounts.parquet]\n      transfers: [transfers.parquet]",
            key,
        ),
    )?;
    Ok((dir, Engine::new(Catalog::load(&path)?)?))
}

async fn rows(result: &mut QueryResult) -> Result<Vec<serde_json::Value>> {
    let mut writer = graphlake::arrow::json::LineDelimitedWriter::new(Vec::new());
    while let Some(batch) = result.batches.try_next().await? {
        writer.write(&batch)?;
    }
    writer.finish()?;
    String::from_utf8(writer.into_inner())?
        .lines()
        .map(|line| Ok(serde_json::from_str(line)?))
        .collect()
}

const TWO_HOPS: &str = "MATCH (a:Account)-[x:Transfer]->(b:Account)-[y:Transfer]->(c:Account)
WHERE a.tenant = 1 AND a.id = 1
RETURN c.id AS destination, count(*) AS paths, sum(y.amount) AS total ORDER BY destination";

#[tokio::test]
async fn single_table_duplicates_fail_before_a_query_can_count_them() -> Result<()> {
    let (files, engine) = local_fixture("key: [tenant, id]")?;
    let accounts = RecordBatch::try_from_iter([
        ("tenant", Arc::new(Int32Array::from(vec![1, 1])) as _),
        ("id", Arc::new(Int64Array::from(vec![1, 1])) as _),
        (
            "status",
            Arc::new(StringArray::from(vec!["active", "active"])) as _,
        ),
    ])?;
    std::fs::write(files.path().join("accounts.parquet"), parquet(&accounts)?)?;
    for (language, query) in [
        (Language::Cypher, "MATCH (a:Account) RETURN count(*) AS n"),
        (Language::Sql, r#"SELECT count(*) AS n FROM "Account""#),
    ] {
        let Err(error) = engine.query(language, query, QueryOptions::default()).await else {
            panic!("duplicate node identities were accepted");
        };
        assert!(format!("{error:#}").contains("duplicate business key"));
    }
    Ok(())
}

#[tokio::test]
async fn explain_plans_without_checking_data_rows() -> Result<()> {
    let (files, engine) = local_fixture("")?;
    let input = accounts()?;
    let duplicated = graphlake::arrow::compute::concat_batches(
        &input.schema(),
        &[input.clone(), input.slice(0, 1)],
    )?;
    std::fs::write(files.path().join("accounts.parquet"), parquet(&duplicated)?)?;
    for (language, query) in [
        (Language::Sql, r#"SELECT count(*) FROM "Transfer""#),
        (
            Language::Cypher,
            "MATCH (a:Account)-[:Transfer*1..2]->(b:Account) RETURN count(b)",
        ),
        (Language::Gremlin, "g.E().hasLabel('Transfer').count()"),
    ] {
        let mut result = engine
            .query(language, &format!("EXPLAIN {query}"), Default::default())
            .await?;
        assert!(!rows(&mut result).await?.is_empty());
        let Err(error) = engine
            .query(
                language,
                &format!("EXPLAIN ANALYZE {query}"),
                Default::default(),
            )
            .await
        else {
            panic!("EXPLAIN ANALYZE did not check duplicate keys");
        };
        assert!(format!("{error:#}").contains("duplicate business key"));
    }
    Ok(())
}

#[tokio::test]
async fn full_topology_checks_duplicate_keys_before_discarding_dangling_edges() -> Result<()> {
    use graphlake::{GraphAlgorithm, GraphProjection, ProjectionAlgorithm};

    let (files, engine) = local_fixture("key: [tenant, id]")?;
    let engine = engine.with_cache(files.path().join("csr"), "key-validation")?;
    let input = transfers()?;
    let duplicate = datafusion::arrow::compute::concat_batches(
        &input.schema(),
        &[input.clone(), input.slice(5, 1)],
    )?;
    std::fs::write(files.path().join("transfers.parquet"), parquet(&duplicate)?)?;
    let options = QueryOptions {
        batch_size: 1,
        ..Default::default()
    };

    let mut scan = engine
        .analyze_graph(
            "Transfer",
            GraphAlgorithm::WeaklyConnectedComponents,
            options.clone(),
            TraversalStrategy::Scan,
        )
        .await?;
    let error = rows(&mut scan).await.unwrap_err();
    assert!(format!("{error:#}").contains("duplicate relationship business key"));

    let projected = engine
        .analyze_projection(
            GraphProjection {
                edges: vec!["Transfer".into()],
                ..Default::default()
            },
            ProjectionAlgorithm::WeaklyConnectedComponents,
            options.clone(),
        )
        .await;
    assert!(
        projected
            .err()
            .unwrap()
            .to_string()
            .contains("duplicate business key")
    );

    let error = engine.refresh("Transfer", options).await.unwrap_err();
    assert!(format!("{error:#}").contains("duplicate relationship business key"));
    Ok(())
}

#[tokio::test]
async fn query_streams_hold_shared_capacity_until_completion_or_drop() -> Result<()> {
    let (files, _) = local_fixture("key: [tenant, id]")?;
    let limits = graphlake::EngineLimits {
        max_queries: 2,
        memory_bytes: 256 << 20,
        spill_bytes: 2 << 30,
    };
    let engine = Engine::new_with_limits(Catalog::load(files.path().join("graph.yaml"))?, limits)?;
    let sql = r#"SELECT id FROM "Account""#;
    let first = engine
        .query(Language::Sql, sql, QueryOptions::default())
        .await?;
    let options = QueryOptions {
        timeout: Duration::from_millis(25),
        ..Default::default()
    };
    let second = engine.clone().query(Language::Sql, sql, options).await;
    assert!(
        second
            .err()
            .unwrap()
            .to_string()
            .contains("waiting for engine resources")
    );
    drop(first);
    let mut first = engine
        .query(Language::Sql, sql, QueryOptions::default())
        .await?;
    assert_eq!(rows(&mut first).await?.len(), 4);
    let options = QueryOptions {
        timeout: Duration::from_secs(2),
        ..Default::default()
    };
    let mut second = engine.query(Language::Sql, sql, options).await?;
    assert_eq!(rows(&mut second).await?.len(), 4);
    let mut abandoned = engine
        .query(
            Language::Sql,
            sql,
            QueryOptions {
                timeout: Duration::from_millis(200),
                ..Default::default()
            },
        )
        .await?;
    let mut admitted = engine
        .query(
            Language::Sql,
            sql,
            QueryOptions {
                timeout: Duration::from_secs(1),
                ..Default::default()
            },
        )
        .await?;
    assert_eq!(rows(&mut admitted).await?.len(), 4);
    assert!(
        abandoned
            .batches
            .try_next()
            .await
            .unwrap_err()
            .to_string()
            .contains("deadline")
    );
    Ok(())
}

#[tokio::test]
async fn strict_snapshot_rejects_a_join_across_independent_reads() -> Result<()> {
    let (files, engine) = local_fixture("key: [tenant, id]")?;
    let engine = engine.with_cache(files.path().join("cache"), "snapshot-mode")?;
    let mut options = QueryOptions::default();
    options.graph.require_consistent_snapshot = true;
    let mut result = engine
        .query(
            Language::Sql,
            r#"SELECT count(*) AS n FROM "Account""#,
            options.clone(),
        )
        .await?;
    assert_eq!(rows(&mut result).await?, [serde_json::json!({"n":4})]);
    let Err(error) = engine
        .query(Language::Cypher, TWO_HOPS, options.clone())
        .await
    else {
        panic!("strict snapshot accepted independently pinned objects");
    };
    assert!(
        error
            .to_string()
            .contains("requires one consistent snapshot")
    );

    let request = Traversal {
        edge: "Transfer".into(),
        direction: Direction::Outgoing,
        start: vec![ScalarValue::Int32(Some(1)), ScalarValue::Int64(Some(1))],
        max_hops: 1,
        kind: TraversalKind::Reachable,
    };
    engine.refresh("Transfer", QueryOptions::default()).await?;
    let Err(error) = engine
        .traverse_with_strategy(request, options, TraversalStrategy::Cached)
        .await
    else {
        panic!("strict snapshot reused a CSR generation built from independent reads");
    };
    assert!(error.to_string().contains("no published CSR generation"));
    Ok(())
}

fn expected_two_hops() -> Vec<serde_json::Value> {
    vec![
        serde_json::json!({"destination": 1, "paths": 2, "total": 60}),
        serde_json::json!({"destination": 2, "paths": 2, "total": 30}),
        serde_json::json!({"destination": 3, "paths": 2, "total": 80}),
    ]
}

#[tokio::test]
async fn fixed_patterns_respect_tenants_endpoints_and_relationship_identity() -> Result<()> {
    for key in ["key: [tenant, id]", ""] {
        let (_files, engine) = local_fixture(key)?;
        let opts = QueryOptions {
            batch_size: 2,
            ..Default::default()
        };
        let mut result = engine
            .query(Language::Cypher, TWO_HOPS, opts.clone())
            .await?;
        assert_eq!(rows(&mut result).await?, expected_two_hops());
        assert_eq!(result.versions.len(), 2);
        assert_eq!(result.metrics().output_rows, 3);
        let mut sql = engine
            .query(
                Language::Sql,
                "SELECT COUNT(*) AS n FROM \"Transfer\"",
                opts.clone(),
            )
            .await?;
        assert_eq!(rows(&mut sql).await?, vec![serde_json::json!({"n": 6})]);
        let mut reverse = engine
            .query(
                Language::Cypher,
                "MATCH (a:Account)<-[:Transfer]-(b:Account) WHERE a.tenant=1 AND a.id=2
             RETURN DISTINCT b.id AS source",
                opts,
            )
            .await?;
        assert_eq!(
            rows(&mut reverse).await?,
            vec![serde_json::json!({"source": 1})]
        );
    }
    let (_files, engine) = local_fixture("key: [tenant, id]")?;
    let mut generated = engine
        .query(
            Language::Sql,
            "SELECT sum(value) AS total FROM range(0, 3)",
            QueryOptions::default(),
        )
        .await?;
    assert_eq!(
        rows(&mut generated).await?,
        vec![serde_json::json!({"total": 3})]
    );
    Ok(())
}

#[tokio::test]
async fn enforces_object_version_memory_and_cancellation() -> Result<()> {
    let (dir, engine) = local_fixture("key: [tenant, id]")?;
    let opts = QueryOptions {
        memory_bytes: 64,
        ..Default::default()
    };
    let error = engine
        .query(Language::Cypher, TWO_HOPS, opts)
        .await
        .err()
        .expect("memory error");
    assert!(format!("{error:#}").contains("memory"), "{error:#}");

    let opts = QueryOptions::default();
    let cancellation = opts.cancellation.clone();
    let mut result = engine.query(Language::Cypher, TWO_HOPS, opts).await?;
    cancellation.cancel();
    assert!(
        result
            .batches
            .try_next()
            .await
            .unwrap_err()
            .to_string()
            .contains("cancelled")
    );

    let opts = QueryOptions {
        timeout: Duration::from_millis(250),
        ..Default::default()
    };
    let mut result = engine
        .query(Language::Sql, "SELECT * FROM \"Account\"", opts)
        .await?;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        result
            .batches
            .try_next()
            .await
            .unwrap_err()
            .to_string()
            .contains("deadline")
    );

    let mut result = engine
        .query(
            Language::Sql,
            "SELECT * FROM \"Account\"",
            QueryOptions::default(),
        )
        .await?;
    let replacement = dir.path().join("replacement.parquet");
    std::fs::write(&replacement, parquet(&accounts()?)?)?;
    std::fs::rename(replacement, dir.path().join("accounts.parquet"))?;
    let error = result.batches.try_next().await.unwrap_err().to_string();
    assert!(
        error.contains("precondition") || error.contains("changed"),
        "{error}"
    );
    Ok(())
}

#[tokio::test]
async fn traversal_distinguishes_vertex_distance_from_edge_unique_paths() -> Result<()> {
    let (_dir, engine) = local_fixture("key: [tenant, id]")?;
    let options = QueryOptions {
        batch_size: 2,
        ..Default::default()
    };
    let traversal = |kind| Traversal {
        edge: "Transfer".into(),
        direction: Direction::Outgoing,
        start: vec![ScalarValue::Int32(Some(1)), ScalarValue::Int64(Some(1))],
        max_hops: 2,
        kind,
    };
    let mut reachable = engine
        .traverse(traversal(TraversalKind::Reachable), options.clone())
        .await?;
    assert_eq!(
        rows(&mut reachable).await?,
        vec![
            serde_json::json!({"tenant": 1, "id": 1, "__graphlake_hops": 0}),
            serde_json::json!({"tenant": 1, "id": 2, "__graphlake_hops": 1}),
            serde_json::json!({"tenant": 1, "id": 3, "__graphlake_hops": 2}),
        ]
    );
    assert_eq!(reachable.metrics().visits_by_depth, [1, 1, 1]);

    let mut shortest = engine
        .traverse(
            traversal(TraversalKind::Shortest {
                target: vec![ScalarValue::Int32(Some(1)), ScalarValue::Int64(Some(3))],
            }),
            options.clone(),
        )
        .await?;
    let answer = rows(&mut shortest).await?;
    assert_eq!(answer.len(), 1);
    assert_eq!(answer[0]["id"], 3);
    assert_eq!(answer[0]["__graphlake_hops"], 2);
    let edges = answer[0]["__graphlake_path"].as_array().unwrap();
    assert!(edges[0]["id"] == 10 || edges[0]["id"] == 11);
    assert_eq!(edges[1], serde_json::json!({"tenant": 1, "id": 13}));

    let mut paths = engine
        .traverse(traversal(TraversalKind::Paths { min_hops: 2 }), options)
        .await?;
    let mut edge_sequences = rows(&mut paths)
        .await?
        .into_iter()
        .map(|row| {
            let edges = row["__graphlake_path"].as_array().unwrap();
            [
                edges[0]["id"].as_i64().unwrap(),
                edges[1]["id"].as_i64().unwrap(),
            ]
        })
        .collect::<Vec<_>>();
    edge_sequences.sort_unstable();
    assert_eq!(
        edge_sequences,
        [[10, 12], [10, 13], [11, 12], [11, 13], [14, 10], [14, 11]]
    );
    assert_eq!(paths.metrics().visits_by_depth, [1, 3, 6]);
    Ok(())
}

#[tokio::test]
async fn cli_emits_arrow_with_typed_composite_keys() -> Result<()> {
    let (dir, _) = local_fixture("key: [tenant, id]")?;
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_graphlake"))
        .arg("--catalog")
        .arg(dir.path().join("graph.yaml"))
        .args([
            "--format",
            "arrow",
            "--diagnostics",
            "traverse",
            "--edge",
            "Transfer",
            "--start",
            "1",
            "--start",
            "1",
            "--max-hops",
            "2",
            "reachable",
        ])
        .output()?;
    ensure!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let mut reader =
        graphlake::arrow::ipc::reader::StreamReader::try_new(output.stdout.as_slice(), None)?;
    let batch = reader.next().expect("result batch")?;
    assert_eq!(batch.num_rows(), 3);
    assert_eq!(
        batch.schema().field(0).data_type(),
        &graphlake::arrow::datatypes::DataType::Int32
    );
    let diagnostics: serde_json::Value = serde_json::from_slice(&output.stderr)?;
    assert_eq!(
        diagnostics["metrics"]["visits_by_depth"],
        serde_json::json!([1, 1, 1])
    );
    Ok(())
}

#[tokio::test]
async fn ranged_cypher_preserves_zero_hops_cycles_and_edge_uniqueness() -> Result<()> {
    for key in ["key: [tenant, id]", ""] {
        let (_dir, engine) = local_fixture(key)?;
        let mut ranged = engine
            .query(
                Language::Cypher,
                "MATCH p=(a:Account {tenant: 1, id: 1})-[r:Transfer*0..2]->(b:Account)
             RETURN length(p) AS hops, size(r) AS size, count(*) AS paths
             ORDER BY hops",
                QueryOptions::default(),
            )
            .await?;
        assert_eq!(
            rows(&mut ranged).await?,
            vec![
                serde_json::json!({"hops": 0, "size": 0, "paths": 1}),
                serde_json::json!({"hops": 1, "size": 1, "paths": 3}),
                serde_json::json!({"hops": 2, "size": 2, "paths": 6}),
            ]
        );
        assert!(ranged.physical_plan.contains("GraphExpandExec"));

        let mut mixed = engine
            .query(
                Language::Cypher,
                "MATCH (a:Account)-[:Transfer*1]->(b:Account)-[:Transfer]->(c:Account)
             WHERE a.tenant = 1 AND a.id = 1
             RETURN c.id AS destination, count(*) AS paths ORDER BY destination",
                QueryOptions::default(),
            )
            .await?;
        assert_eq!(
            rows(&mut mixed).await?,
            vec![
                serde_json::json!({"destination": 1, "paths": 2}),
                serde_json::json!({"destination": 2, "paths": 2}),
                serde_json::json!({"destination": 3, "paths": 2}),
            ]
        );

        let mut pair = engine
            .query(
                Language::Cypher,
                "MATCH (a:Account)<-[:Transfer*1]-(b:Account)<-[:Transfer*1]-(c:Account)
             WHERE a.tenant = 1 AND a.id = 1
             RETURN count(*) AS paths",
                QueryOptions::default(),
            )
            .await?;
        assert_eq!(
            rows(&mut pair).await?,
            vec![serde_json::json!({"paths": 3})]
        );
    }
    Ok(())
}

#[tokio::test]
async fn ranged_directed_edge_between_distinct_types_matches_only_one_hop() -> Result<()> {
    let (dir, _) = local_fixture("key: [tenant, id]")?;
    let mut catalog = Catalog::load(dir.path().join("graph.yaml"))?;
    catalog
        .nodes
        .insert("Recipient".into(), catalog.nodes["Account"].clone());
    catalog.edges.get_mut("Transfer").unwrap().to.node = "Recipient".into();
    let engine = Engine::new(catalog)?;
    for query in [
        "MATCH p=(a:Account {tenant:1,id:1})-[r:Transfer*0..0]->(b:Account) RETURN b.id AS id, length(p) AS hops, nodes(p) AS nodes, r AS edges",
        "MATCH p=(a:Account {tenant:1,id:1})-[r:Transfer*0..1]->(b:Account) RETURN b.id AS id, length(p) AS hops, nodes(p) AS nodes, r AS edges",
        "MATCH p=(a:Account {tenant:1,id:1})<-[r:Transfer*0..0]-(b) RETURN b.id AS id, length(p) AS hops, nodes(p) AS nodes, r AS edges",
    ] {
        let mut result = engine
            .query(Language::Cypher, query, Default::default())
            .await?;
        assert_eq!(
            rows(&mut result).await?,
            [serde_json::json!({"id":1,"hops":0,"nodes":[{"tenant":1,"id":1}],"edges":[]})],
            "{query}"
        );
    }
    let mut result = engine.query(Language::Cypher,
        "MATCH p=(a:Account {tenant:1,id:1})-[:Transfer*0..0]->(b:Account)-[:Transfer]->(c:Recipient {id:2})
         RETURN length(p) AS hops, size(nodes(p)) AS n, size(relationships(p)) AS e",
        Default::default()).await?;
    assert_eq!(
        rows(&mut result).await?,
        vec![serde_json::json!({"hops":1,"n":2,"e":1}); 2]
    );
    let mut result = engine
        .query(
            Language::Cypher,
            "MATCH p=(a:Account {tenant: 1, id: 1})-[r:Transfer*0..3]->(b:Recipient)
         RETURN length(p) AS hops, size(r) AS size, count(*) AS paths",
            QueryOptions::default(),
        )
        .await?;
    assert_eq!(
        rows(&mut result).await?,
        vec![serde_json::json!({"hops": 1, "size": 1, "paths": 3})]
    );
    let mut result = engine
        .query(
            Language::Cypher,
            "MATCH p=(a:Account {tenant: 1, id: 1})-[:Transfer*2]-(b:Account)
         RETURN b.id AS id, count(*) AS paths ORDER BY id",
            QueryOptions::default(),
        )
        .await?;
    assert_eq!(
        rows(&mut result).await?,
        [
            serde_json::json!({"id":1,"paths":2}),
            serde_json::json!({"id":2,"paths":1})
        ]
    );
    Ok(())
}

fn traversal_request(direction: Direction, kind: TraversalKind) -> Traversal {
    Traversal {
        edge: "Transfer".into(),
        direction,
        start: vec![ScalarValue::Int32(Some(1)), ScalarValue::Int64(Some(1))],
        max_hops: 2,
        kind,
    }
}

#[tokio::test]
async fn parquet_key_validation_reuses_only_the_pinned_file_set() -> Result<()> {
    let (directory, engine) = local_fixture("key: [tenant, id]")?;
    let query = r#"SELECT id FROM "Account" ORDER BY id LIMIT 1"#;
    let mut cold = engine
        .query(Language::Sql, query, Default::default())
        .await?;
    let expected = rows(&mut cold).await?;
    let cold_bytes = cold.metrics().reads.object_bytes;
    let mut warm = engine
        .query(Language::Sql, query, Default::default())
        .await?;
    assert_eq!(rows(&mut warm).await?, expected);
    assert!(warm.metrics().reads.object_bytes < cold_bytes);
    let one = accounts()?.slice(0, 1);
    let duplicated = graphlake::arrow::compute::concat_batches(&one.schema(), [&one, &one])?;
    let replacement = directory.path().join("replacement.parquet");
    std::fs::write(&replacement, parquet(&duplicated)?)?;
    std::fs::rename(replacement, directory.path().join("accounts.parquet"))?;
    let error = engine
        .query(Language::Sql, query, Default::default())
        .await
        .err()
        .unwrap();
    assert!(format!("{error:#}").contains("duplicate business key in Account"));
    Ok(())
}

#[tokio::test]
async fn persisted_csr_keeps_old_readers_and_recovers_from_failed_refresh() -> Result<()> {
    let (dir, engine) = local_fixture("key: [tenant, id]")?;
    let cache = dir.path().join("csr");
    let engine = engine.with_cache(&cache, "finance-read")?;
    let opts = QueryOptions::default();
    let first = engine.refresh("Transfer", opts.clone()).await?;
    assert_eq!((first.vertices, first.edges), (4, 6));
    let data = std::fs::read_dir(&cache)?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .find(|path| path.extension().is_some_and(|ext| ext == "csr"))
        .unwrap();
    let interrupted = cache.join(format!(
        ".{}-stage-interrupted",
        data.file_stem().unwrap().to_string_lossy()
    ));
    std::fs::write(&interrupted, b"interrupted refresh")?;

    let request = traversal_request(Direction::Outgoing, TraversalKind::Reachable);
    let mut pinned = engine
        .traverse_with_strategy(request.clone(), opts.clone(), TraversalStrategy::Cached)
        .await?;

    let mut reverse = engine
        .traverse_with_strategy(
            traversal_request(Direction::Incoming, TraversalKind::Reachable),
            opts.clone(),
            TraversalStrategy::Cached,
        )
        .await?;
    assert_eq!(rows(&mut reverse).await?.len(), 2);
    assert_eq!(reverse.metrics().reads.requests, 0);

    let source = dir.path().join("transfers.parquet");
    let updated = dir.path().join("new.parquet");
    std::fs::write(&updated, parquet(&transfers()?.slice(0, 2))?)?;
    std::fs::rename(&updated, &source)?;
    let failed = engine
        .refresh(
            "Transfer",
            QueryOptions {
                memory_bytes: 64,
                ..opts.clone()
            },
        )
        .await;
    assert!(failed.is_err());
    assert!(!interrupted.exists());
    let mut after_failure = engine
        .traverse_with_strategy(request.clone(), opts.clone(), TraversalStrategy::Cached)
        .await?;
    assert_eq!(rows(&mut after_failure).await?.len(), 3);
    assert_eq!(
        after_failure.metrics().csr_generation.as_deref(),
        Some(first.generation.as_str())
    );

    let mut live = engine.traverse(request.clone(), opts.clone()).await?;
    assert_eq!(rows(&mut live).await?.len(), 2);
    assert!(
        live.metrics().csr_generation.is_none(),
        "auto served an obsolete graph"
    );
    let second = engine.refresh("Transfer", opts.clone()).await?;
    assert_ne!(first.generation, second.generation);
    assert_eq!(second.edges, 2);
    assert_eq!(rows(&mut pinned).await?.len(), 3);
    assert_eq!(
        pinned.metrics().csr_generation.as_deref(),
        Some(first.generation.as_str())
    );

    let catalog = Catalog::load(dir.path().join("graph.yaml"))?;
    let restarted = Engine::new(catalog.clone())?.with_cache(&cache, "finance-read")?;
    let other_scope = Engine::new(catalog)?.with_cache(&cache, "different-policy")?;
    assert!(
        other_scope
            .traverse_with_strategy(request.clone(), opts.clone(), TraversalStrategy::Cached)
            .await
            .is_err()
    );
    std::fs::rename(&source, &updated)?;
    let mut offline = restarted
        .traverse_with_strategy(request.clone(), opts.clone(), TraversalStrategy::Cached)
        .await?;
    assert_eq!(rows(&mut offline).await?.len(), 2);
    assert_eq!(
        offline.metrics().csr_generation.as_deref(),
        Some(second.generation.as_str())
    );
    assert_eq!(offline.metrics().reads.requests, 0);

    let mut damaged = std::fs::read(&data)?;
    *damaged.last_mut().unwrap() ^= 1;
    std::fs::write(data, damaged)?;
    let error = restarted
        .traverse_with_strategy(request.clone(), opts.clone(), TraversalStrategy::Cached)
        .await
        .err()
        .unwrap();
    assert!(format!("{error:#}").contains("checksum mismatch"));
    std::fs::rename(&updated, &source)?;
    let mut fallback = restarted.traverse(request.clone(), opts.clone()).await?;
    assert_eq!(rows(&mut fallback).await?.len(), 2);
    assert!(fallback.metrics().csr_generation.is_none());
    restarted.refresh("Transfer", opts.clone()).await?;
    let mut recovered = restarted.traverse(request, opts).await?;
    assert_eq!(rows(&mut recovered).await?.len(), 2);
    Ok(())
}

#[tokio::test]
async fn gremlin_repeat_project_emit_rejects_excessive_nesting() -> Result<()> {
    let (_dir, engine) = local_fixture("key: [tenant, id]")?;
    let err = engine
        .query(
            Language::Gremlin,
            "g.V().repeat(__.project('a')).emit()",
            QueryOptions::default(),
        )
        .await
        .err()
        .expect("unbounded repeat(project) must be rejected before exponential nesting");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("nesting") || msg.contains("limit") || msg.contains("depth"),
        "unexpected error: {msg}"
    );
    Ok(())
}

#[tokio::test]
async fn cypher_reduce_concat_and_array_concat_respect_memory_budget() -> Result<()> {
    let (_dir, engine) = local_fixture("key: [tenant, id]")?;
    let tight = QueryOptions {
        memory_bytes: 2 * 1024 * 1024,
        ..Default::default()
    };

    // 1. Exponential Utf8View/struct doubling in Cypher reduce() must hit the memory pool.
    let reduce_attempt = async {
        let mut r = engine
            .query(
                Language::Cypher,
                "RETURN size(reduce(acc = {s: 'aaaaaaaa'}, i IN range(1, 26) | {s: acc.s + acc.s}).s) AS n",
                tight.clone(),
            )
            .await?;
        rows(&mut r).await
    }
    .await;
    assert!(
        reduce_attempt.is_err(),
        "exponential reduce() bypassed memory_bytes budget"
    );

    // 2. Chained Cypher `+` string doubling must hit the memory pool.
    let add_query = "WITH 'aaaaaaaa' AS s \
        WITH s+s AS s WITH s+s AS s WITH s+s AS s WITH s+s AS s WITH s+s AS s \
        WITH s+s AS s WITH s+s AS s WITH s+s AS s WITH s+s AS s WITH s+s AS s \
        WITH s+s AS s WITH s+s AS s WITH s+s AS s WITH s+s AS s WITH s+s AS s \
        WITH s+s AS s WITH s+s AS s WITH s+s AS s WITH s+s AS s WITH s+s AS s \
        RETURN size(s) AS n";
    let add_attempt = async {
        let mut r = engine
            .query(Language::Cypher, add_query, tight.clone())
            .await?;
        rows(&mut r).await
    }
    .await;
    assert!(
        add_attempt.is_err(),
        "chained Cypher string doubling bypassed memory_bytes budget"
    );

    // 3. SQL array_concat exponential list doubling must hit the memory pool.
    let array_sql = "WITH a0 AS (SELECT range(0, 256) AS a), \
        a1 AS (SELECT array_concat(a, a) AS a FROM a0), \
        a2 AS (SELECT array_concat(a, a) AS a FROM a1), \
        a3 AS (SELECT array_concat(a, a) AS a FROM a2), \
        a4 AS (SELECT array_concat(a, a) AS a FROM a3), \
        a5 AS (SELECT array_concat(a, a) AS a FROM a4), \
        a6 AS (SELECT array_concat(a, a) AS a FROM a5), \
        a7 AS (SELECT array_concat(a, a) AS a FROM a6), \
        a8 AS (SELECT array_concat(a, a) AS a FROM a7), \
        a9 AS (SELECT array_concat(a, a) AS a FROM a8), \
        a10 AS (SELECT array_concat(a, a) AS a FROM a9), \
        a11 AS (SELECT array_concat(a, a) AS a FROM a10), \
        a12 AS (SELECT array_concat(a, a) AS a FROM a11) \
        SELECT array_length(a) AS n FROM a12";
    let array_attempt = async {
        let mut r = engine.query(Language::Sql, array_sql, tight).await?;
        rows(&mut r).await
    }
    .await;
    assert!(
        array_attempt.is_err(),
        "exponential SQL array_concat bypassed memory_bytes budget"
    );

    // 4. Cypher `+` broadcasting a large scalar across many rows must hit the memory pool.
    let broadcast_options = QueryOptions {
        memory_bytes: 8 * 1024 * 1024,
        ..Default::default()
    }
    .with_parameters_json(&serde_json::json!({ "big": "a".repeat(32 * 1024) }).to_string())?;
    let broadcast_attempt = async {
        let mut r = engine
            .query(
                Language::Cypher,
                "WITH range(1, 2000) AS r UNWIND r AS i RETURN $big + toString(i) AS s",
                broadcast_options,
            )
            .await?;
        rows(&mut r).await
    }
    .await;
    assert!(
        broadcast_attempt.is_err(),
        "Cypher scalar broadcast `+` bypassed memory_bytes budget"
    );
    Ok(())
}

#[tokio::test]
async fn cypher_in_list_fast_path_preserves_three_valued_logic_and_nan() -> Result<()> {
    let (_dir, engine) = local_fixture("key: [tenant, id]")?;
    let options = QueryOptions::default().with_parameters_json(
        &serde_json::json!({
            "with_null": [1, serde_json::Value::Null, 3],
            "empty": [],
        })
        .to_string(),
    )?;

    let mut r = engine
        .query(
            Language::Cypher,
            "WITH range(1, 5000) AS big \
             RETURN \
             (null IN $empty) AS null_empty, \
             ((null IN big) IS NULL) AS null_nonempty_is_null, \
             (42 IN big) AS hit_big, \
             (9999 IN big) AS miss_big, \
             (1 IN $with_null) AS hit_with_null, \
             ((2 IN $with_null) IS NULL) AS miss_with_null_is_null, \
             ((0.0 / 0.0) IN [0.0 / 0.0, 1.0]) AS nan_in_nan",
            options.clone(),
        )
        .await?;
    let out = rows(&mut r).await?;
    assert_eq!(
        out,
        vec![serde_json::json!({
            "null_empty": false,
            "null_nonempty_is_null": true,
            "hit_big": true,
            "miss_big": false,
            "hit_with_null": true,
            "miss_with_null_is_null": true,
            "nan_in_nan": false,
        })]
    );

    // Multi-row input (`number_rows > 1`) explicitly exercises the HashSet fast path.
    let mut multi = engine
        .query(
            Language::Cypher,
            "UNWIND [1, 2, null, 0.0 / 0.0] AS x \
             RETURN \
             (x IN $empty) AS in_empty, \
             (x IN $with_null) AS in_with_null, \
             (x IN [0.0 / 0.0, 1.0]) AS in_nan_list",
            options,
        )
        .await?;
    let multi_out = rows(&mut multi).await?;
    assert_eq!(
        multi_out,
        vec![
            serde_json::json!({"in_empty": false, "in_with_null": true, "in_nan_list": true}),
            serde_json::json!({"in_empty": false, "in_nan_list": false}),
            serde_json::json!({"in_empty": false}),
            serde_json::json!({"in_empty": false, "in_nan_list": false}),
        ]
    );
    Ok(())
}

#[tokio::test]
async fn shortest_path_and_all_shortest_paths_find_cycles_back_to_start() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let users = RecordBatch::try_from_iter([
        ("id", Arc::new(Int64Array::from(vec![1, 2, 3, 4])) as _),
        (
            "name",
            Arc::new(StringArray::from(vec!["a", "b", "c", "self"])) as _,
        ),
    ])?;
    let edges = RecordBatch::try_from_iter([
        (
            "id",
            Arc::new(Int64Array::from(vec![10, 11, 12, 13, 14])) as _,
        ),
        ("src", Arc::new(Int64Array::from(vec![1, 2, 1, 3, 4])) as _),
        ("dst", Arc::new(Int64Array::from(vec![2, 1, 3, 1, 4])) as _),
    ])?;
    std::fs::write(dir.path().join("users.parquet"), parquet(&users)?)?;
    std::fs::write(dir.path().join("edges.parquet"), parquet(&edges)?)?;
    let path = dir.path().join("graph.yaml");
    std::fs::write(
        &path,
        "graph: cycles
sources:
  data:
    type: parquet
    tables:
      users: [users.parquet]
      edges: [edges.parquet]
nodes:
  User:
    source: data.users
    key: [id]
    properties: [name]
edges:
  LINK:
    source: data.edges
    key: [id]
    from: { node: User, columns: [src] }
    to: { node: User, columns: [dst] }
",
    )?;
    let engine = Engine::new(Catalog::load(&path)?)?;
    let mut single = engine
        .query(
            Language::Cypher,
            "MATCH (u:User {id: 1}), p = shortestPath((u)-[:LINK*1..4]->(u)) RETURN length(p) AS hops",
            QueryOptions::default(),
        )
        .await?;
    assert_eq!(
        rows(&mut single).await?,
        vec![serde_json::json!({"hops": 2})]
    );

    let mut all = engine
        .query(
            Language::Cypher,
            "MATCH (u:User {id: 1}), p = allShortestPaths((u)-[:LINK*1..4]->(u)) RETURN length(p) AS hops",
            QueryOptions::default(),
        )
        .await?;
    assert_eq!(
        rows(&mut all).await?,
        vec![
            serde_json::json!({"hops": 2}),
            serde_json::json!({"hops": 2}),
        ]
    );

    let mut self_loop = engine
        .query(
            Language::Cypher,
            "MATCH (u:User {id: 4}), p = shortestPath((u)-[:LINK*1..4]->(u)) RETURN length(p) AS hops",
            QueryOptions::default(),
        )
        .await?;
    assert_eq!(
        rows(&mut self_loop).await?,
        vec![serde_json::json!({"hops": 1})]
    );
    Ok(())
}

#[tokio::test]
async fn gremlin_times_zero_repeat_preserves_input_with_barriers() -> Result<()> {
    let (_dir, engine) = local_fixture("key: [tenant, id]")?;
    let mut count_barrier = engine
        .query(
            Language::Gremlin,
            "g.V().hasLabel('Account').times(0).repeat(__.count()).values('status').order()",
            QueryOptions::default(),
        )
        .await?;
    let statuses = rows(&mut count_barrier).await?;
    assert_eq!(
        statuses,
        vec![
            serde_json::json!({"value": "active"}),
            serde_json::json!({"value": "active"}),
            serde_json::json!({"value": "locked"}),
            serde_json::json!({"value": "other"}),
        ]
    );

    let mut dedup_barrier = engine
        .query(
            Language::Gremlin,
            "g.V().hasLabel('Account').times(0).repeat(__.out('Transfer').dedup()).values('status').order()",
            QueryOptions::default(),
        )
        .await?;
    assert_eq!(rows(&mut dedup_barrier).await?, statuses);
    Ok(())
}

#[tokio::test]
async fn undirected_shortest_path_avoids_edge_reuse_and_finds_true_cycles() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let users = RecordBatch::try_from_iter([(
        "id",
        Arc::new(Int64Array::from((1..=12).collect::<Vec<_>>())) as _,
    )])?;
    let edges = RecordBatch::try_from_iter([
        (
            "id",
            Arc::new(Int64Array::from(vec![
                10, // isolated edge 1-2
                20, 21, 22, // triangle 3-4-5-3
                30, 31, 32, 33, // 4-cycle 6-7-8-9-6
                40, 41, // parallel edges 10-11
                50, // self-loop 12-12
            ])) as _,
        ),
        (
            "src",
            Arc::new(Int64Array::from(vec![1, 3, 4, 5, 6, 7, 8, 9, 10, 10, 12])) as _,
        ),
        (
            "dst",
            Arc::new(Int64Array::from(vec![2, 4, 5, 3, 7, 8, 9, 6, 11, 11, 12])) as _,
        ),
    ])?;
    std::fs::write(dir.path().join("users.parquet"), parquet(&users)?)?;
    std::fs::write(dir.path().join("edges.parquet"), parquet(&edges)?)?;
    let path = dir.path().join("graph.yaml");
    std::fs::write(
        &path,
        "graph: undirected_cycles
sources:
  data:
    type: parquet
    tables:
      users: [users.parquet]
      edges: [edges.parquet]
nodes:
  User:
    source: data.users
    key: [id]
    properties: []
edges:
  LINK:
    source: data.edges
    key: [id]
    from: { node: User, columns: [src] }
    to: { node: User, columns: [dst] }
",
    )?;
    let engine = Engine::new(Catalog::load(&path)?)?;

    // Isolated undirected edge 1-2 must NOT produce an edge-repeating [10, 10] 2-hop cycle.
    let mut no_cycle = engine
        .query(
            Language::Cypher,
            "MATCH (u:User {id: 1}), p = shortestPath((u)-[:LINK*1..4]-(u)) RETURN length(p) AS hops",
            QueryOptions::default(),
        )
        .await?;
    assert_eq!(rows(&mut no_cycle).await?, Vec::<serde_json::Value>::new());

    // Open-ended undirected shortestPath from 1 must reach only 2, never bounce back to 1.
    let mut open_ended = engine
        .query(
            Language::Cypher,
            "MATCH (u:User {id: 1}), p = shortestPath((u)-[:LINK*1..4]-(v:User)) RETURN v.id AS dst, length(p) AS hops ORDER BY dst",
            QueryOptions::default(),
        )
        .await?;
    assert_eq!(
        rows(&mut open_ended).await?,
        vec![serde_json::json!({"dst": 2, "hops": 1})]
    );

    // Triangle 3-4-5-3: shortest undirected cycle has length 3; allShortestPaths yields both orientations.
    let mut triangle = engine
        .query(
            Language::Cypher,
            "MATCH (u:User {id: 3}), p = shortestPath((u)-[:LINK*1..5]-(u)) RETURN length(p) AS hops, [r IN relationships(p) | r.id] AS rids",
            QueryOptions::default(),
        )
        .await?;
    let tri_rows = rows(&mut triangle).await?;
    assert_eq!(tri_rows.len(), 1);
    assert_eq!(tri_rows[0]["hops"], 3);
    let rids = tri_rows[0]["rids"].as_array().unwrap();
    let unique: std::collections::BTreeSet<_> = rids.iter().filter_map(|v| v.as_i64()).collect();
    assert_eq!(unique.len(), 3, "triangle cycle must use 3 distinct edges");

    let mut triangle_all = engine
        .query(
            Language::Cypher,
            "MATCH (u:User {id: 3}), p = allShortestPaths((u)-[:LINK*1..5]-(u)) RETURN length(p) AS hops",
            QueryOptions::default(),
        )
        .await?;
    assert_eq!(
        rows(&mut triangle_all).await?,
        vec![
            serde_json::json!({"hops": 3}),
            serde_json::json!({"hops": 3}),
        ]
    );

    // 4-cycle 6-7-8-9-6: length 4, both orientations.
    let mut square_all = engine
        .query(
            Language::Cypher,
            "MATCH (u:User {id: 6}), p = allShortestPaths((u)-[:LINK*1..5]-(u)) RETURN length(p) AS hops",
            QueryOptions::default(),
        )
        .await?;
    assert_eq!(
        rows(&mut square_all).await?,
        vec![
            serde_json::json!({"hops": 4}),
            serde_json::json!({"hops": 4}),
        ]
    );

    // Parallel edges 10-11 (ids 40 and 41): valid edge-unique 2-cycle in both orientations.
    let mut parallel_all = engine
        .query(
            Language::Cypher,
            "MATCH (u:User {id: 10}), p = allShortestPaths((u)-[:LINK*1..4]-(u)) RETURN length(p) AS hops, [r IN relationships(p) | r.id] AS rids",
            QueryOptions::default(),
        )
        .await?;
    let par_rows = rows(&mut parallel_all).await?;
    assert_eq!(par_rows.len(), 2);
    for row in &par_rows {
        assert_eq!(row["hops"], 2);
        let ids: std::collections::BTreeSet<_> = row["rids"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|v| v.as_i64())
            .collect();
        assert_eq!(ids, std::collections::BTreeSet::from([40, 41]));
    }

    // Undirected self-loop 12-12: length 1.
    let mut self_loop = engine
        .query(
            Language::Cypher,
            "MATCH (u:User {id: 12}), p = shortestPath((u)-[:LINK*1..4]-(u)) RETURN length(p) AS hops",
            QueryOptions::default(),
        )
        .await?;
    assert_eq!(
        rows(&mut self_loop).await?,
        vec![serde_json::json!({"hops": 1})]
    );
    Ok(())
}

#[tokio::test]
async fn shortest_path_where_can_reference_outer_variables() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let users =
        RecordBatch::try_from_iter([("id", Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])) as _)])?;
    let edges = RecordBatch::try_from_iter([
        ("id", Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])) as _),
        ("src", Arc::new(Int64Array::from(vec![1, 2, 1, 3, 5])) as _),
        ("dst", Arc::new(Int64Array::from(vec![2, 4, 3, 5, 4])) as _),
        (
            "weight",
            Arc::new(Int64Array::from(vec![10, 10, 50, 50, 50])) as _,
        ),
    ])?;
    std::fs::write(dir.path().join("users.parquet"), parquet(&users)?)?;
    std::fs::write(dir.path().join("edges.parquet"), parquet(&edges)?)?;
    let path = dir.path().join("graph.yaml");
    std::fs::write(
        &path,
        "graph: outer_shortest
sources:
  data:
    type: parquet
    tables:
      users: [users.parquet]
      edges: [edges.parquet]
nodes:
  User:
    source: data.users
    key: [id]
    properties: []
edges:
  LINK:
    source: data.edges
    key: [id]
    from: { node: User, columns: [src] }
    to: { node: User, columns: [dst] }
    properties: [weight]
",
    )?;
    let engine = Engine::new(Catalog::load(&path)?)?;

    let mut filtered = engine
        .query(
            Language::Cypher,
            "WITH 25 AS min_w MATCH (a:User {id: 1}), (b:User {id: 4}) MATCH p = shortestPath((a)-[r:LINK*1..5]->(b)) WHERE all(e IN r WHERE e.weight >= min_w) RETURN length(p) AS hops",
            QueryOptions::default(),
        )
        .await?;
    assert_eq!(
        rows(&mut filtered).await?,
        vec![serde_json::json!({"hops": 3})]
    );

    let mut inline_endpoints = engine
        .query(
            Language::Cypher,
            "WITH 25 AS min_w MATCH p = shortestPath((a:User {id: 1})-[r:LINK*1..5]->(b:User {id: 4})) WHERE all(e IN r WHERE e.weight >= min_w) RETURN length(p) AS hops",
            QueryOptions::default(),
        )
        .await?;
    assert_eq!(
        rows(&mut inline_endpoints).await?,
        vec![serde_json::json!({"hops": 3})]
    );

    let mut optional_none = engine
        .query(
            Language::Cypher,
            "WITH 100 AS min_w MATCH (a:User {id: 1}), (b:User {id: 4}) OPTIONAL MATCH p = shortestPath((a)-[r:LINK*1..5]->(b)) WHERE all(e IN r WHERE e.weight >= min_w) RETURN length(p) AS hops",
            QueryOptions::default(),
        )
        .await?;
    assert_eq!(rows(&mut optional_none).await?, vec![serde_json::json!({})]);
    Ok(())
}

#[tokio::test]
async fn gremlin_repeat_barriers_where_predicates_and_variant_mean() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let accounts = RecordBatch::try_from_iter([
        ("id", Arc::new(Int64Array::from(vec![1, 2])) as _),
        ("score", Arc::new(Int64Array::from(vec![10, 20])) as _),
    ])?;
    let merchants = RecordBatch::try_from_iter([
        ("id", Arc::new(Int64Array::from(vec![3])) as _),
        (
            "score",
            Arc::new(graphlake::arrow::array::Float64Array::from(vec![30.0])) as _,
        ),
    ])?;
    let transfers = RecordBatch::try_from_iter([
        ("id", Arc::new(Int64Array::from(vec![1, 2])) as _),
        ("src", Arc::new(Int64Array::from(vec![1, 2])) as _),
        ("dst", Arc::new(Int64Array::from(vec![2, 1])) as _),
    ])?;
    std::fs::write(dir.path().join("accounts.parquet"), parquet(&accounts)?)?;
    std::fs::write(dir.path().join("merchants.parquet"), parquet(&merchants)?)?;
    std::fs::write(dir.path().join("transfers.parquet"), parquet(&transfers)?)?;
    let path = dir.path().join("graph.yaml");
    std::fs::write(
        &path,
        "graph: gremlin_regressions
sources:
  data:
    type: parquet
    tables:
      accounts: [accounts.parquet]
      merchants: [merchants.parquet]
      transfers: [transfers.parquet]
nodes:
  Account:
    source: data.accounts
    key: [id]
    properties: [score]
  Merchant:
    source: data.merchants
    key: [id]
    properties: [score]
edges:
  Transfer:
    source: data.transfers
    key: [id]
    from: { node: Account, columns: [src] }
    to: { node: Account, columns: [dst] }
",
    )?;
    let engine = Engine::new(Catalog::load(&path)?)?;

    // times(0).emit().repeat(...) must preserve input kind rather than merging the unexecuted body's type.
    let mut emit_zero = engine
        .query(
            Language::Gremlin,
            "g.V().hasLabel('Account').times(0).emit().repeat(__.label()).values('score').sum()",
            QueryOptions::default(),
        )
        .await?;
    assert_eq!(
        rows(&mut emit_zero).await?,
        vec![serde_json::json!({"value": 30})]
    );

    // fold() and group() inside repeat must be treated as barriers (preserving input on times(0) and working with fold().unfold()).
    let mut times_zero_fold = engine
        .query(
            Language::Gremlin,
            "g.V().hasLabel('Account').values('score').order().times(0).repeat(__.fold())",
            QueryOptions::default(),
        )
        .await?;
    assert_eq!(
        rows(&mut times_zero_fold).await?,
        vec![
            serde_json::json!({"value": 10}),
            serde_json::json!({"value": 20}),
        ]
    );

    let mut times_zero_group = engine
        .query(
            Language::Gremlin,
            "g.V().hasLabel('Account').values('score').order().times(0).repeat(__.group())",
            QueryOptions::default(),
        )
        .await?;
    assert_eq!(
        rows(&mut times_zero_group).await?,
        vec![
            serde_json::json!({"value": 10}),
            serde_json::json!({"value": 20}),
        ]
    );

    let mut repeat_fold_unfold = engine
        .query(
            Language::Gremlin,
            "g.V().hasLabel('Account').values('score').order().repeat(__.fold().unfold()).times(1)",
            QueryOptions::default(),
        )
        .await?;
    assert_eq!(
        rows(&mut repeat_fold_unfold).await?,
        vec![
            serde_json::json!({"value": 10}),
            serde_json::json!({"value": 20}),
        ]
    );

    // where('a', neq('b')) and where(P.neq('a')) inside repeat must not be rejected as nested traversals.
    let mut where_two_arg = engine
        .query(
            Language::Gremlin,
            "g.V().hasLabel('Account').as('a').repeat(__.out('Transfer').as('b').where('a', neq('b'))).times(1).count()",
            QueryOptions::default(),
        )
        .await?;
    assert_eq!(
        rows(&mut where_two_arg).await?,
        vec![serde_json::json!({"value": 2})]
    );

    let mut where_one_arg = engine
        .query(
            Language::Gremlin,
            "g.V().hasLabel('Account').as('a').repeat(__.out('Transfer').where(P.neq('a'))).times(1).count()",
            QueryOptions::default(),
        )
        .await?;
    assert_eq!(
        rows(&mut where_one_arg).await?,
        vec![serde_json::json!({"value": 2})]
    );

    // Mixed numeric variant values through min().mean() must unwrap Variant struct in __graphlake_gremlin_number.
    let mut variant_mean = engine
        .query(
            Language::Gremlin,
            "g.V().values('score').min().mean()",
            QueryOptions::default(),
        )
        .await?;
    assert_eq!(
        rows(&mut variant_mean).await?,
        vec![serde_json::json!({"value": 10.0})]
    );
    Ok(())
}
