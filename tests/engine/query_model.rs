use std::sync::Arc;

use anyhow::{Context, Result};
use graphlake::{
    Catalog, Comparison, Direction, Engine, GraphOptions, GraphProjection, Language, Predicate,
    ProjectionAlgorithm, QueryOptions, ScalarValue, Traversal, TraversalKind, TraversalStrategy,
    arrow::array::{BooleanArray, Date32Array, Int32Array, Int64Array, RecordBatch, StringArray},
};
use serde_json::json;

use super::{local_fixture, parquet, rows, transfers};

#[tokio::test]
async fn cypher_aggregated_order_only_uses_projected_values() -> Result<()> {
    let (_dir, engine) = local_fixture("key: [tenant, id]")?;
    let error = engine
        .query(
            Language::Cypher,
            "MATCH (a:Account) RETURN a.tenant AS tenant, count(*) AS n ORDER BY a.id",
            Default::default(),
        )
        .await
        .err()
        .context("an ungrouped order key was accepted")?;
    assert!(format!("{error:#}").contains("ORDER BY after aggregation"));
    let mut result = engine
        .query(
            Language::Cypher,
            "MATCH (a:Account) RETURN a.tenant AS tenant, count(*) AS n ORDER BY n DESC",
            Default::default(),
        )
        .await?;
    assert_eq!(
        rows(&mut result).await?,
        [json!({"tenant": 1, "n": 3}), json!({"tenant": 2, "n": 1})]
    );
    let mut result = engine
        .query(
            Language::Cypher,
            "MATCH (a:Account) RETURN a.tenant AS tenant, count(*) AS n
             ORDER BY a.tenant + count(a.id) ASC, n DESC",
            Default::default(),
        )
        .await?;
    assert_eq!(
        rows(&mut result).await?,
        [json!({"tenant": 2, "n": 1}), json!({"tenant": 1, "n": 3})]
    );
    Ok(())
}

#[tokio::test]
async fn variable_path_reuses_a_constant_anchor_from_the_previous_match() -> Result<()> {
    let (_dir, engine) = local_fixture("key: [tenant, id]")?;
    let mut result = engine
        .query(
            Language::Cypher,
            "MATCH (a:Account) WHERE a.tenant=1 AND a.id IN $ids
         MATCH (a)-[:Transfer*1..2]->(b:Account) RETURN b.id AS id ORDER BY id",
            QueryOptions::default().with_parameters_json(r#"{"ids":[1]}"#)?,
        )
        .await?;
    let actual = rows(&mut result).await?;
    assert!(!actual.is_empty());
    assert_eq!(result.metrics().visits_by_depth.first(), Some(&1));
    let mut same_match = engine.query(Language::Cypher,
        "MATCH (a:Account)-[:Transfer*1..2]->(b:Account) WHERE a.tenant=1 AND a.id IN [1] RETURN b.id AS id ORDER BY id",
        Default::default()).await?;
    assert_eq!(actual, rows(&mut same_match).await?);
    Ok(())
}

#[tokio::test]
async fn cypher_inline_properties_can_reference_values_in_scope() -> Result<()> {
    let (_dir, engine) = local_fixture("key: [tenant, id]")?;
    let mut result = engine
        .query(
            Language::Cypher,
            "UNWIND [1,3] AS i MATCH (a:Account {tenant: 1, id: i}) RETURN a.id AS id ORDER BY id",
            Default::default(),
        )
        .await?;
    assert_eq!(
        rows(&mut result).await?,
        [json!({"id": 1}), json!({"id": 3})]
    );
    Ok(())
}

#[tokio::test]
async fn cypher_size_counts_unicode_and_preserves_list_and_null_results() -> Result<()> {
    let (_dir, engine) = local_fixture("key: [tenant, id]")?;
    let mut result = engine.query(Language::Cypher,
        "WITH '中🦊a' AS text RETURN size(text) AS text_len, size([1,null,2]) AS list_len, size(null) AS null_len",
        Default::default()).await?;
    use futures::TryStreamExt;
    let batch = result
        .batches
        .try_next()
        .await?
        .context("missing projected row")?;
    for name in ["text_len", "list_len"] {
        let array = batch
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<graphlake::arrow::array::UInt64Array>()
            .unwrap();
        assert_eq!(array.value(0), 3);
    }
    assert!(batch.column_by_name("null_len").unwrap().is_null(0));
    Ok(())
}

#[tokio::test]
async fn scd2_current_and_historical_use_validity_with_null_and_sentinel() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let batch = RecordBatch::try_from_iter([
        (
            "id",
            Arc::new(Int64Array::from(vec![1, 1, 2, 2, 3, 4])) as _,
        ),
        (
            "name",
            Arc::new(StringArray::from(vec![
                "effective",
                "future",
                "expired",
                "next",
                "null-end",
                "sentinel-end",
            ])) as _,
        ),
        (
            "start",
            Arc::new(Date32Array::from(vec![
                Some(15_000),
                Some(40_000),
                Some(12_000),
                Some(40_000),
                None,
                Some(15_000),
            ])) as _,
        ),
        (
            "end",
            Arc::new(Date32Array::from(vec![
                Some(40_000),
                Some(47_482),
                Some(15_000),
                None,
                None,
                Some(47_482),
            ])) as _,
        ),
    ])?;
    std::fs::write(dir.path().join("scd.parquet"), parquet(&batch)?)?;
    let config = dir.path().join("scd.yaml");
    std::fs::write(
        &config,
        r#"graph: history
sources:
  lake: {type: parquet, tables: {records: [scd.parquet]}}
nodes:
  Record:
    source: lake.records
    key: [id]
    properties: [name]
    scd2: {valid_from: start, valid_to: end, open_end: '2100-01-01'}
"#,
    )?;
    let engine = Engine::new(Catalog::load(config)?)?;
    for (time, expected) in [(None, "effective"), (Some("2080-01-01"), "future")] {
        let mut result = engine
            .query(
                Language::Cypher,
                "MATCH (n:Record) WHERE n.id <> 2 RETURN n.name AS name ORDER BY n.id",
                QueryOptions {
                    graph: GraphOptions {
                        snapshot_time: time.map(str::to_owned),
                        ..Default::default()
                    },
                    ..Default::default()
                },
            )
            .await?;
        assert_eq!(
            rows(&mut result).await?,
            [
                json!({"name": expected}),
                json!({"name": "null-end"}),
                json!({"name": "sentinel-end"})
            ]
        );
    }
    Ok(())
}

#[tokio::test]
async fn gremlin_has_label_without_a_model_mapping_matches_no_elements() -> Result<()> {
    let (_dir, engine) = local_fixture("key: [tenant, id]")?;
    let mut result = engine
        .query(
            Language::Gremlin,
            "g.V().hasLabel('Missing').count()",
            Default::default(),
        )
        .await?;
    assert_eq!(rows(&mut result).await?, [json!({"value": 0})]);
    Ok(())
}

#[tokio::test]
async fn cypher_correlated_call_paginates_in_input_order_across_partitions() -> Result<()> {
    let (_dir, engine) = local_fixture("key: [tenant, id]")?;
    for (body, repeated) in [
        ("CALL (k) { RETURN k AS x } RETURN x AS y", false),
        ("RETURN k AS y UNION ALL RETURN k AS y", true),
        ("WITH DISTINCT k RETURN k AS y", false),
    ] {
        let query = format!(
            "UNWIND range(1,40) AS k WITH k ORDER BY k DESC
             CALL (k) {{ {body} }} RETURN y SKIP 5 LIMIT 10"
        );
        let mut result = engine
            .query(
                Language::Cypher,
                &query,
                QueryOptions {
                    partitions: 4,
                    batch_size: 1,
                    ..Default::default()
                },
            )
            .await
            .with_context(|| query.clone())?;
        let expected = (1..=40)
            .rev()
            .flat_map(|y| std::iter::repeat_n(y, if repeated { 2 } else { 1 }))
            .skip(5)
            .take(10)
            .map(|y| json!({"y":y}))
            .collect::<Vec<_>>();
        assert_eq!(rows(&mut result).await?, expected, "{query}");
    }
    Ok(())
}

#[tokio::test]
async fn cypher_correlated_call_large_page_preserves_key_and_subquery_order() -> Result<()> {
    let (_dir, engine) = local_fixture("key: [tenant, id]")?;
    for distinct in ["", "DISTINCT "] {
        let query = format!(
            "UNWIND range(1,112) + [112,1] AS k WITH k ORDER BY k DESC
             CALL (k) {{
                UNWIND CASE WHEN k % 3 = 0 THEN [] ELSE [2,1,3,3] END AS i
                RETURN {distinct}k * 10 + i AS y ORDER BY y DESC LIMIT 2
             }}
             RETURN k,y SKIP 7 LIMIT 130"
        );
        let mut result = engine
            .query(
                Language::Cypher,
                &query,
                QueryOptions {
                    partitions: 4,
                    batch_size: 64,
                    ..Default::default()
                },
            )
            .await
            .with_context(|| query.clone())?;
        let expected = (1..=112)
            .rev()
            .flat_map(|k| std::iter::repeat_n(k, if k == 112 || k == 1 { 2 } else { 1 }))
            .filter(|k| k % 3 != 0)
            .flat_map(|k| {
                [3, if distinct.is_empty() { 3 } else { 2 }].map(|i| json!({"k":k,"y": k * 10 + i}))
            })
            .skip(7)
            .take(130)
            .collect::<Vec<_>>();
        assert_eq!(rows(&mut result).await?, expected, "{query}");
    }
    let mut result = engine
        .query(
            Language::Cypher,
            "UNWIND range(1,80) AS k WITH k ORDER BY k DESC
             CALL (k) {
                 UNWIND range(0,114) AS i WITH i LIMIT 115
                 RETURN k * 1000 + i AS y ORDER BY y DESC SKIP 110 LIMIT 2
             } RETURN k,y SKIP 7 LIMIT 130",
            QueryOptions {
                partitions: 4,
                batch_size: 64,
                ..Default::default()
            },
        )
        .await?;
    let expected = (1..=80)
        .rev()
        .flat_map(|k| [4, 3].map(|i| json!({"k":k,"y": k * 1000 + i})))
        .skip(7)
        .take(130)
        .collect::<Vec<_>>();
    assert_eq!(rows(&mut result).await?, expected);
    Ok(())
}

#[tokio::test]
async fn cypher_correlated_call_expands_only_the_imported_vertices_across_batches() -> Result<()> {
    let dir = tempfile::tempdir()?;
    rusqlite::Connection::open(dir.path().join("ring.db"))?.execute_batch(
        "CREATE TABLE vertices (id INTEGER PRIMARY KEY) STRICT;
         CREATE TABLE edges (id INTEGER PRIMARY KEY, src INTEGER, dst INTEGER) STRICT;
         WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<64)
         INSERT INTO vertices SELECT i FROM n;
         INSERT INTO edges SELECT id, id, id%64+1 FROM vertices;",
    )?;
    let config = dir.path().join("ring.yaml");
    std::fs::write(
        &config,
        "graph: ring
sources:
  db: {type: sqlite, path: ring.db}
nodes:
  N: {source: db.vertices, key: [id]}
edges:
  E: {source: db.edges, key: [id], from: {node: N, columns: [src]}, to: {node: N, columns: [dst]}}
",
    )?;
    let engine = Engine::new(Catalog::load(config)?)?;
    let mut result = engine
        .query(
            Language::Cypher,
            "MATCH (p:N) WHERE p.id <= 33 WITH p ORDER BY p.id
         CALL (p) { MATCH (p)-[:E*1..2]->(q:N) RETURN q.id AS to }
         RETURN p.id AS start_id, to ORDER BY start_id, to",
            QueryOptions {
                batch_size: 64,
                ..Default::default()
            },
        )
        .await?;
    let expected = (1..=33)
        .flat_map(|from| [from + 1, from + 2].map(|to| json!({"start_id":from,"to":to})))
        .collect::<Vec<_>>();
    assert_eq!(rows(&mut result).await?, expected);
    assert_eq!(result.metrics().visits_by_depth.get(1).copied(), Some(33));
    Ok(())
}

#[tokio::test]
async fn cypher_row_call_replays_ordered_parquet_reads_for_each_key() -> Result<()> {
    let (_dir, engine) = local_fixture("key: [tenant, id]")?;
    let mut result = engine
        .query(
            Language::Cypher,
            "UNWIND [1,3,2,1] AS k
             CALL (k) {
                 MATCH (a:Account) WHERE a.id >= k
                 RETURN DISTINCT a.id AS id ORDER BY id LIMIT 1
             } RETURN k,id",
            QueryOptions {
                batch_size: 1,
                partitions: 4,
                ..Default::default()
            },
        )
        .await?;
    assert_eq!(
        rows(&mut result).await?,
        [
            json!({"k":1,"id":1}),
            json!({"k":3,"id":3}),
            json!({"k":2,"id":2}),
            json!({"k":1,"id":1})
        ]
    );
    Ok(())
}

#[tokio::test]
async fn cypher_with_filters_after_per_key_pagination() -> Result<()> {
    let (_dir, engine) = local_fixture("key: [tenant, id]")?;
    let mut result = engine
        .query(
            Language::Cypher,
            "UNWIND [1,2,3,4] AS x WITH x AS y ORDER BY y SKIP 1 LIMIT 1 WHERE x > 1 RETURN y",
            Default::default(),
        )
        .await?;
    assert_eq!(rows(&mut result).await?, [json!({"y":2})]);

    let mut scoped = engine
        .query(
            Language::Cypher,
            "MATCH (a:Account {tenant:1}) WITH a.id AS id ORDER BY id LIMIT 1
         WHERE EXISTS { MATCH (a)-[:Transfer]->(:Account) }
         RETURN id",
            Default::default(),
        )
        .await?;
    assert_eq!(rows(&mut scoped).await?, [json!({"id":1})]);

    for (projection, items, predicate) in [
        ("x AS y", "[1,2,3]", "x > k AND y > k"),
        ("DISTINCT x AS y", "[1,2,2,3]", "y > k"),
        ("x AS y, count(*) AS n", "[1,2,2,3,3,3]", "n > k"),
    ] {
        let query = format!(
            "UNWIND [2,1,2] AS k CALL (k) {{
                UNWIND {items} AS x WITH {projection}
                ORDER BY y SKIP 0 LIMIT 1 WHERE {predicate} RETURN y
             }} RETURN k,y ORDER BY k"
        );
        let mut result = engine
            .query(
                Language::Cypher,
                &query,
                QueryOptions {
                    partitions: 4,
                    batch_size: 1,
                    ..Default::default()
                },
            )
            .await
            .with_context(|| query.clone())?;
        assert!(rows(&mut result).await?.is_empty(), "{query}");
    }

    let mut result = engine
        .query(
            Language::Cypher,
            "UNWIND [0,2,0] AS k CALL (k) {
            UNWIND CASE WHEN k = 0 THEN [] ELSE [1,2] END AS x
            WITH count(*) + 1 AS n, collect(x) AS xs ORDER BY n LIMIT 1
            RETURN n,xs
         } RETURN k,n,xs ORDER BY k",
            QueryOptions {
                partitions: 4,
                batch_size: 1,
                ..Default::default()
            },
        )
        .await?;
    let mut results = rows(&mut result).await?;
    results[2]["xs"]
        .as_array_mut()
        .context("aggregate list")?
        .sort_by_key(|x| x.as_i64());
    assert_eq!(
        results,
        [
            json!({"k":0,"n":1,"xs":[]}),
            json!({"k":0,"n":1,"xs":[]}),
            json!({"k":2,"n":3,"xs":[1,2]}),
        ]
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires a writable PostgreSQL fixture"]
async fn cypher_correlated_call_reads_keys_and_columns_and_reuses_batches_postgres() -> Result<()> {
    let dsn = std::env::var("GRAPHLAKE_TEST_POSTGRES")?;
    let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls).await?;
    let driver = tokio::spawn(connection);
    let schema = format!("graphlake_call_{}", std::process::id());
    client.batch_execute(&format!(
        "CREATE SCHEMA {schema};
         CREATE TABLE {schema}.nodes (id bigint PRIMARY KEY, payload text);
         CREATE TABLE {schema}.edges (id bigint PRIMARY KEY, src bigint, dst bigint);
         CREATE INDEX ON {schema}.edges (src);
         INSERT INTO {schema}.nodes SELECT i, repeat('x', 2048) FROM generate_series(1,4096) i;
         INSERT INTO {schema}.edges SELECT i,i,i+1 FROM generate_series(1,4095) i;
         INSERT INTO {schema}.nodes SELECT i, repeat('x', 2048) FROM generate_series(10000,30000) i;
         INSERT INTO {schema}.nodes SELECT i, NULL FROM generate_series(31000,100000) i;
         INSERT INTO {schema}.edges SELECT i,10000,i FROM generate_series(10001,15000) i;
         CREATE TABLE {schema}.across (LIKE {schema}.edges INCLUDING ALL);
         INSERT INTO {schema}.across SELECT i,30000,i FROM generate_series(10001,22000) i;
         INSERT INTO {schema}.across VALUES (200001,30000,10001),(200002,30000,999999);
         INSERT INTO {schema}.edges SELECT i + 30000,20000,i FROM generate_series(10001,15000) i;
         CREATE TABLE {schema}.mapped_delta (uid bigint PRIMARY KEY, text_b text);
         INSERT INTO {schema}.mapped_delta VALUES (1, repeat('x',2048)), (2,'conflicting');
         ANALYZE {schema}.nodes;
         ANALYZE {schema}.edges;
         CREATE TABLE {schema}.names (tenant bigint, id text COLLATE \"C\", PRIMARY KEY (tenant,id));
         CREATE TABLE {schema}.links (
             id bigint PRIMARY KEY, st bigint, si text COLLATE \"POSIX\", dt bigint, di text COLLATE \"POSIX\"
         );
         CREATE INDEX ON {schema}.links (st,si);"
    )).await?;
    let text_id = "a'\\甲";
    client
        .execute(
            &format!("INSERT INTO {schema}.names VALUES (1,$1),(1,'target'),(2,$1)"),
            &[&text_id],
        )
        .await?;
    client
        .execute(
            &format!("INSERT INTO {schema}.links VALUES (1,1,$1,1,'target')"),
            &[&text_id],
        )
        .await?;
    let check = async {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("graph.yaml");
        std::fs::write(
            &path,
            format!(
                r#"
graph: call
sources:
  pg: {{type: postgres, dsn_env: GRAPHLAKE_TEST_POSTGRES}}
  remote: {{type: postgres, dsn_env: GRAPHLAKE_TEST_POSTGRES}}
nodes:
  Node: {{source: pg.{schema}.nodes, key: [id], properties: [payload]}}
  Mapped:
    key: [id]
    properties: [payload]
    inputs:
      - table: pg.{schema}.nodes
      - table: pg.{schema}.mapped_delta
        columns: {{id: uid, payload: text_b}}
  Name: {{source: pg.{schema}.names, key: [tenant,id]}}
edges:
  Next:
    source: pg.{schema}.edges
    key: [id]
    from: {{node: Node, columns: [src]}}
    to: {{node: Node, columns: [dst]}}
  Across:
    source: remote.{schema}.across
    key: [id]
    from: {{node: Node, columns: [src]}}
    to: {{node: Node, columns: [dst]}}
  Link:
    source: pg.{schema}.links
    key: [id]
    from: {{node: Name, columns: [st,si]}}
    to: {{node: Name, columns: [dt,di]}}
"#
            ),
        )?;
        let engine = Engine::new(Catalog::load(path)?)?;
        let options = QueryOptions {
            batch_size: 8,
            graph: GraphOptions {
                require_consistent_snapshot: true,
                ..Default::default()
            },
            ..Default::default()
        };
        for (query, expected, max_reads) in [
            (
                "MATCH (m:Mapped {id:1}) RETURN m.id AS id",
                vec![json!({"id":1})],
                2,
            ),
            (
                "MATCH (m:Mapped {id:2}) RETURN m.id AS id LIMIT 0",
                Vec::new(),
                0,
            ),
            (
                "UNWIND [1,4000] AS k CALL (k) { MATCH (m:Mapped) WHERE m.id=k
              RETURN m.id AS id } RETURN id ORDER BY id",
                vec![json!({"id":1}), json!({"id":4000})],
                3,
            ),
        ] {
            let mut result = engine
                .query(Language::Cypher, query, options.clone())
                .await?;
            assert_eq!(rows(&mut result).await?, expected, "{query}");
            assert!(result.metrics().reads.postgres_rows <= max_reads, "{query}");
        }
        let conflict = async {
            let mut result = engine
                .query(
                    Language::Cypher,
                    "MATCH (m:Mapped {id:2}) WHERE m.payload='conflicting' RETURN m.id AS id",
                    options.clone(),
                )
                .await?;
            rows(&mut result).await
        }
        .await
        .expect_err("conflicting records for a matched key");
        assert!(format!("{conflict:#}").contains("conflicting records for Mapped"));

        let source = "MATCH (a:Node {id:1}) UNWIND range(1,64) AS i WITH a,i%2 AS k
            CALL (a,k) { MATCH (a)-[:Next]->()-[:Next]->(b) RETURN b.id+k AS child }";
        for (limit, expected) in [(0, 0), (64, 64)] {
            let mut result = engine
                .query(
                    Language::Cypher,
                    &format!("{source} RETURN child LIMIT {limit}"),
                    options.clone(),
                )
                .await?;
            let mut children = rows(&mut result).await?;
            children.sort_by_key(|row| row["child"].as_i64());
            let expected_rows = (0..expected)
                .map(|i| json!({"child": if i < expected / 2 {3} else {4}}))
                .collect::<Vec<_>>();
            assert_eq!(children, expected_rows);
            let reads = result.metrics().reads;
            if limit == 0 {
                assert_eq!(reads.postgres_rows, 0);
            } else {
                assert!(reads.postgres_rows < 40, "{reads:?}");
                assert!(reads.postgres_arrow_bytes < 4096, "{reads:?}");
            }
        }
        let mut result = engine
            .query(
                Language::Cypher,
                "UNWIND [1,4000,5000] AS k CALL (k) { MATCH (b:Node) WHERE b.id=k
             RETURN b.id AS id } RETURN id ORDER BY id",
                options.clone(),
            )
            .await?;
        assert_eq!(
            rows(&mut result).await?,
            [json!({"id":1}), json!({"id":4000})]
        );
        assert_eq!(result.metrics().reads.postgres_rows, 2);
        assert!(result.metrics().reads.postgres_arrow_bytes < 512);

        let mut result = engine
            .query(
                Language::Cypher,
                "UNWIND range(1,128) AS k WITH k ORDER BY k
                 CALL (k) { MATCH (b:Node) WHERE b.id = k RETURN b.id AS id }
                 RETURN id LIMIT 256",
                QueryOptions {
                    batch_size: 128,
                    ..options.clone()
                },
            )
            .await?;
        let expected = (1..=128).map(|id| json!({"id":id})).collect::<Vec<_>>();
        assert_eq!(rows(&mut result).await?, expected);
        let reads = result.metrics().reads;
        assert!((1..64).contains(&reads.requests), "{reads:?}");
        assert_eq!(reads.postgres_rows, 128, "{reads:?}");
        eprintln!(
            "CALL 128 distinct matching keys: {} PostgreSQL requests",
            reads.requests
        );

        let mut result = engine
            .query(
                Language::Cypher,
                "UNWIND range(100001,100128) + [1] AS k WITH k ORDER BY k DESC
                 CALL (k) { MATCH (b:Node) WHERE b.id = k RETURN b.id AS id }
                 RETURN id LIMIT 1",
                QueryOptions {
                    batch_size: 128,
                    ..options.clone()
                },
            )
            .await?;
        assert_eq!(rows(&mut result).await?, [json!({"id":1})]);
        let reads = result.metrics().reads;
        assert!((1..64).contains(&reads.requests), "{reads:?}");
        assert_eq!(reads.postgres_rows, 1, "{reads:?}");
        eprintln!(
            "CALL sparse LIMIT 1: {} PostgreSQL requests",
            reads.requests
        );

        let mut result = engine
            .query(
                Language::Cypher,
                "MATCH (a:Node {id:10000}) CALL (a) {
                MATCH (a)-[e:Next]->(b)
                RETURN count(e) AS n,min(b.id) AS lo,max(b.id) AS hi
             } RETURN n,lo,hi",
                QueryOptions {
                    batch_size: 1024,
                    memory_bytes: 64 * 1024 * 1024,
                    ..options.clone()
                },
            )
            .await?;
        assert_eq!(
            rows(&mut result).await?,
            [json!({"n":5000,"lo":10001,"hi":15000})]
        );
        let reads = result.metrics().reads;
        assert!(reads.postgres_rows < 22000, "{reads:?}");
        assert!(reads.postgres_arrow_bytes < 1024 * 1024, "{reads:?}");
        for stage in ["", "WITH b LIMIT 1"] {
            let mut result = engine
                .query(
                    Language::Cypher,
                    &format!(
                        "MATCH (a:Node {{id:10000}}) CALL (a) {{
                    MATCH (a)-[:Next]->(b) {stage} RETURN b.payload AS p
                }} RETURN p IS NOT NULL AS ok LIMIT 1"
                    ),
                    QueryOptions {
                        memory_bytes: 8 * 1024 * 1024,
                        spill_bytes: 0,
                        ..options.clone()
                    },
                )
                .await?;
            assert_eq!(rows(&mut result).await?, [json!({"ok":true})]);
            let reads = result.metrics().reads;
            assert!(reads.postgres_arrow_bytes < 1024 * 1024, "{reads:?}");
        }

        let mut result = engine
            .query(
                Language::Cypher,
                "MATCH (a:Node {id:10000}) CALL (a) {
                MATCH (c)-[:Next]->(b)<-[:Next]-(a)
                RETURN count(*) AS n,count(DISTINCT c.id) AS d
             } RETURN n,d",
                QueryOptions {
                    batch_size: 1024,
                    memory_bytes: 64 * 1024 * 1024,
                    ..options.clone()
                },
            )
            .await?;
        assert_eq!(rows(&mut result).await?, [json!({"n":5000,"d":1})]);
        let reads = result.metrics().reads;
        assert!(reads.postgres_rows < 27000, "{reads:?}");

        let mut result = engine
            .query(
                Language::Cypher,
                "MATCH (a:Node {id:1}) UNWIND range(1,64) AS i WITH a,i%2 AS k
             CALL (a,k) {
                MATCH (a)-[:Next]->(b)
                WITH DISTINCT b ORDER BY b.id LIMIT 1
                MATCH (b)-[e:Next]->(c)
                WITH c.id + k AS v WHERE e.id = 2
                RETURN v
             } RETURN count(v) AS n,sum(v) AS s",
                options.clone(),
            )
            .await?;
        assert_eq!(rows(&mut result).await?, [json!({"n":64,"s":224})]);
        assert!(result.metrics().reads.postgres_rows < 60);

        let mut result = engine
            .query(
                Language::Cypher,
                "MATCH (a:Node {id:1}) UNWIND range(1,64) AS i WITH a,i%2 AS k
             CALL (a,k) { MATCH (a)-[:Next]->(b) RETURN DISTINCT b.id+k AS v }
             RETURN count(v) AS n,sum(v) AS s",
                options.clone(),
            )
            .await?;
        assert_eq!(rows(&mut result).await?, [json!({"n":64,"s":160})]);
        assert!(result.metrics().reads.postgres_rows < 25);

        let mut result = engine
            .query(
                Language::Cypher,
                "MATCH (a:Node {id:30000}) CALL (a) {
                    MATCH (a)-[e:Across]->(b) RETURN count(e) AS n,count(DISTINCT b.id) AS d
                } RETURN n,d",
                QueryOptions {
                    memory_bytes: 8 * 1024 * 1024,
                    batch_size: 128,
                    graph: GraphOptions::default(),
                    ..options.clone()
                },
            )
            .await?;
        assert_eq!(rows(&mut result).await?, [json!({"n":12001,"d":12000})]);
        assert!(result.metrics().reads.postgres_rows < 60000);

        let value = |k| json!({"k":k,"p":"x".repeat(2048)});
        for (query, expected) in [
            (
                "MATCH (a:Node {id:10000}) CALL (a) {
                    MATCH (a)-[:Next]->(b) RETURN b.id AS id,b.payload AS p ORDER BY id
                } RETURN id AS k,p ORDER BY k SKIP 4999 LIMIT 1",
                vec![value(15000)],
            ),
            (
                "MATCH (a:Node {id:10000}) CALL (a) {
                    MATCH (a)-[:Next]->(b) WITH b ORDER BY b.id DESC SKIP 4999 LIMIT 1
                    RETURN b.id AS k,b.payload AS p
                } RETURN k,p ORDER BY k LIMIT 1",
                vec![value(10001)],
            ),
            (
                "MATCH (a:Node {id:10000}) CALL (a) {
                    MATCH (a)-[:Next]->(b) WITH b AS n WHERE n.id > 10000
                    RETURN n.id AS k,n.payload AS p ORDER BY k
                } RETURN k,p ORDER BY k SKIP 4999 LIMIT 1",
                vec![value(15000)],
            ),
            (
                "MATCH (a:Node {id:10000}) CALL (a) {
                    MATCH (a)-[:Next]->(b) WITH DISTINCT b ORDER BY b.id SKIP 4999 LIMIT 1
                    RETURN b.id AS k,b.payload AS p
                } RETURN k,p",
                vec![value(15000)],
            ),
            (
                "MATCH (a:Node {id:10000}) CALL (a) {
                    CALL (a) {
                        MATCH (a)-[:Next]->(b) WITH b,count(b) AS c ORDER BY b.id SKIP 4999 LIMIT 1
                        RETURN b.id AS k,b.payload AS payload,c
                    } RETURN k,payload AS p,c
                } RETURN k,p,c",
                vec![json!({"k":15000,"p":"x".repeat(2048),"c":1})],
            ),
            (
                "MATCH (a:Node {id:10000}) CALL (a) {
                    MATCH (a)-[:Next]->(b) RETURN b.id AS k,b AS node ORDER BY k
                } RETURN k,node.payload AS p ORDER BY k SKIP 4999 LIMIT 1",
                vec![value(15000)],
            ),
            (
                "MATCH (a:Node {id:4096}) CALL (a) {
                    OPTIONAL MATCH (a)-[:Next]->(b) RETURN b.id AS k,b.payload AS p
                } RETURN k IS NULL AS missing,p IS NULL AS no_payload ORDER BY k LIMIT 1",
                vec![json!({"missing":true,"no_payload":true})],
            ),
            (
                "MATCH (a:Node {id:10000}) CALL (a) {
                    MATCH (a)-[:Next]->(b) WHERE b.id <= 12500 RETURN b.id AS k,b.payload AS p
                    UNION ALL
                    MATCH (a)-[:Next]->(b) WHERE b.id >= 12500 RETURN b.id AS k,b.payload AS p
                } RETURN k,p ORDER BY k SKIP 2499 LIMIT 2",
                vec![value(12500), value(12500)],
            ),
        ] {
            let mut result = engine
                .query(
                    Language::Cypher,
                    query,
                    QueryOptions {
                        memory_bytes: 8 * 1024 * 1024,
                        batch_size: 64,
                        ..options.clone()
                    },
                )
                .await
                .with_context(|| query.to_owned())?;
            assert_eq!(rows(&mut result).await?, expected, "{query}");
            let reads = result.metrics().reads;
            assert!(reads.postgres_arrow_bytes < 512 * 1024, "{query} {reads:?}");
        }

        let mut options = options;
        options.parameters.insert("id".into(), json!(text_id));
        let mut result = engine.query(
            Language::Cypher,
            "MATCH (a:Name {tenant:1,id:$id}) CALL (a) { MATCH (a)-[:Link]->(b) RETURN b.id AS id }
             RETURN id LIMIT 1",
            options,
        ).await?;
        assert_eq!(rows(&mut result).await?, [json!({"id":"target"})]);
        Ok::<(), anyhow::Error>(())
    }
    .await;
    let cleanup = client
        .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
        .await;
    drop(client);
    driver.await??;
    check?;
    cleanup?;
    Ok(())
}

#[tokio::test]
async fn parameterized_queries_and_path_properties_use_bound_values() -> Result<()> {
    let (_dir, engine) = local_fixture("key: [tenant, id]")?;
    let text = "'\" \\ /* $ignored */ 北京\n";
    let mut options = QueryOptions::default();
    options.parameters.extend([
        ("account".into(), json!({"tenant":1,"id":1})),
        ("ids".into(), json!([10, 11])),
        ("note".into(), json!(text)),
    ]);
    let mut result = engine
        .query(
            Language::Cypher,
            "MATCH p=(a:Account $account)-[:Transfer]->(b:Account) WITH p
         UNWIND relationships(p) AS e WITH e WHERE e.id IN $ids
         RETURN e.amount AS amount, $note AS note ORDER BY amount /* $ignored */",
            options,
        )
        .await?;
    assert_eq!(
        rows(&mut result).await?,
        [
            json!({"amount":10,"note":text}),
            json!({"amount":20,"note":text})
        ]
    );
    let error = engine
        .query(
            Language::Cypher,
            "RETURN $missing AS value",
            Default::default(),
        )
        .await
        .err()
        .unwrap();
    assert!(error.to_string().contains("missing Cypher parameter"));
    Ok(())
}

#[tokio::test]
async fn untyped_matches_merge_relationship_types_before_aggregation() -> Result<()> {
    let (dir, _) = local_fixture("key: [tenant, id]")?;
    let mut catalog = Catalog::load(dir.path().join("graph.yaml"))?;
    catalog
        .nodes
        .insert("Recipient".into(), catalog.nodes["Account"].clone());
    let mut copy = catalog.edges["Transfer"].clone();
    copy.to.node = "Recipient".into();
    catalog.edges.insert("External".into(), copy);
    let mut unpriced = catalog.edges["Transfer"].clone();
    unpriced.properties.retain(|property| property != "amount");
    catalog.edges.insert("Unpriced".into(), unpriced);
    let engine = Engine::new(catalog)?;
    for (query, expected) in [
        ("MATCH (a)-->(b) RETURN count(*) AS n", json!({"n":18})),
        (
            "MATCH (a:Account)-->(b:Account) RETURN count(*) AS n",
            json!({"n":12}),
        ),
        (
            "MATCH (a)-->(b), (b:Recipient) RETURN count(*) AS n",
            json!({"n":6}),
        ),
        (
            "MATCH (a)-[{amount: 10}]->(b) RETURN count(*) AS n",
            json!({"n":2}),
        ),
        (
            "MATCH (a:Recipient)-->(b:Recipient) RETURN count(*) AS n",
            json!({"n":0}),
        ),
        (
            "MATCH p=(a)-[]->(b) WITH p UNWIND relationships(p) AS e RETURN sum(e.amount) AS total",
            json!({"total":440}),
        ),
    ] {
        let mut result = engine
            .query(Language::Cypher, query, Default::default())
            .await?;
        assert_eq!(rows(&mut result).await?, [expected], "{query}");
    }
    let mut result = engine
        .query(
            Language::Cypher,
            "CALL db.labels() YIELD label RETURN label ORDER BY label",
            Default::default(),
        )
        .await?;
    assert_eq!(
        rows(&mut result).await?,
        [json!({"label":"Account"}), json!({"label":"Recipient"})]
    );
    assert_eq!(result.metrics().reads.requests, 0);
    Ok(())
}

#[tokio::test]
async fn gremlin_branches_and_until_operate_on_each_traverser() -> Result<()> {
    let (_dir, engine) = local_fixture("key: [tenant, id]")?;
    let options = QueryOptions {
        batch_size: 1,
        partitions: 3,
        ..Default::default()
    };
    for (query, expected) in [
        (
            "g.V([1,1]).hasLabel('Account').valueMap('id','status')",
            vec![json!({"id":[1],"status":["active"]})],
        ),
        (
            "g.V([1,1]).hasLabel('Account').repeat(__.out().simplePath()).until(__.has('id',3)).values('id')",
            vec![json!(3), json!(3)],
        ),
        (
            "g.V([1,3]).hasLabel('Account').until(__.has('id',3)).repeat(__.out()).values('id')",
            vec![json!(3)],
        ),
        (
            "g.V([1,1]).hasLabel('Account').union(__.out(),__.in()).values('id').order()",
            vec![json!(1), json!(1), json!(2), json!(2), json!(2)],
        ),
        (
            "g.V([1,1],[1,3]).hasLabel('Account').coalesce(__.out(),__.identity()).values('id').order()",
            vec![json!(1), json!(2), json!(2), json!(3)],
        ),
        (
            "g.V([1,1],[1,3]).hasLabel('Account').choose(__.has('id',3),__.identity(),__.out()).values('id').order()",
            vec![json!(1), json!(2), json!(2), json!(3)],
        ),
    ] {
        let mut result = engine
            .query(Language::Gremlin, query, options.clone())
            .await?;
        assert_eq!(
            rows(&mut result).await?,
            expected
                .into_iter()
                .map(|value| json!({"value":value}))
                .collect::<Vec<_>>(),
            "{query}"
        );
    }
    let cyclic = async {
        let mut result = engine.query(
            Language::Gremlin,
            "g.V().hasLabel('Account').has('id',1).limit(1).repeat(__.has('id',1)).until(__.has('id',99)).count()",
            Default::default(),
        ).await?;
        rows(&mut result).await
    }.await;
    assert!(format!("{:#}", cyclic.expect_err("unbounded repeat")).contains("256 iterations"));
    Ok(())
}

#[tokio::test]
async fn graph_queries_bound_expanded_expressions_before_sql_planning() -> Result<()> {
    let (_files, engine) = local_fixture("key: [tenant, id]")?;
    let slices = format!("RETURN [1,2,3]{} AS value", "[1..]".repeat(16));
    let subscripts = format!("RETURN [1]{} AS value", "[0]".repeat(1500));
    let math = format!(
        "g.V().limit(1).math('{}_{}')",
        "(".repeat(2000),
        ")".repeat(2000)
    );
    let filters = format!("g.V(){}.count()", ".filter(__.has('id'))".repeat(25));
    let union = format!(
        "g.V(){}.count()",
        ".union(__.identity(),__.identity())".repeat(25)
    );
    let coalesce = format!(
        "g.V(){}.count()",
        ".coalesce(__.identity(),__.identity())".repeat(20)
    );
    let choose = format!(
        "g.V(){}.count()",
        ".choose(__.has('id'),__.identity(),__.identity())".repeat(20)
    );
    let using = format!("USING {{SnapShotTime:{}1}} RETURN 1", "-".repeat(6000));
    for (language, query, expected) in [
        (Language::Cypher, slices, "generated SQL size limit"),
        (
            Language::Cypher,
            subscripts,
            "Cypher statement exceeds the supported expression complexity",
        ),
        (Language::Gremlin, math, "expression complexity"),
        (Language::Gremlin, filters, "correlated plan size limit"),
        (Language::Gremlin, union, "correlated plan size limit"),
        (Language::Gremlin, coalesce, "correlated plan size limit"),
        (Language::Gremlin, choose, "correlated plan size limit"),
        (
            Language::Cypher,
            using,
            "USING options exceeds the supported expression complexity",
        ),
    ] {
        let error = engine
            .query(language, &query, Default::default())
            .await
            .err()
            .context("oversized query reached execution")?;
        assert!(format!("{error:#}").contains(expected), "{error:#}");
    }
    Ok(())
}

#[tokio::test]
async fn flat_lists_wide_projections_and_independent_branch_filters_remain_valid() -> Result<()> {
    let (_files, engine) = local_fixture("key: [tenant, id]")?;
    let ids = (1..=800).collect::<Vec<_>>();
    let options = QueryOptions::default().with_parameters_json(&json!({"ids":ids}).to_string())?;
    let columns = (0..130)
        .map(|i| format!("a.id AS col{i}"))
        .collect::<Vec<_>>()
        .join(", ");
    let mut result = engine
        .query(
            Language::Cypher,
            &format!("MATCH (a:Account) WHERE a.id IN $ids RETURN {columns}"),
            options,
        )
        .await?;
    assert_eq!(result.batches.schema().fields().len(), 130);
    assert_eq!(rows(&mut result).await?.len(), 4);
    let list = format!("{}[17]{}", "[x IN ".repeat(40), " | x]".repeat(40));
    let mut result = engine
        .query(
            Language::Cypher,
            &format!("RETURN {list} AS xs"),
            Default::default(),
        )
        .await?;
    assert_eq!(rows(&mut result).await?, [json!({"xs":[17]})]);
    let values = (1..=800)
        .map(|id| id.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let mut result = engine
        .query(
            Language::Cypher,
            &format!("RETURN 797 IN [{values}] AS hit, -1 IN [{values}] AS miss"),
            Default::default(),
        )
        .await?;
    assert_eq!(rows(&mut result).await?, [json!({"hit":true,"miss":false})]);
    let filters = vec!["__.filter(__.has('id'))"; 12].join(", ");
    for (query, expected) in [
        (
            format!("g.V().hasLabel('Account').has('id',within({values})).count()"),
            4,
        ),
        (
            format!("g.V().hasLabel('Account').union({filters}).count()"),
            48,
        ),
    ] {
        let mut result = engine
            .query(Language::Gremlin, &query, Default::default())
            .await?;
        assert_eq!(rows(&mut result).await?, [json!({"value":expected})]);
    }
    Ok(())
}

#[tokio::test]
async fn cypher_reports_arithmetic_and_sum_overflow() -> Result<()> {
    let (_files, engine) = local_fixture("key: [tenant, id]")?;
    for query in [
        "RETURN 9223372036854775807 + 1 AS value",
        "MATCH (a:Account) RETURN a.tenant * 9223372036854775807 AS value",
        "UNWIND [9223372036854775807, 1] AS n RETURN sum(n) AS value",
        "UNWIND [9223372036854775807, 1, 1] AS n RETURN sum(DISTINCT n) AS value",
        "UNWIND [[3,9223372036854775807],[3,1],[5,7]] AS pair RETURN pair[0] AS bucket, sum(pair[1]) AS value",
    ] {
        let result = async {
            let mut result = engine
                .query(
                    Language::Cypher,
                    query,
                    QueryOptions {
                        batch_size: 1,
                        partitions: 2,
                        ..Default::default()
                    },
                )
                .await?;
            rows(&mut result).await
        }
        .await;
        let error = result.expect_err(query);
        assert!(
            format!("{error:#}").to_lowercase().contains("overflow"),
            "{query}: {error:#}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn narrow_cypher_integers_widen_and_gremlin_sum_reports_overflow() -> Result<()> {
    use graphlake::arrow::array::{ArrayRef, Int8Array, Int32Array, StringArray};
    let (files, engine) = local_fixture("key: [tenant, id]")?;
    for (tenant, operator, expected) in [
        (
            Arc::new(Int8Array::from(vec![100; 2])) as ArrayRef,
            "+",
            200_i64,
        ),
        (
            Arc::new(Int32Array::from(vec![50_000; 2])) as ArrayRef,
            "*",
            2_500_000_000,
        ),
    ] {
        let batch = RecordBatch::try_from_iter([
            ("tenant", tenant),
            ("id", Arc::new(Int64Array::from(vec![1, i64::MAX])) as _),
            (
                "status",
                Arc::new(StringArray::from(vec!["active"; 2])) as _,
            ),
        ])?;
        let path = files.path().join("replacement.parquet");
        std::fs::write(&path, super::parquet(&batch)?)?;
        std::fs::rename(path, files.path().join("accounts.parquet"))?;
        let mut result = engine
            .query(
                Language::Cypher,
                &format!("MATCH (a:Account) RETURN a.tenant {operator} a.tenant AS value"),
                Default::default(),
            )
            .await?;
        assert_eq!(rows(&mut result).await?, vec![json!({"value":expected}); 2]);
    }
    let result = async {
        let mut sum = engine
            .query(
                Language::Gremlin,
                "g.V().hasLabel('Account').values('id').sum()",
                Default::default(),
            )
            .await?;
        rows(&mut sum).await
    }
    .await;
    assert!(format!("{:#}", result.expect_err("checked Gremlin sum")).contains("overflow"));
    Ok(())
}

#[tokio::test]
async fn composed_cypher_preserves_optional_rows_and_scope() -> Result<()> {
    let (_files, engine) = local_fixture("key: [tenant, id]")?;
    let query = "MATCH (a:Account) WHERE a.tenant = 1
        OPTIONAL MATCH (a)-[t:Transfer]->(b:Account) WHERE t.amount >= 40
        WITH a, count(t) AS transfers, count(*) AS rows
        RETURN a.id AS id, transfers, rows ORDER BY id";
    let mut answer = engine
        .query(Language::Cypher, query, Default::default())
        .await?;
    assert_eq!(
        rows(&mut answer).await?,
        [
            json!({"id":1,"transfers":1,"rows":1}),
            json!({"id":2,"transfers":1,"rows":1}),
            json!({"id":3,"transfers":0,"rows":1}),
        ]
    );

    for predicate in [
        "EXISTS { MATCH (b)-[:Transfer]->(c:Account) WHERE c.id = x }",
        "COUNT { MATCH (b)-[:Transfer]->(c:Account) WHERE c.id = x } > 0",
    ] {
        let query = format!(
            "UNWIND [3, 2, 3, 7] AS x
             OPTIONAL MATCH (a:Account {{tenant:1,id:1}})-[t:Transfer]->(b:Account)
             WHERE {predicate}
             RETURN x, count(t) AS hits, count(*) AS rows ORDER BY x"
        );
        let mut answer = engine
            .query(
                Language::Cypher,
                &query,
                QueryOptions {
                    batch_size: 1,
                    partitions: 4,
                    ..Default::default()
                },
            )
            .await?;
        assert_eq!(
            rows(&mut answer).await?,
            [
                json!({"x":2,"hits":1,"rows":1}),
                json!({"x":3,"hits":4,"rows":4}),
                json!({"x":7,"hits":0,"rows":1})
            ],
            "{query}"
        );
    }

    let mut answer = engine
        .query(
            Language::Cypher,
            "UNWIND [1, 1, null] AS id
         WITH id WHERE id IS NOT NULL
         MATCH (a:Account) WHERE a.tenant = 1 AND a.id = id
         RETURN a.id AS id UNION ALL RETURN 3 AS id",
            Default::default(),
        )
        .await?;
    let mut values = rows(&mut answer).await?;
    values.sort_by_key(|row| row["id"].as_i64());
    assert_eq!(values, [json!({"id":1}), json!({"id":1}), json!({"id":3})]);

    let mut answer = engine
        .query(
            Language::Cypher,
            "MATCH (a:Account) WHERE a.tenant = 1
         CALL { WITH a MATCH (a)-[:Transfer]->(b:Account) RETURN count(b) AS n }
         RETURN a.id AS id, n ORDER BY id",
            Default::default(),
        )
        .await?;
    assert_eq!(
        rows(&mut answer).await?,
        [
            json!({"id":1,"n":3}),
            json!({"id":2,"n":2}),
            json!({"id":3,"n":0}),
        ]
    );

    let mut answer = engine
        .query(
            Language::Cypher,
            "MATCH (a:Account) WHERE a.tenant = 1
         CALL (a) {
           MATCH (a)-[:Transfer]->(b:Account)
           WITH count(b) AS n
           RETURN a.id AS id, n
         }
         CALL () { RETURN 7 AS constant }
         RETURN id, n, constant ORDER BY id",
            Default::default(),
        )
        .await?;
    assert_eq!(
        rows(&mut answer).await?,
        [
            json!({"id":1,"n":3,"constant":7}),
            json!({"id":2,"n":2,"constant":7}),
            json!({"id":3,"n":0,"constant":7}),
        ]
    );
    let mut answer = engine
        .query(
            Language::Cypher,
            "MATCH (a:Account) WHERE a.tenant = 1
         CALL (a) {
           MATCH (a)-[:Transfer]->(b:Account)
           CALL (b) {
             MATCH (b)-[:Transfer]->(c:Account)
             RETURN count(c) AS n
           }
           RETURN sum(n) AS total
         }
         RETURN a.id AS id, total ORDER BY id",
            Default::default(),
        )
        .await?;
    assert_eq!(
        rows(&mut answer).await?,
        [
            json!({"id":1,"total":7}),
            json!({"id":2,"total":3}),
            json!({"id":3,"total":0}),
        ]
    );
    let mut answer = engine
        .query(
            Language::Cypher,
            "CALL { UNWIND [1, 2, 2] AS id RETURN id UNION RETURN 2 AS id }
         WITH collect(id) AS ids
         UNWIND ids AS id RETURN id ORDER BY id",
            Default::default(),
        )
        .await?;
    assert_eq!(rows(&mut answer).await?, [json!({"id":1}), json!({"id":2})]);
    Ok(())
}

#[tokio::test]
async fn cypher_collect_uses_the_preceding_with_order() -> Result<()> {
    let (_files, engine) = local_fixture("key: [tenant, id]")?;
    for (query, expected) in [
        (
            "MATCH (a:Account) WITH a.id AS id, a.tenant AS tenant
             ORDER BY tenant ASC, id DESC
             RETURN collect(id) AS ids",
            json!({"ids":[3,2,1,2]}),
        ),
        (
            "MATCH (a:Account) WITH a.id AS id ORDER BY a.tenant DESC, a.id ASC
             RETURN collect(DISTINCT id) AS ids",
            json!({"ids":[2,1,3]}),
        ),
        (
            "UNWIND [0,1,2,3] AS i WITH [1.0,'1',1,null][i] AS x ORDER BY i
             RETURN collect(DISTINCT x) AS xs",
            json!({"xs":[{"Float64":1.0},{"Utf8View":"1"}]}),
        ),
        (
            "MATCH (a:Account) WITH DISTINCT a.id AS id ORDER BY a.id DESC
             RETURN collect(id) AS ids",
            json!({"ids":[3,2,1]}),
        ),
        (
            "MATCH (a:Account) WITH a.id AS id ORDER BY id DESC RETURN * LIMIT 1",
            json!({"id":3}),
        ),
        (
            "MATCH (a:Account) WITH a AS account ORDER BY account.id DESC LIMIT 2
             RETURN collect(account.id) AS ids",
            json!({"ids":[3,2]}),
        ),
    ] {
        let mut result = engine
            .query(
                Language::Cypher,
                query,
                QueryOptions {
                    batch_size: 1,
                    ..Default::default()
                },
            )
            .await?;
        assert_eq!(rows(&mut result).await?, [expected], "{query}");
    }
    Ok(())
}

#[tokio::test]
async fn cypher_collect_distinct_merges_ordered_spills_within_memory_budget() -> Result<()> {
    let (_files, engine) = local_fixture("key: [tenant, id]")?;
    let mut result = engine
        .query(
            Language::Cypher,
            "UNWIND range(0,131071) AS i WITH i % 65536 AS x ORDER BY i DESC
         RETURN collect(DISTINCT x) AS xs",
            QueryOptions {
                memory_bytes: 8 * 1024 * 1024,
                spill_bytes: 128 * 1024 * 1024,
                batch_size: 128,
                ..Default::default()
            },
        )
        .await?;
    let values = (0..65536).rev().collect::<Vec<_>>();
    assert_eq!(rows(&mut result).await?, [json!({"xs":values})]);
    assert!(result.metrics().spilled_bytes > 0);
    Ok(())
}

#[tokio::test]
async fn cypher_undirected_and_shortest_paths_keep_parallel_edge_identity() -> Result<()> {
    let (_files, engine) = local_fixture("key: [tenant, id]")?;
    for statement in [
        "MATCH (a:Account)-[:Transfer]-(b:Account) RETURN a.id, b.id",
        "MATCH (a:Account), (b:Account) WHERE a.id = b.tenant RETURN a.id, b.id",
    ] {
        let mut result = engine
            .query(Language::Cypher, statement, Default::default())
            .await?;
        assert!(
            result.physical_plan.contains("HashJoinExec"),
            "{}",
            result.physical_plan
        );
        assert!(
            !result.physical_plan.contains("NestedLoopJoinExec"),
            "{}",
            result.physical_plan
        );
        assert!(!rows(&mut result).await?.is_empty());
    }
    let mut answer = engine
        .query(
            Language::Cypher,
            "MATCH (a:Account {tenant:1,id:1})-[t:Transfer]-(b:Account)
         RETURN b.id AS id, count(t) AS n ORDER BY id",
            Default::default(),
        )
        .await?;
    assert_eq!(
        rows(&mut answer).await?,
        [json!({"id":1,"n":1}), json!({"id":2,"n":3})]
    );

    let mut answer = engine.query(Language::Cypher,
        "MATCH p=allShortestPaths((a:Account {tenant:1,id:1})-[:Transfer*]->(b:Account {tenant:1,id:3}))
         RETURN length(p) AS hops, relationships(p) AS edges, nodes(p) AS nodes", Default::default()).await?;
    let mut paths = rows(&mut answer).await?;
    paths.sort_by_key(|row| row["edges"][0]["id"].as_i64());
    assert_eq!(
        paths,
        [
            json!({"hops":2,"edges":[{"tenant":1,"id":10},{"tenant":1,"id":13}],"nodes":[{"tenant":1,"id":1},{"tenant":1,"id":2},{"tenant":1,"id":3}]}),
            json!({"hops":2,"edges":[{"tenant":1,"id":11},{"tenant":1,"id":13}],"nodes":[{"tenant":1,"id":1},{"tenant":1,"id":2},{"tenant":1,"id":3}]}),
        ]
    );

    let mut answer = engine.query(Language::Cypher,
        "MATCH p=shortestPath((a:Account {tenant:1,id:3})-[:Transfer*1..3]-(b:Account {tenant:1,id:1}))
         RETURN length(p) AS hops", Default::default()).await?;
    assert_eq!(rows(&mut answer).await?, [json!({"hops":2})]);
    let mut answer = engine.query(Language::Cypher,
        "MATCH p=allShortestPaths((a:Account {tenant:1,id:1})-[:Transfer*1..2]->(b:Account {tenant:1,id:2}))
         WHERE length(p)=2 RETURN count(p) AS paths", Default::default()).await?;
    assert_eq!(rows(&mut answer).await?, [json!({"paths":2})]);
    let mut answer = engine.query(Language::Cypher,
        "MATCH p=allShortestPaths((a:Account {tenant:1,id:1})-[:Transfer*2..3]->(b:Account {tenant:1,id:2}))
         RETURN length(p) AS hops, count(*) AS paths", Default::default()).await?;
    assert_eq!(rows(&mut answer).await?, [json!({"hops":2,"paths":2})]);
    Ok(())
}

#[tokio::test]
async fn mapped_history_and_partition_use_the_same_graph_for_query_and_cache() -> Result<()> {
    mapped_history(None).await
}

#[tokio::test]
#[ignore = "requires a writable PostgreSQL fixture"]
async fn mapped_postgres_and_parquet_share_the_same_scoped_graph() -> Result<()> {
    let dsn = std::env::var("GRAPHLAKE_TEST_POSTGRES")?;
    let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls).await?;
    let driver = tokio::spawn(connection);
    let schema = format!("graphlake_model_{}", std::process::id());
    client
        .batch_execute(&format!(
            "CREATE SCHEMA {schema};
         CREATE TABLE {schema}.current_accounts (
           tenant_id int NOT NULL, account_id bigint NOT NULL, display_name text,
           effective date, expires date, deleted boolean
         );
         INSERT INTO {schema}.current_accounts VALUES
         (1,1,'current','2026-01-01',NULL,false),
         (1,2,'second',DATE '1970-01-01'+20000,NULL,false),
         (1,4,'isolated',DATE '1970-01-01'+20000,NULL,false),
         (1,9,'deleted',DATE '1970-01-01'+20000,NULL,true),
         (2,1,'other tenant',DATE '1970-01-01'+20000,NULL,false)"
        ))
        .await?;
    let result = mapped_history(Some(&schema)).await;
    client
        .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
        .await?;
    drop(client);
    driver.await??;
    result
}

async fn mapped_history(pg_schema: Option<&str>) -> Result<()> {
    let dir = tempfile::tempdir()?;
    let write = |file,
                 tenants: Vec<i32>,
                 ids: Vec<i64>,
                 names: Vec<&str>,
                 from: Vec<i32>,
                 to,
                 deleted|
     -> Result<()> {
        let batch = RecordBatch::try_from_iter([
            ("tenant_id", Arc::new(Int32Array::from(tenants)) as _),
            ("account_id", Arc::new(Int64Array::from(ids)) as _),
            ("display_name", Arc::new(StringArray::from(names)) as _),
            ("effective", Arc::new(Date32Array::from(from)) as _),
            ("expires", Arc::new(Date32Array::from(to)) as _),
            ("deleted", Arc::new(BooleanArray::from(deleted)) as _),
        ])?;
        std::fs::write(dir.path().join(file), parquet(&batch)?)?;
        Ok(())
    };
    write(
        "current.parquet",
        vec![1, 1, 1, 1, 2],
        vec![1, 2, 4, 9, 1],
        vec!["current", "second", "isolated", "deleted", "other tenant"],
        vec![20454, 20000, 20000, 20000, 20000],
        vec![None; 5],
        vec![false, false, false, true, false],
    )?;
    write(
        "history.parquet",
        vec![1, 1],
        vec![1, 2],
        vec!["historical", "second"],
        vec![20000, 20000],
        vec![Some(20454), None],
        vec![false, false],
    )?;
    write(
        "conflict.parquet",
        vec![1],
        vec![2],
        vec!["conflict"],
        vec![20000],
        vec![None],
        vec![false],
    )?;
    std::fs::write(
        dir.path().join("transfers.parquet"),
        parquet(&transfers()?)?,
    )?;

    let mapping = r#"graph: mapped
sources:
  lake:
    type: parquet
    tables:
      current: [current.parquet]
      history: [history.parquet]
      conflict: [conflict.parquet]
      transfers: [transfers.parquet]
nodes:
  Account:
    key: [tenant, id]
    properties: [name]
    scd2: {valid_from: start, valid_to: end}
    inputs:
      - table: lake.current
        columns: {tenant: tenant_id, id: account_id, name: display_name, start: effective, end: expires}
        where: [{column: deleted, op: eq, value: false}]
      - table: lake.history
        columns: {tenant: tenant_id, id: account_id, name: display_name, start: effective, end: expires}
edges:
  Transfer:
    source: lake.transfers
    key: [tenant, id]
    properties: [amount]
    from: {node: Account, columns: [tenant, src]}
    to: {node: Account, columns: [tenant, dst]}
"#;
    let mapping = if let Some(schema) = pg_schema {
        mapping
            .replace(
                "sources:\n",
                "sources:\n  pg: {type: postgres, dsn_env: GRAPHLAKE_TEST_POSTGRES}\n",
            )
            .replace(
                "- table: lake.current",
                &format!("- table: pg.{schema}.current_accounts"),
            )
    } else {
        mapping.to_owned()
    };
    let config = dir.path().join("graph.yaml");
    std::fs::write(&config, &mapping)?;
    let engine =
        Engine::new(Catalog::load(&config)?)?.with_cache(dir.path().join("csr"), "tests")?;
    let mut answer = engine.query(Language::Cypher,
        "USING {logicalPartition: {tenant:1}, SnapShotTime:'2025-06-01'}
         MATCH (a:Account) OPTIONAL MATCH (a)-[t:Transfer]->(b:Account)
         WHERE t.amount >= 20 WITH a, count(t) AS n RETURN a.id AS id, a.name AS name, n ORDER BY id",
        Default::default()).await?;
    assert_eq!(
        rows(&mut answer).await?,
        [
            json!({"id":1,"name":"historical","n":2}),
            json!({"id":2,"name":"second","n":1}),
            json!({"id":4,"name":"isolated","n":0}),
        ]
    );
    let options = QueryOptions {
        graph: GraphOptions {
            logical_partition: vec![Predicate {
                column: "tenant".into(),
                op: Comparison::Eq,
                value: json!(1),
            }],
            snapshot_time: Some("2026-01-01".into()),
            ..Default::default()
        },
        ..Default::default()
    };
    let mut answer = engine
        .query(
            Language::Sql,
            r#"SELECT name FROM "Account" WHERE id = 1"#,
            options.clone(),
        )
        .await?;
    assert_eq!(rows(&mut answer).await?, [json!({"name":"current"})]);
    let refresh = engine
        .refresh("Transfer", options.clone())
        .await
        .context("refresh scoped graph")?;
    assert_eq!((refresh.vertices, refresh.edges), (3, 4));
    let mut projected = engine
        .analyze_projection(
            GraphProjection {
                edges: vec!["Transfer".into()],
                weight: Some("amount".into()),
                ..Default::default()
            },
            ProjectionAlgorithm::WeaklyConnectedComponents,
            options.clone(),
        )
        .await?;
    assert_eq!(rows(&mut projected).await?.len(), refresh.vertices);
    assert_eq!(projected.metrics().algorithm.unwrap().edges, refresh.edges);
    let request = Traversal {
        edge: "Transfer".into(),
        direction: Direction::Outgoing,
        start: vec![ScalarValue::Int32(Some(1)), ScalarValue::Int64(Some(1))],
        max_hops: 2,
        kind: TraversalKind::Reachable,
    };
    let mut live = engine
        .traverse_with_strategy(request.clone(), options.clone(), TraversalStrategy::Scan)
        .await?;
    let mut cached = engine
        .traverse_with_strategy(request.clone(), options.clone(), TraversalStrategy::Cached)
        .await?;
    assert_eq!(rows(&mut live).await?, rows(&mut cached).await?);
    let mut different = options;
    different.graph.snapshot_time = Some("2025-06-01".into());
    assert!(
        engine
            .traverse_with_strategy(request, different, TraversalStrategy::Cached)
            .await
            .is_err()
    );

    std::fs::write(
        &config,
        mapping.replace("- table: lake.history", "- table: lake.conflict"),
    )?;
    let conflict = Engine::new(Catalog::load(&config)?)?;
    let error = conflict
        .query(
            Language::Sql,
            r#"SELECT name FROM "Account""#,
            Default::default(),
        )
        .await
        .err()
        .expect("conflicting key");
    assert!(format!("{error:#}").contains("conflicting records for Account"));
    let preferred = mapping
        .replace("- table: lake.history", "- table: lake.conflict")
        .replace(
            "  Account:\n",
            "  Account:\n    on_conflict: prefer_first\n",
        );
    std::fs::write(&config, preferred)?;
    let engine = Engine::new(Catalog::load(&config)?)?;
    let mut answer = engine
        .query(
            Language::Sql,
            r#"SELECT name FROM "Account" WHERE tenant = 1 AND id = 2"#,
            Default::default(),
        )
        .await?;
    assert_eq!(rows(&mut answer).await?, [json!({"name":"second"})]);
    Ok(())
}

#[tokio::test]
async fn gremlin_traverses_and_keeps_step_order() -> Result<()> {
    let (_files, engine) = local_fixture("key: [tenant, id]")?;
    for (query, expected) in [
        (
            "g.V().hasLabel('Account').order().by('id',desc).limit(1).values('id')",
            vec![3],
        ),
        (
            "g.V().hasLabel('Account').order().by('id',desc).has('tenant',1).values('id').limit(2)",
            vec![3, 2],
        ),
        (
            "g.E().hasLabel('Transfer').order().by('amount',desc).inV().values('id').dedup().limit(3)",
            vec![2, 1, 3],
        ),
        (
            "g.V().hasLabel('Account').order().by('id',desc).order().by('tenant',asc).values('id')",
            vec![3, 2, 1, 2],
        ),
    ] {
        let mut result = engine
            .query(
                Language::Gremlin,
                query,
                QueryOptions {
                    batch_size: 1,
                    ..Default::default()
                },
            )
            .await?;
        assert_eq!(
            rows(&mut result).await?,
            expected
                .into_iter()
                .map(|value| json!({"value":value}))
                .collect::<Vec<_>>(),
            "{query}"
        );
    }
    let mut answer = engine.query(Language::Gremlin,
        "g.V([1,1]).hasLabel('Account').repeat(__.out('Transfer')).times(2).dedup().values('id').order()",
        Default::default()).await?;
    assert_eq!(
        rows(&mut answer).await?,
        [json!({"value":1}), json!({"value":2}), json!({"value":3})]
    );
    let mut answer = engine.query(Language::Gremlin,
        "g.V([1,1]).hasLabel('Account').as('start').outE('Transfer').has('amount',gt(10)).inV().select('start').count()",
        Default::default()).await?;
    assert_eq!(rows(&mut answer).await?, [json!({"value":2})]);
    let mut answer = engine
        .query(
            Language::Gremlin,
            "g.V([1,1]).hasLabel('Account').outE('Transfer').has('id',10).inV().path()",
            Default::default(),
        )
        .await?;
    let path = rows(&mut answer).await?;
    assert_eq!(
        path[0]["value"][1],
        json!({"label":"Transfer","kind":"edge","key":["1","10"]})
    );
    for (query, expected) in [
        ("g.V([1,1]).hasLabel('Account').both().count()", 5),
        (
            "g.V([1,1]).hasLabel('Account').both().simplePath().count()",
            3,
        ),
        (
            "g.V([1,1]).has('Account','id',1).repeat(__.out()).times(0).count()",
            3,
        ),
    ] {
        let mut answer = engine
            .query(Language::Gremlin, query, Default::default())
            .await?;
        assert_eq!(
            rows(&mut answer).await?,
            [json!({"value":expected})],
            "{query}"
        );
    }
    let mut answer = engine.query(Language::Gremlin,
        "g.V([1,1]).hasLabel('Account').as('start').out().has('id',2).select('start').path().limit(1)",
        Default::default()).await?;
    let selected = rows(&mut answer).await?;
    assert_eq!(selected[0]["value"].as_array().unwrap().len(), 3);
    assert_eq!(selected[0]["value"][0], selected[0]["value"][2]);
    let mut answer = engine
        .query(
            Language::Gremlin,
            "g.V([1,1]).hasLabel('Account').out().has('id',2).values('id').path().limit(1)",
            Default::default(),
        )
        .await?;
    let values = rows(&mut answer).await?;
    assert_eq!(
        values[0]["value"][2],
        json!({"label":"","kind":"value","key":["2"]})
    );
    Ok(())
}
