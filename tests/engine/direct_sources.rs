use super::rows;
use anyhow::{Result, ensure};
use futures::TryStreamExt;
use graphlake::{
    Catalog, Engine, Language, QueryOptions,
    arrow::array::{Array, Decimal128Array, Int64Array},
};

#[tokio::test]
async fn duckdb_reads_exact_values_from_a_read_only_file_and_keeps_materialized_results()
-> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("source.duckdb");
    let connection = duckdb::Connection::open(&path)?;
    connection.execute_batch("CREATE TABLE accounts (id BIGINT, amount DECIMAL(18,2), name VARCHAR); INSERT INTO accounts VALUES (9007199254740993, 15.25, 'before'), (9007199254740994, NULL, 'other')")?;
    connection.execute_batch(
        "INSERT INTO accounts SELECT i, 0.01, 'unselected' FROM range(4096) AS ids(i)",
    )?;
    drop(connection);
    let model = dir.path().join("graph.yaml");
    std::fs::write(
        &model,
        "graph: duckdb\nsources:\n  db: {type: duckdb, path: source.duckdb}\nnodes:\n  Account: {source: db.main.accounts, key: [id], properties: [amount, name]}\n",
    )?;
    let engine = Engine::new(Catalog::load(&model)?)?;
    let mut result = engine
        .query(
            Language::Sql,
            "SELECT id, amount FROM \"Account\" WHERE id >= 9007199254740993 ORDER BY id",
            QueryOptions {
                batch_size: 1,
                ..Default::default()
            },
        )
        .await?;
    let mut batches = Vec::new();
    while let Some(batch) = result.batches.try_next().await? {
        batches.push(batch);
    }
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
    assert_eq!(result.metrics().reads.warehouse_rows, 2);
    let mut control = engine.query(
        Language::Sql,
        "WITH a AS (SELECT * FROM \"Account\") SELECT count(*) AS hits FROM a WHERE id >= 9007199254740993",
        Default::default(),
    ).await?;
    assert_eq!(
        rows(&mut control).await?,
        vec![serde_json::json!({"hits": 2})]
    );
    assert_eq!(control.metrics().reads.warehouse_rows, 4098);
    assert_eq!(
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        9007199254740993
    );
    assert_eq!(
        batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap()
            .value(0),
        1525
    );
    assert_eq!(
        batches
            .iter()
            .map(|b| b.column(1).null_count())
            .sum::<usize>(),
        1
    );
    std::fs::copy(&path, dir.path().join("other.duckdb"))?;
    let multi = std::fs::read_to_string(&model)?.replace(
        "nodes:\n",
        "  other: {type: duckdb, path: other.duckdb}\nnodes:\n",
    ) + "  Mirror: {source: other.main.accounts, key: [id], properties: [amount]}\n";
    std::fs::write(&model, multi)?;
    let multi = Engine::new(Catalog::load(&model)?)?;
    let mut result = multi
        .query(
            Language::Cypher,
            "MATCH (a:Account {id:9007199254740993}), (b:Mirror {id:9007199254740993})
         RETURN a.id AS left, b.id AS right",
            Default::default(),
        )
        .await?;
    assert_eq!(
        rows(&mut result).await?,
        [serde_json::json!({
            "left": 9007199254740993i64, "right": 9007199254740993i64
        })]
    );

    let mut old = engine
        .query(
            Language::Sql,
            "SELECT name FROM \"Account\" WHERE id=9007199254740993",
            Default::default(),
        )
        .await?;
    let connection = duckdb::Connection::open(&path)?;
    connection.execute(
        "UPDATE accounts SET name='after' WHERE id=9007199254740993",
        [],
    )?;
    drop(connection);
    assert_eq!(
        rows(&mut old).await?,
        vec![serde_json::json!({"name":"before"})]
    );
    let mut next = engine
        .query(
            Language::Sql,
            "SELECT name FROM \"Account\" WHERE id=9007199254740993",
            Default::default(),
        )
        .await?;
    assert_eq!(
        rows(&mut next).await?,
        vec![serde_json::json!({"name":"after"})]
    );
    std::fs::write(
        &model,
        "graph: missing\nsources:\n  db: {type: duckdb, path: missing.duckdb}\nnodes:\n  Account: {source: db.main.accounts, key: [id]}\n",
    )?;
    let missing = Engine::new(Catalog::load(&model)?)?;
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
    assert!(!dir.path().join("missing.duckdb").exists());
    Ok(())
}

#[tokio::test]
#[ignore = "requires a disposable MongoDB server in GRAPHLAKE_TEST_MONGODB_DSN"]
async fn mongodb_native_reads_nested_fields_exact_keys_and_detects_type_drift() -> Result<()> {
    use mongodb::{
        Client,
        bson::{Document, doc},
    };
    let client = Client::with_uri_str(std::env::var("GRAPHLAKE_TEST_MONGODB_DSN")?).await?;
    let name = format!("graphlake_{}", uuid::Uuid::new_v4().simple());
    let database = client.database(&name);
    let result = async {
        let payload = "x".repeat(6 * 1024 * 1024);
        database.collection::<Document>("accounts").insert_many([
            doc! { "_id": 9007199254740993i64, "profile": {"name":"alice", "payload":payload}, "tags":["a","b"] },
            doc! { "_id": 9007199254740994i64, "profile": {"name":"bob"}, "tags":[] },
        ]).await?;
        let catalog: Catalog = serde_saphyr::from_str(&format!("graph: mongo\nsources:\n  db: {{type: mongodb_native, dsn_env: GRAPHLAKE_TEST_MONGODB_DSN}}\nnodes:\n  Account: {{source: db.{name}.accounts, key: [_id], properties: [profile.name, profile.payload, tags]}}\n"))?;
        let engine = Engine::new(catalog)?;
        let mut result = engine.query(Language::Sql, "SELECT _id, \"profile.name\", tags FROM \"Account\" ORDER BY _id", QueryOptions { batch_size: 1, memory_bytes: 16 * 1024 * 1024, partitions: 1, ..Default::default() }).await?;
        assert_eq!(rows(&mut result).await?, vec![serde_json::json!({"_id":9007199254740993i64,"profile.name":"alice","tags":"[\"a\",\"b\"]"}), serde_json::json!({"_id":9007199254740994i64,"profile.name":"bob","tags":"[]"})]);
        database.collection::<Document>("drift").insert_many([doc!{"id":1i64,"value":1i64}, doc!{"id":2i64,"value":"changed"}]).await?;
        let catalog: Catalog = serde_saphyr::from_str(&format!("graph: drift\nsources:\n  db: {{type: mongodb_native, dsn_env: GRAPHLAKE_TEST_MONGODB_DSN, schema_sample_size: 1}}\nnodes:\n  Item: {{source: db.{name}.drift, key: [id], properties: [value]}}\n"))?;
        let drift = Engine::new(catalog)?;
        let error = drift.query(Language::Sql, "SELECT * FROM \"Item\"", Default::default()).await.err().expect("type drift must fail");
        ensure!(format!("{error:#}").contains("type changed"), "{error:#}");
        Ok::<(), anyhow::Error>(())
    }.await;
    database.drop().await?;
    result
}

#[tokio::test]
#[ignore = "requires a disposable MySQL-compatible server and GRAPHLAKE_TEST_MYSQL_COMPAT_KIND/DSN"]
async fn mysql_compatible_sources_read_real_types_and_statement_snapshots() -> Result<()> {
    use mysql_async::{Conn, Opts, prelude::Queryable};
    let kind = std::env::var("GRAPHLAKE_TEST_MYSQL_COMPAT_KIND")?;
    ensure!(
        ["doris", "starrocks", "tidb", "oceanbase", "mariadb"].contains(&kind.as_str()),
        "unsupported test dialect"
    );
    let mut connection = Conn::new(Opts::from_url(&std::env::var(
        "GRAPHLAKE_TEST_MYSQL_COMPAT_DSN",
    )?)?)
    .await?;
    let name = format!("graphlake_{}", uuid::Uuid::new_v4().simple());
    connection
        .query_drop(format!("CREATE DATABASE {name}"))
        .await?;
    let result = async {
        let storage = if matches!(kind.as_str(), "doris" | "starrocks") { " DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num'='1')" } else { " ENGINE=InnoDB" };
        connection.query_drop(format!("CREATE TABLE {name}.accounts (id BIGINT NOT NULL, amount DECIMAL(18,2), name VARCHAR(64)){storage}")).await?;
        connection.query_drop(format!("INSERT INTO {name}.accounts VALUES (9007199254740993, 15.25, 'one'), (9007199254740994, NULL, 'two')")).await?;
        let catalog: Catalog = serde_saphyr::from_str(&format!("graph: compatible\nsources:\n  db: {{type: {kind}, dsn_env: GRAPHLAKE_TEST_MYSQL_COMPAT_DSN, tls: false}}\nnodes:\n  Account: {{source: db.{name}.accounts, key: [id], properties: [amount, name]}}\n"))?;
        let engine = Engine::new(catalog)?;
        let mut result = engine.query(Language::Sql, "SELECT id, amount, name FROM \"Account\" ORDER BY id", QueryOptions { batch_size: 1, ..Default::default() }).await?;
        let batch = result.batches.try_next().await?.expect("first row");
        assert_eq!(batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap().value(0), 9007199254740993);
        assert_eq!(batch.column(1).as_any().downcast_ref::<Decimal128Array>().unwrap().value(0), 1525);
        let mut nulls = batch.column(1).null_count();
        while let Some(batch) = result.batches.try_next().await? { nulls += batch.column(1).null_count(); }
        assert_eq!(nulls, 1);
        assert_eq!(result.versions.len(), 1);
        Ok::<(), anyhow::Error>(())
    }.await;
    connection
        .query_drop(format!("DROP DATABASE {name}"))
        .await?;
    connection.disconnect().await?;
    result
}

#[tokio::test]
#[ignore = "requires a real SQL Server or Oracle fixture catalog in GRAPHLAKE_TEST_DIRECT_SQL_CATALOG"]
async fn native_sql_sources_preserve_integer_decimal_timestamp_and_null_values() -> Result<()> {
    use graphlake::arrow::array::TimestampMicrosecondArray;
    let catalog = Catalog::load(std::env::var("GRAPHLAKE_TEST_DIRECT_SQL_CATALOG")?)?;
    let engine = Engine::new(catalog)?;
    let mut result = engine
        .query(
            Language::Sql,
            "SELECT \"ID\", \"AMOUNT\", \"CREATED\" FROM \"Record\" ORDER BY \"ID\"",
            QueryOptions {
                batch_size: 1,
                ..Default::default()
            },
        )
        .await?;
    let batch = result.batches.try_next().await?.expect("first fixture row");
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        9007199254740993
    );
    assert_eq!(
        batch
            .column(1)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap()
            .value(0),
        1525
    );
    assert_eq!(
        batch
            .column(2)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap()
            .value(0),
        1794301200123456
    );
    let mut count = batch.num_rows();
    let mut nulls = batch.column(1).null_count();
    while let Some(batch) = result.batches.try_next().await? {
        count += batch.num_rows();
        nulls += batch.column(1).null_count();
    }
    assert_eq!(count, 2);
    assert_eq!(nulls, 1);
    let mut point = engine
        .query(
            Language::Sql,
            r#"SELECT "ID" FROM "Record" WHERE "ID" = 9007199254740994"#,
            Default::default(),
        )
        .await?;
    assert_eq!(
        rows(&mut point).await?,
        vec![serde_json::json!({"ID": 9007199254740994i64})]
    );
    assert_eq!(point.metrics().reads.warehouse_rows, 1);
    Ok(())
}

#[tokio::test]
async fn duckdb_sql_reads_only_used_columns_and_pushes_proven_top_and_aggregates() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let connection = duckdb::Connection::open(dir.path().join("source.duckdb"))?;
    connection.execute_batch(
        "CREATE TABLE accounts (id BIGINT PRIMARY KEY, amount BIGINT, payload VARCHAR);
         INSERT INTO accounts SELECT i, CASE WHEN i%2=0 THEN NULL ELSE -i END, repeat('x', 1024)
           FROM range(4096) AS ids(i);
         CREATE TABLE duplicates AS SELECT 1::BIGINT AS id UNION ALL SELECT 9 UNION ALL SELECT 9;
         CREATE TABLE empty_accounts (id BIGINT PRIMARY KEY, amount BIGINT);
         CREATE SCHEMA other;
         CREATE TABLE other.accounts (tenant BIGINT, id BIGINT, PRIMARY KEY (tenant, id));
         INSERT INTO other.accounts VALUES (1, 1), (1, 2), (2, 2)",
    )?;
    drop(connection);
    let model = dir.path().join("graph.yaml");
    std::fs::write(
        &model,
        "graph: duckdb\nsources:\n  db: {type: duckdb, path: source.duckdb}\n
nodes:
  Account: {source: db.main.accounts, key: [id], properties: [amount, payload]}
  Duplicate: {source: db.main.duplicates, key: [id]}
  Empty: {source: db.main.empty_accounts, key: [id], properties: [amount]}
  Composite: {source: db.other.accounts, key: [tenant, id]}
  Partial: {source: db.other.accounts, key: [id]}
",
    )?;
    let engine = Engine::new(Catalog::load(&model)?)?;
    let read = |sql: &'static str| engine.query(Language::Sql, sql, Default::default());
    let mut narrow = read(r#"SELECT id FROM "Account" WHERE id < 8 ORDER BY id"#).await?;
    let mut wide = read(r#"SELECT * FROM "Account" WHERE id < 8 ORDER BY id"#).await?;
    let ids = rows(&mut narrow).await?;
    assert_eq!(
        ids,
        rows(&mut wide)
            .await?
            .into_iter()
            .map(|row| serde_json::json!({"id":row["id"]}))
            .collect::<Vec<_>>()
    );
    assert_eq!(narrow.metrics().reads.warehouse_rows, 8);
    assert!(
        narrow.metrics().reads.warehouse_arrow_bytes * 10
            < wide.metrics().reads.warehouse_arrow_bytes
    );

    let mut top = engine
        .query(
            Language::Sql,
            r#"SELECT id, amount FROM "Account" ORDER BY id DESC LIMIT 4"#,
            QueryOptions {
                memory_bytes: 32 << 20,
                spill_bytes: 64,
                ..Default::default()
            },
        )
        .await?;
    let mut control = read(
        r#"WITH a AS (SELECT * FROM "Account") SELECT id, amount FROM a ORDER BY id DESC LIMIT 4"#,
    )
    .await?;
    assert_eq!(rows(&mut top).await?, rows(&mut control).await?);
    assert_eq!(top.metrics().reads.warehouse_rows, 4);
    assert_eq!(top.metrics().reads.snapshot_spilled_bytes, 0);
    assert_eq!(control.metrics().reads.warehouse_rows, 4096);
    let mut wide_snapshot = engine
        .query(
            Language::Sql,
            r#"SELECT SUM(LENGTH(payload)) AS bytes FROM "Account""#,
            QueryOptions {
                memory_bytes: 32 << 20,
                spill_bytes: 16 << 20,
                ..Default::default()
            },
        )
        .await?;
    assert_eq!(
        rows(&mut wide_snapshot).await?,
        [serde_json::json!({"bytes": 4194304})]
    );
    assert!(wide_snapshot.metrics().reads.snapshot_spilled_bytes > 0);
    let mut filtered_top = read(
        r#"SELECT id, amount FROM "Account" WHERE amount IS NOT NULL AND id >= 4000 ORDER BY id DESC LIMIT 4"#,
    )
    .await?;
    let mut filtered_control = read(
        r#"WITH a AS (SELECT * FROM "Account") SELECT id, amount FROM a WHERE amount IS NOT NULL AND id >= 4000 ORDER BY id DESC LIMIT 4"#,
    )
    .await?;
    assert_eq!(
        rows(&mut filtered_top).await?,
        rows(&mut filtered_control).await?
    );
    assert_eq!(filtered_top.metrics().reads.warehouse_rows, 4);

    let mut composite =
        read(r#"SELECT tenant, id FROM "Composite" ORDER BY id DESC, tenant LIMIT 1"#).await?;
    assert_eq!(
        rows(&mut composite).await?,
        [serde_json::json!({"tenant": 1, "id": 2})]
    );
    assert_eq!(composite.metrics().reads.warehouse_rows, 1);

    for (sql, expected) in [
        (
            r#"SELECT COUNT(*) AS answer FROM "Account""#,
            serde_json::json!(4096),
        ),
        (
            r#"SELECT COUNT(amount) AS answer FROM "Account""#,
            serde_json::json!(2048),
        ),
        (
            r#"SELECT COUNT(*) AS answer FROM "Account" WHERE amount IS NULL"#,
            serde_json::json!(2048),
        ),
        (
            r#"SELECT MIN(amount) AS answer FROM "Account""#,
            serde_json::json!(-4095),
        ),
        (
            r#"SELECT MAX(amount) AS answer FROM "Account""#,
            serde_json::json!(-1),
        ),
        (
            r#"SELECT MIN(amount) AS answer FROM "Empty""#,
            serde_json::Value::Null,
        ),
        (
            r#"SELECT COUNT(*) AS answer FROM "Empty""#,
            serde_json::json!(0),
        ),
    ] {
        let mut result = read(sql).await?;
        let expected = if expected.is_null() {
            // The JSON row helper omits null fields.
            serde_json::json!({})
        } else {
            serde_json::json!({"answer": expected})
        };
        assert_eq!(rows(&mut result).await?, vec![expected]);
        assert_eq!(result.metrics().reads.warehouse_rows, 1);
    }
    let mut ordered_alias =
        read(r#"SELECT amount AS id FROM "Account" ORDER BY id LIMIT 1"#).await?;
    assert_eq!(
        rows(&mut ordered_alias).await?,
        vec![serde_json::json!({"id": -4095})]
    );
    assert_eq!(ordered_alias.metrics().reads.warehouse_rows, 4096);
    for sql in [
        r#"SELECT id FROM "Duplicate" ORDER BY id LIMIT 1"#,
        r#"SELECT COUNT(*) AS n FROM "Duplicate""#,
        r#"SELECT id FROM "Partial" ORDER BY id LIMIT 1"#,
    ] {
        let result = read(sql).await;
        let error = match result {
            Ok(mut result) => rows(&mut result).await.unwrap_err(),
            Err(error) => error,
        };
        ensure!(
            format!("{error:#}").contains("duplicate business key"),
            "{error:#}"
        );
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires a disposable MySQL 8.4 server in GRAPHLAKE_TEST_MYSQL"]
async fn mysql_single_scan_is_lazy_and_releases_the_read_connection() -> Result<()> {
    use mysql_async::{Conn, Opts, prelude::Queryable};
    let opts = Opts::from_url(&std::env::var("GRAPHLAKE_TEST_MYSQL")?)?;
    let mut server = Conn::new(opts).await?;
    let schema = format!("graphlake_{}", uuid::Uuid::new_v4().simple());
    server
        .query_drop(format!("CREATE DATABASE {schema} DEFAULT CHARSET=utf8mb4"))
        .await?;
    let test = async {
        server.query_drop(format!("CREATE TABLE {schema}.items (id BIGINT PRIMARY KEY, payload VARCHAR(2048)) ENGINE=InnoDB")).await?;
        server.query_drop("SET cte_max_recursion_depth=30000").await?;
        server.query_drop(format!("INSERT INTO {schema}.items WITH RECURSIVE n(id) AS (SELECT 1 UNION ALL SELECT id+1 FROM n WHERE id<20000) SELECT id, REPEAT('x',2048) FROM n")).await?;
        let model: Catalog = serde_saphyr::from_str(&format!("graph: stream\nsources:\n  mysql: {{type: mysql, dsn_env: GRAPHLAKE_TEST_MYSQL, tls: false}}\nnodes:\n  Item: {{source: mysql.{schema}.items, key: [id], properties: [payload]}}\n"))?;
        let engine = Engine::new(model)?;
        let options = QueryOptions { batch_size: 32, ..Default::default() };
        let cancel = options.cancellation.clone();
        let mut query = engine.query(Language::Sql, r#"SELECT id, payload FROM "Item""#, options).await?;
        assert_eq!(query.metrics().reads.mysql_rows, 0);
        let batch = query.batches.try_next().await?.expect("first MySQL batch");
        assert_eq!(batch.num_rows(), 32);
        assert!(query.metrics().reads.mysql_rows < 20000);
        cancel.cancel();
        assert!(query.batches.try_next().await.is_err());
        drop(query);
        // ALTER must acquire the metadata lock that the cancelled snapshot held.
        server.query_drop("SET SESSION lock_wait_timeout=5").await?;
        server.query_drop(format!("ALTER TABLE {schema}.items ADD COLUMN extra INT")).await?;
        let mut next = engine.query(Language::Sql, r#"SELECT id FROM "Item" WHERE id=20000"#, Default::default()).await?;
        assert_eq!(rows(&mut next).await?, [serde_json::json!({"id":20000})]);
        Ok::<_, anyhow::Error>(())
    }.await;
    server.query_drop(format!("DROP DATABASE {schema}")).await?;
    server.disconnect().await?;
    test
}
