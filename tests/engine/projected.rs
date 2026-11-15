use anyhow::Context;
use graphlake::{
    CommunityOptions, GraphProjection, Node2VecOptions, ProjectionAlgorithm, Route, RouteStrategy,
    VertexKey,
    arrow::array::{ArrayRef, Decimal128Array, Float64Array},
};
use serde_json::json;

use super::*;

#[tokio::test]
async fn wildcard_match_joins_many_labels_without_enumerating_combinations() -> Result<()> {
    let (dir, _, _) = fixture()?;
    let mut catalog = Catalog::load(dir.path().join("graph.yaml"))?;
    let vertex = catalog.nodes["Account"].clone();
    let mut edge = catalog.edges["Invalid"].clone();
    edge.properties.clear();
    let batch = RecordBatch::try_from_iter([
        ("id", Arc::new(Int64Array::from(vec![1])) as _),
        ("src", Arc::new(Int64Array::from(vec![1])) as _),
        ("dst", Arc::new(Int64Array::from(vec![1])) as _),
    ])?;
    std::fs::write(dir.path().join("invalid.parquet"), parquet(&batch)?)?;
    catalog.nodes.clear();
    catalog.edges.clear();
    for index in 0..12 {
        let label = format!("V{index:02}");
        catalog.nodes.insert(label.clone(), vertex.clone());
        edge.from.node = label;
        edge.to.node = format!("V{:02}", (index + 1) % 12);
        catalog.edges.insert(format!("E{index:02}"), edge.clone());
    }
    let engine = Engine::new(catalog)?;
    for (query, expected) in [
        ("MATCH ()-->()-->() RETURN count(*) AS n", 12),
        ("MATCH ()- [*2]->() RETURN count(*) AS n", 12),
        ("MATCH ()-[*0..1]->() RETURN count(*) AS n", 48),
        (
            "MATCH ()-->() , ()-->() , ()-->() RETURN count(*) AS n",
            1320,
        ),
        (
            "MATCH (a:V00) OPTIONAL MATCH (a)-[r]->(:V03) RETURN count(r) AS n",
            0,
        ),
    ] {
        let mut result = engine
            .query(Language::Cypher, query, Default::default())
            .await?;
        assert_eq!(rows(&mut result).await?, [json!({"n":expected})], "{query}");
    }
    Ok(())
}

#[tokio::test]
async fn gremlin_barriers_are_scoped_to_branches_and_loop_rounds() -> Result<()> {
    let (_dir, engine, _) = fixture()?;
    let options = QueryOptions {
        batch_size: 1,
        partitions: 3,
        ..Default::default()
    };
    for (query, expected) in [
        (
            "g.V(1,3).hasLabel('Account').union(__.out('Transfer').count()).order()",
            vec![0, 3],
        ),
        (
            "g.V(1,3).hasLabel('Account').coalesce(__.out('Transfer').count(),__.identity()).is(0).count()",
            vec![1],
        ),
        (
            "g.V(1).hasLabel('Account').union(__.out('Transfer').dedup().order().by('id',desc).values('id'))",
            vec![3, 2],
        ),
        (
            "g.V(1,2).hasLabel('Account').union(__.out('Transfer').dedup().order().by('id',desc).limit(1).values('id')).order()",
            vec![3, 3],
        ),
        (
            "g.V(2).hasLabel('Account').union(__.out().order().by('id').limit(1)).hasLabel('Account').values('id')",
            vec![1],
        ),
        (
            "g.V(2).hasLabel('Account').union(__.union(__.out('Owns').count(),__.out('Transfer').values('id')).dedup().count())",
            vec![2],
        ),
        (
            "g.V(1,3).hasLabel('Account').choose(__.out('Transfer').count().is(0),__.values('id'),__.out('Transfer').order().by('id').limit(1).values('id')).order()",
            vec![2, 3],
        ),
        (
            "g.V(1,2).hasLabel('Account').repeat(__.out('Transfer').dedup().order().by('id',desc).limit(1)).times(1).values('id')",
            vec![3],
        ),
        (
            "g.V(1,2).hasLabel('Account').repeat(__.out('Transfer').dedup().order().by('id',desc).limit(1)).until(__.has('id',3)).values('id')",
            vec![3],
        ),
        (
            "g.V(1).hasLabel('Account').repeat(__.out('Transfer').simplePath().dedup().order().by('id').limit(1)).until(__.has('id',3)).values('id')",
            vec![3],
        ),
        (
            "g.V(1,3).hasLabel('Account').repeat(__.out('Transfer').count()).until(__.is(3))",
            vec![3],
        ),
        (
            "g.V(3).hasLabel('Account').repeat(__.identity()).until(__.out('Transfer').count().is(0)).values('id')",
            vec![3],
        ),
        (
            "g.V(1,2).hasLabel('Account').union(__.repeat(__.out('Transfer').order().by('id',desc).limit(1)).until(__.has('id',3)).values('id'))",
            vec![3, 3],
        ),
        (
            "g.V(99).hasLabel('Account').repeat(__.out('Transfer').count()).times(1)",
            vec![],
        ),
        (
            "g.V(1,2).hasLabel('Account').repeat(__.count()).times(64)",
            vec![1],
        ),
    ] {
        let mut result = engine
            .query(Language::Gremlin, query, options.clone())
            .await
            .map_err(|error| error.context(query.to_owned()))?;
        let expected = expected
            .into_iter()
            .map(|value| json!({"value":value}))
            .collect::<Vec<_>>();
        assert_eq!(rows(&mut result).await?, expected, "{query}");
    }
    Ok(())
}

#[tokio::test]
async fn wildcard_paths_cross_relationship_types_with_typed_endpoints() -> Result<()> {
    let (dir, engine, _) = fixture()?;
    let options = QueryOptions {
        batch_size: 2,
        partitions: 3,
        ..Default::default()
    };
    let query = "MATCH p=(a:Account {id:1})-[r*3]->(d:Device {id:'2'})
        RETURN length(p) AS hops, nodes(p) AS vertices, r AS edges";
    let result = rows(
        &mut engine
            .query(Language::Cypher, query, options.clone())
            .await?,
    )
    .await?;
    assert_eq!(result.len(), 4);
    assert!(result.iter().all(|row| row["hops"] == 3
        && row["vertices"][0] == json!({"label":"Account","key":["1"]})
        && row["vertices"][2] == json!({"label":"Device","key":["1"]})
        && row["vertices"][3] == json!({"label":"Device","key":["2"]})
        && row["edges"][1]["label"] == "Owns"
        && row["edges"][2] == json!({"label":"Peer","key":["0"]})));
    for (query, count) in [
        (
            "MATCH (d:Device {id:'2'})<-[*3]-(a:Account {id:1}) RETURN count(*) AS n",
            4,
        ),
        ("MATCH (a:Account {id:1})-[*2]->(a) RETURN count(*) AS n", 3),
        (
            "MATCH (a:Account {id:2})-[:Owns*1]->(d)-[*1]->(e:Device {id:'2'}) RETURN count(*) AS n",
            1,
        ),
        (
            "MATCH (a:Device {id:'1'})-[:Peer]-(b:Device {id:'2'})-[*1]-(a) RETURN count(*) AS n",
            0,
        ),
        (
            "MATCH (a:Device {id:'4'})-[*0..2]->(b) RETURN count(*) AS n",
            1,
        ),
        (
            "MATCH (a:Account {id:1})-[r*3]->(b:Device {id:'2'}) WITH r UNWIND r AS e RETURN count(e.weight) AS n",
            12,
        ),
        (
            "MATCH p=allShortestPaths((a:Account {id:1})-[*]->(d:Device {id:'2'})) RETURN count(*) AS n",
            4,
        ),
        (
            "MATCH p=allShortestPaths((a:Account {id:1})-[*3..4]->(d:Device {id:'2'})) WHERE length(p) = 3 RETURN count(*) AS n",
            4,
        ),
    ] {
        assert_eq!(
            rows(
                &mut engine
                    .query(Language::Cypher, query, options.clone())
                    .await?
            )
            .await?,
            [json!({"n":count})],
            "{query}"
        );
    }
    let mut catalog = Catalog::load(dir.path().join("graph.yaml"))?;
    catalog.edges.clear();
    let edgeless = Engine::new(catalog)?;
    let query = "MATCH p=(a:Device {id:'4'})-[*0..2]->(b) RETURN length(p) AS hops, relationships(p) AS edges";
    assert_eq!(
        rows(&mut edgeless.query(Language::Cypher, query, options).await?).await?,
        [json!({"hops":0,"edges":[]})]
    );
    Ok(())
}

#[tokio::test]
async fn gremlin_controls_preserve_heterogeneous_traversers() -> Result<()> {
    let (_dir, engine, _) = fixture()?;
    let options = QueryOptions {
        batch_size: 1,
        partitions: 3,
        ..Default::default()
    };
    for (query, expected) in [
        (
            "g.V(2).hasLabel('Account').repeat(__.choose(__.hasLabel('Account'),__.out('Owns'),__.out('Peer')).simplePath()).until(__.hasLabel('Device').has('id','2')).values('id')",
            vec![json!({"value":"2"})],
        ),
        (
            "g.V(2).hasLabel('Account').repeat(__.out('Owns').values('id')).until(__.is('1'))",
            vec![json!({"value":"1"})],
        ),
        (
            "g.V(1,2).hasLabel('Account').coalesce(__.out('Owns'),__.identity()).label().order()",
            vec![json!({"value":"Account"}), json!({"value":"Device"})],
        ),
        (
            "g.V(1,2).hasLabel('Account').choose(__.has('id',2),__.outE('Owns'),__.identity()).label().order()",
            vec![json!({"value":"Account"}), json!({"value":"Owns"})],
        ),
        (
            "g.V(2).hasLabel('Account').union(__.values('id'),__.out('Owns').values('id')).order()",
            vec![
                json!({"value":{"value_0":2}}),
                json!({"value":{"value_1":"1"}}),
            ],
        ),
        (
            "g.V(2).hasLabel('Account').union(__.as('marker'),__.out('Owns').as('marker')).select('marker').label().order()",
            vec![json!({"value":"Account"}), json!({"value":"Device"})],
        ),
        (
            "g.V(2).hasLabel('Account').union(__.repeat(__.out('Owns')).until(__.hasLabel('Device')),__.identity()).label().order()",
            vec![json!({"value":"Account"}), json!({"value":"Device"})],
        ),
    ] {
        assert_eq!(
            rows(
                &mut engine
                    .query(Language::Gremlin, query, options.clone())
                    .await?
            )
            .await?,
            expected,
            "{query}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn route_strategies_keep_exact_costs_and_detect_invalid_graphs() -> Result<()> {
    let (dir, engine, _) = fixture()?;
    let projection = |edges: &[&str]| GraphProjection {
        edges: edges.iter().map(|edge| (*edge).into()).collect(),
        weight: Some("weight".into()),
        ..Default::default()
    };
    let vertex = |id| VertexKey {
        label: "Account".into(),
        key: vec![ScalarValue::Int64(Some(id))],
    };
    let request = Route {
        start: vertex(1),
        target: vertex(3),
        k: 1,
    };
    let mut delta = engine
        .route_with_strategy(
            projection(&["Transfer"]),
            request.clone(),
            RouteStrategy::DeltaStepping,
            Default::default(),
        )
        .await?;
    assert_eq!(rows(&mut delta).await?[0]["cost_exact"], "2");
    for strategy in [RouteStrategy::BellmanFord, RouteStrategy::Johnson] {
        let result = engine
            .route_with_strategy(
                projection(&["Transfer", "Invalid"]),
                request.clone(),
                strategy,
                Default::default(),
            )
            .await;
        assert!(
            result
                .err()
                .unwrap()
                .to_string()
                .contains("negative-weight cycle")
        );
    }
    let result = engine
        .route_with_strategy(
            projection(&["Transfer"]),
            request.clone(),
            RouteStrategy::DagLongest,
            Default::default(),
        )
        .await;
    assert!(
        result
            .err()
            .unwrap()
            .to_string()
            .contains("acyclic projection")
    );

    let weights = Decimal128Array::from(vec![-25, 60, 90]).with_precision_and_scale(12, 2)?;
    let dag = RecordBatch::try_from_iter([
        ("id", Arc::new(Int64Array::from(vec![1, 2, 3])) as _),
        ("src", Arc::new(Int64Array::from(vec![1, 2, 1])) as _),
        ("dst", Arc::new(Int64Array::from(vec![2, 3, 3])) as _),
        ("weight", Arc::new(weights) as _),
    ])?;
    std::fs::write(dir.path().join("transfers.parquet"), parquet(&dag)?)?;
    for (strategy, cost, hops) in [
        (RouteStrategy::BellmanFord, "0.35", 2),
        (RouteStrategy::Johnson, "0.35", 2),
        (RouteStrategy::DagLongest, "0.9", 1),
    ] {
        let mut result = engine
            .route_with_strategy(
                projection(&["Transfer"]),
                request.clone(),
                strategy,
                Default::default(),
            )
            .await?;
        let values = rows(&mut result).await?;
        assert_eq!(values[0]["cost_exact"], cost);
        assert_eq!(values[0]["hops"], hops);
    }
    let options = QueryOptions::default().with_parameters_json(
        r#"{"route":{"relationshipTypes":["Transfer"],"relationshipWeightProperty":"weight",
            "strategy":"johnson","start":{"label":"Account","key":[1]},
            "target":{"label":"Account","key":[3]}}}"#,
    )?;
    let mut result = engine
        .query(
            Language::Cypher,
            "CALL algo.shortestPath($route) YIELD costExact, hops RETURN costExact, hops",
            options,
        )
        .await?;
    assert_eq!(
        rows(&mut result).await?,
        [json!({"costExact":"0.35","hops":2})]
    );
    Ok(())
}

#[tokio::test]
async fn cypher_algorithms_export_complete_results_to_registered_files() -> Result<()> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let (dir, _, _) = fixture()?;
    let mut catalog = Catalog::load(dir.path().join("graph.yaml"))?;
    catalog
        .export_locations
        .insert("reports".into(), dir.path().to_str().unwrap().into());
    let engine = Engine::new(catalog)?;
    let query = "CALL algo.labelPropagation({relationshipTypes:['Transfer'],relationshipWeightProperty:'weight',direction:'undirected'})
         YIELD id, communityId RETURN id.key AS key, communityId ORDER BY key";
    let mut plan = engine
        .query(
            Language::Cypher,
            &format!("EXPLAIN EXPORT TO 'reports:communities.parquet' {query}"),
            Default::default(),
        )
        .await?;
    assert!(!rows(&mut plan).await?.is_empty());
    let path = dir.path().join("communities.parquet");
    assert!(!path.exists());
    let command = format!("EXPORT TO 'reports:communities.parquet' {query}");
    let mut result = engine
        .query(Language::Cypher, &command, Default::default())
        .await?;
    assert!(!path.exists());
    assert_eq!(
        rows(&mut result).await?,
        [json!({"destination":"reports:communities.parquet","rows":3})]
    );
    assert_eq!(result.metrics().algorithm.unwrap().components, Some(1));
    let original = std::fs::read(&path)?;
    let mut reader =
        ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(&path)?)?.build()?;
    assert_eq!(reader.next().unwrap()?.num_rows(), 3);
    let mut duplicate = engine
        .query(Language::Cypher, &command, Default::default())
        .await?;
    assert!(rows(&mut duplicate).await.is_err());
    assert_eq!(std::fs::read(&path)?, original);
    let mut csv = engine.query(Language::Cypher, "EXPORT TO 'reports:labels.csv' CALL db.labels() YIELD label RETURN label ORDER BY label", Default::default()).await?;
    assert_eq!(
        rows(&mut csv).await?,
        [json!({"destination":"reports:labels.csv","rows":2})]
    );
    assert_eq!(
        std::fs::read_to_string(dir.path().join("labels.csv"))?,
        "label\nAccount\nDevice\n"
    );
    for (language, command) in [
        (
            Language::Sql,
            "EXPORT TO 'reports:sql.csv' SELECT count(*) AS n FROM \"Account\"",
        ),
        (
            Language::Gremlin,
            "EXPORT TO 'reports:gremlin.csv' g.V().hasLabel('Account').count()",
        ),
    ] {
        let mut result = engine.query(language, command, Default::default()).await?;
        assert_eq!(rows(&mut result).await?[0]["rows"], 1);
    }
    let options = QueryOptions::default();
    let cancellation = options.cancellation.clone();
    let mut result = engine
        .query(
            Language::Cypher,
            "EXPORT TO 'reports:cancelled.csv' RETURN 1",
            options,
        )
        .await?;
    cancellation.cancel();
    assert!(rows(&mut result).await.is_err());
    assert!(!dir.path().join("cancelled.csv").exists());
    assert!(
        engine
            .query(
                Language::Cypher,
                "EXPORT TO 'reports:../escape.csv' RETURN 1",
                Default::default()
            )
            .await
            .is_err()
    );
    Ok(())
}

fn fixture() -> Result<(TempDir, Engine, GraphProjection)> {
    let dir = tempfile::tempdir()?;
    let edges = |from: ArrayRef, to: ArrayRef, weights: Vec<f64>| -> Result<RecordBatch> {
        Ok(RecordBatch::try_from_iter([
            (
                "id",
                Arc::new(Int64Array::from_iter_values(0..weights.len() as i64)) as _,
            ),
            ("src", from),
            ("dst", to),
            ("weight", Arc::new(Float64Array::from(weights)) as _),
        ])?)
    };
    for (table, batch) in [
        (
            "accounts",
            RecordBatch::try_from_iter([("id", Arc::new(Int64Array::from(vec![1, 2, 3])) as _)])?,
        ),
        (
            "devices",
            RecordBatch::try_from_iter([(
                "id",
                Arc::new(StringArray::from(vec!["1", "2", "3", "4"])) as _,
            )])?,
        ),
        (
            "transfers",
            edges(
                Arc::new(Int64Array::from(vec![1, 1, 2, 1, 2])),
                Arc::new(Int64Array::from(vec![2, 2, 3, 3, 1])),
                vec![1.0, 2.0, 1.0, 5.0, 0.5],
            )?,
        ),
        (
            "owns",
            edges(
                Arc::new(Int64Array::from(vec![2, 3])),
                Arc::new(StringArray::from(vec!["1", "1"])),
                vec![1.0, 1.0],
            )?,
        ),
        (
            "peers",
            edges(
                Arc::new(StringArray::from(vec!["1", "2", "3"])),
                Arc::new(StringArray::from(vec!["2", "3", "1"])),
                vec![5.0, 5.0, 5.0],
            )?,
        ),
        (
            "invalid",
            edges(
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(Int64Array::from(vec![2])),
                vec![-1.0],
            )?,
        ),
    ] {
        std::fs::write(
            dir.path().join(format!("{table}.parquet")),
            parquet(&batch)?,
        )?;
    }
    let path = dir.path().join("graph.yaml");
    std::fs::write(
        &path,
        r#"
graph: mixed
sources:
  lake:
    type: parquet
    tables:
      accounts: [accounts.parquet]
      devices: [devices.parquet]
      transfers: [transfers.parquet]
      owns: [owns.parquet]
      peers: [peers.parquet]
      invalid: [invalid.parquet]
nodes:
  Account: {source: lake.accounts, key: [id]}
  Device: {source: lake.devices, key: [id]}
edges:
  Transfer:
    source: lake.transfers
    key: [id]
    from: {node: Account, columns: [src]}
    to: {node: Account, columns: [dst]}
    properties: [weight]
  Owns:
    source: lake.owns
    key: [id]
    from: {node: Account, columns: [src]}
    to: {node: Device, columns: [dst]}
    properties: [weight]
  Peer:
    source: lake.peers
    key: [id]
    from: {node: Device, columns: [src]}
    to: {node: Device, columns: [dst]}
    properties: [weight]
  Invalid:
    source: lake.invalid
    key: [id]
    from: {node: Account, columns: [src]}
    to: {node: Account, columns: [dst]}
    properties: [weight]
"#,
    )?;
    let engine = Engine::new(Catalog::load(path)?)?;
    let projection = GraphProjection {
        edges: vec!["Transfer".into(), "Owns".into(), "Peer".into()],
        weight: Some("weight".into()),
        ..Default::default()
    };
    Ok((dir, engine, projection))
}

#[tokio::test]
async fn routes_rank_large_integer_and_decimal_costs_exactly() -> Result<()> {
    let (files, engine, mut projection) = fixture()?;
    projection.edges = vec!["Transfer".into()];
    let key = |id| VertexKey {
        label: "Account".into(),
        key: vec![ScalarValue::Int64(Some(id))],
    };
    let route = Route {
        start: key(1),
        target: key(2),
        k: 2,
    };
    let cases: [(ArrayRef, [&str; 2]); 2] = [
        (
            Arc::new(Int64Array::from(vec![
                9_007_199_254_740_993,
                9_007_199_254_740_992,
            ])),
            ["9007199254740992", "9007199254740993"],
        ),
        (
            Arc::new(
                Decimal128Array::from(vec![1000000000000000001_i128, 1000000000000000000])
                    .with_precision_and_scale(20, 18)?,
            ),
            ["1", "1.000000000000000001"],
        ),
    ];
    for (weight, expected) in cases {
        let edges = RecordBatch::try_from_iter([
            ("id", Arc::new(Int64Array::from(vec![0, 1])) as ArrayRef),
            ("src", Arc::new(Int64Array::from(vec![1, 1]))),
            ("dst", Arc::new(Int64Array::from(vec![2, 2]))),
            ("weight", weight),
        ])?;
        std::fs::write(files.path().join("transfers.parquet"), parquet(&edges)?)?;
        let mut result = engine
            .route(projection.clone(), route.clone(), QueryOptions::default())
            .await?;
        let result = rows(&mut result).await?;
        assert_eq!(result[0]["edges"][0]["key"], json!(["1"]));
        assert_eq!(result[0]["cost_exact"], expected[0]);
        assert_eq!(result[1]["cost_exact"], expected[1]);
    }
    Ok(())
}

#[test]
fn community_detection_can_stop_during_local_moving() -> Result<()> {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let mut builder = leiden_rs::GraphDataBuilder::new(128);
    for node in 0..127 {
        builder.add_edge(node, node + 1, 1.0)?;
    }
    let graph = builder.build()?;
    let checks = AtomicUsize::new(0);
    let result = leiden_rs::Leiden::new(Default::default())
        .run_interruptible(&graph, &|| checks.fetch_add(1, Ordering::Relaxed) >= 12);
    assert!(matches!(result, Err(leiden_rs::LeidenError::Cancelled)));
    Ok(())
}

#[tokio::test]
async fn projected_routes_preserve_parallel_edges_and_exclude_cycles() -> Result<()> {
    let (_dir, engine, projection) = fixture()?;
    let key = |label: &str| VertexKey {
        label: label.into(),
        key: vec![ScalarValue::Utf8(Some("1".into()))],
    };
    let request = Route {
        start: key("Account"),
        target: key("Device"),
        k: 10,
    };
    let routes = rows(
        &mut engine
            .route(projection.clone(), request.clone(), Default::default())
            .await?,
    )
    .await?;
    let costs = routes
        .iter()
        .map(|row| row["cost"].as_f64().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(costs, [2.0, 3.0, 3.0, 4.0, 6.0]);
    assert_eq!(
        routes[0]["vertices"],
        json!([
            {"label": "Account", "key": ["1"]},
            {"label": "Account", "key": ["2"]},
            {"label": "Device", "key": ["1"]},
        ])
    );
    assert_eq!(
        routes[0]["edges"],
        json!([
            {"label": "Transfer", "key": ["0"]},
            {"label": "Owns", "key": ["0"]},
        ])
    );
    let mut invalid = projection.clone();
    invalid.edges.push("Invalid".into());
    assert!(
        engine
            .route(invalid, request.clone(), Default::default())
            .await
            .is_err()
    );

    let reverse = Route {
        start: request.target,
        target: request.start,
        k: 1,
    };
    assert!(
        rows(
            &mut engine
                .route(projection.clone(), reverse.clone(), Default::default())
                .await?
        )
        .await?
        .is_empty()
    );
    let incoming = GraphProjection {
        direction: Direction::Incoming,
        ..projection
    };
    assert_eq!(
        rows(&mut engine.route(incoming, reverse, Default::default()).await?).await?[0]["cost"],
        json!(2.0)
    );
    Ok(())
}

#[tokio::test]
async fn projected_analytics_share_mixed_labels_and_keep_isolates() -> Result<()> {
    let (_dir, engine, projection) = fixture()?;
    let analyze =
        |algorithm| engine.analyze_projection(projection.clone(), algorithm, Default::default());
    let rank = rows(
        &mut analyze(ProjectionAlgorithm::PageRank {
            damping: 0.85,
            tolerance: 1e-10,
            max_iterations: 150,
        })
        .await?,
    )
    .await?;
    assert_eq!(rank.len(), 7);
    let mass: f64 = rank
        .iter()
        .map(|row| row["__graphlake_rank"].as_f64().unwrap())
        .sum();
    assert!((mass - 1.0).abs() < 1e-10);
    let weak = rows(&mut analyze(ProjectionAlgorithm::WeaklyConnectedComponents).await?).await?;
    assert_eq!(
        weak[5]["__graphlake_component"],
        json!({"label": "Account", "key": ["1"]})
    );
    assert_eq!(
        weak[6]["__graphlake_component"],
        json!({"label": "Device", "key": ["4"]})
    );

    for algorithm in [
        ProjectionAlgorithm::Louvain(CommunityOptions::default()),
        ProjectionAlgorithm::Leiden(CommunityOptions::default()),
    ] {
        let result = rows(&mut analyze(algorithm).await?).await?;
        let groups = result
            .iter()
            .map(|row| &row["__graphlake_community"])
            .collect::<Vec<_>>();
        assert!(groups[..3].iter().all(|&group| group == groups[0]));
        assert!(groups[3..6].iter().all(|&group| group == groups[3]));
        assert_ne!(groups[0], groups[3]);
        assert_ne!(groups[3], groups[6]);
        assert_ne!(groups[0], groups[6]);
    }
    let algorithm = ProjectionAlgorithm::Node2Vec(Node2VecOptions {
        dimensions: 8,
        ..Default::default()
    });
    let embeddings = rows(&mut analyze(algorithm).await?).await?;
    assert_eq!(embeddings, rows(&mut analyze(algorithm).await?).await?);
    assert_eq!(embeddings.len(), 7);
    assert!(embeddings.iter().all(|row| {
        row["__graphlake_embedding"]
            .as_array()
            .is_some_and(|values| {
                values.len() == 8
                    && values
                        .iter()
                        .all(|value| value.as_f64().is_some_and(f64::is_finite))
            })
    }));
    Ok(())
}

#[tokio::test]
async fn cypher_compatibility_preserves_mixed_nodes_and_nested_values() -> Result<()> {
    let (_dir, engine, _) = fixture()?;
    for (query, expected) in [
        (
            "MATCH (n) RETURN count(n) AS total",
            vec![json!({"total": 7})],
        ),
        (
            "WITH {name: 'mapped'} AS m RETURN properties(m).name AS name, properties({}) AS empty",
            vec![json!({"name":"mapped","empty":{}})],
        ),
        (
            "MATCH (n), (m) WHERE id(n) = id(m) RETURN count(*) AS total",
            vec![json!({"total": 7})],
        ),
        (
            "MATCH (n) RETURN id(n) AS id ORDER BY id",
            [
                "Account[1]",
                "Account[2]",
                "Account[3]",
                "Device[1]",
                "Device[2]",
                "Device[3]",
                "Device[4]",
            ]
            .map(|id| json!({"id":id}))
            .to_vec(),
        ),
        (
            "MATCH (a:Account {id: 1}) OPTIONAL MATCH (a)-[e:Owns]->(d) RETURN CASE WHEN d IS NULL THEN 'missing' END AS status, id(d) AS missingId, properties(e) AS missingProperties, properties(a).id AS key",
            vec![json!({"status":"missing","key":1})],
        ),
        (
            "MATCH (:Account {id: 1})-[e:Transfer]->() RETURN id(e) AS id, properties(e).weight AS weight ORDER BY id",
            vec![
                json!({"id":"Transfer[0]","weight":1.0}),
                json!({"id":"Transfer[1]","weight":2.0}),
                json!({"id":"Transfer[3]","weight":5.0}),
            ],
        ),
        (
            "WITH [1,2,3,4] AS xs RETURN CASE 2 WHEN 1 THEN 'x' WHEN 2 THEN 'y' ELSE 'z' END AS simple, xs[1..3] AS middle, xs[-2..] AS tail, xs[..-1] AS head, xs[..0] AS empty, xs[3..2] AS reversed, xs[100..] AS beyond, xs[..null] AS missing, xs[-100..100] AS allItems",
            vec![
                json!({"simple":"y","middle":[2,3],"tail":[3,4],"head":[1,2,3],"empty":[],"reversed":[],"beyond":[],"allItems":[1,2,3,4]}),
            ],
        ),
    ] {
        let mut result = engine
            .query(Language::Cypher, query, Default::default())
            .await
            .with_context(|| query.to_owned())?;
        assert_eq!(
            rows(&mut result).await.with_context(|| query.to_owned())?,
            expected,
            "{query}"
        );
    }

    let mut schema = engine
        .query(
            Language::Cypher,
            "CALL db.schema.visualization() YIELD nodes, relationships RETURN nodes, relationships",
            Default::default(),
        )
        .await?;
    let result = rows(&mut schema).await?;
    assert_eq!(
        result[0]["nodes"][0],
        json!({"id":"Account","label":"Account","name":"Account","indexes":[],"constraints":[]})
    );
    assert_eq!(
        result[0]["relationships"][1],
        json!({"id":"Owns","type":"Owns","name":"Owns","source":"Account","target":"Device"})
    );
    assert_eq!(schema.metrics().reads.requests, 0);
    Ok(())
}

#[tokio::test]
async fn gremlin_collections_keep_types_and_per_traverser_scope() -> Result<()> {
    let (_dir, engine, _) = fixture()?;
    for (query, expected) in [
        (
            "g.V(2).hasLabel('Account').values('id').unfold()",
            vec![json!(2)],
        ),
        (
            "g.V(2).hasLabel('Account').project('name','degree').by(label()).by(out('Transfer').count()).unfold().select('key')",
            vec![json!("name"), json!("degree")],
        ),
        (
            "g.V(2).hasLabel('Account').valueMap('id').unfold().select('value').unfold()",
            vec![json!(2)],
        ),
        ("g.V().count()", vec![json!(7)]),
        ("g.V().hasLabel('Account','Device').count()", vec![json!(7)]),
        (
            "g.V().label().order()",
            [
                "Account", "Account", "Account", "Device", "Device", "Device", "Device",
            ]
            .map(|label| json!(label))
            .to_vec(),
        ),
        ("g.V('Account[1]').id()", vec![json!("Account[1]")]),
        (
            "g.V().hasLabel('Account').order().by('id',desc).values('id').fold().unfold()",
            vec![json!(3), json!(2), json!(1)],
        ),
        ("g.V(99).hasLabel('Account').fold()", vec![json!([])]),
        (
            "g.V().fold().unfold().hasLabel('Device').out('Peer').count()",
            vec![json!(3)],
        ),
        (
            "g.V().hasLabel('Account').order().by('id').project('id','degree').by('id').by(out('Transfer').count())",
            vec![
                json!({"id":1,"degree":3}),
                json!({"id":2,"degree":2}),
                json!({"id":3,"degree":0}),
            ],
        ),
        (
            "g.V().group().by(label()).by(count())",
            vec![json!({"Account":3,"Device":4})],
        ),
        (
            "g.V().group().by(label()).by(count()).unfold().select('key').order()",
            vec![json!("Account"), json!("Device")],
        ),
        (
            "g.V().hasLabel('Account').order().by('id',desc).group().by(label()).by('id')",
            vec![json!({"Account":[3,2,1]})],
        ),
        (
            "g.V(1,3).hasLabel('Account').order().by('id').union(__.out('Transfer').group().by(label()).by(count()))",
            vec![json!({"Account":3}), json!({})],
        ),
        (
            "g.V(2).hasLabel('Account').as('a').out('Owns').as('b').select('a','b').by('id')",
            vec![json!({"a":2,"b":"1"})],
        ),
        (
            "g.V(2).hasLabel('Account').as('a').out('Owns').as('b').select('a','b').select('b').values('id')",
            vec![json!("1")],
        ),
        (
            "g.V().as('a').identity().as('b').select('a','b').select('b').hasLabel('Device').count()",
            vec![json!(4)],
        ),
    ] {
        let mut result = engine
            .query(Language::Gremlin, query, Default::default())
            .await
            .with_context(|| query.to_owned())?;
        let output = rows(&mut result).await.with_context(|| query.to_owned())?;
        assert_eq!(
            output,
            expected
                .into_iter()
                .map(|value| json!({"value":value}))
                .collect::<Vec<_>>(),
            "{query}"
        );
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires a writable PostgreSQL fixture"]
async fn gremlin_qualified_ids_cross_postgres_node_labels() -> Result<()> {
    let dsn = std::env::var("GRAPHLAKE_TEST_POSTGRES")?;
    let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls).await?;
    let driver = tokio::spawn(connection);
    let schema = format!("graphlake_compat_{}", std::process::id());
    client
        .batch_execute(&format!(
            "CREATE SCHEMA {schema};
         CREATE TABLE {schema}.accounts (tenant bigint, id bigint, PRIMARY KEY (tenant, id));
         CREATE TABLE {schema}.devices (id text PRIMARY KEY);
         INSERT INTO {schema}.accounts VALUES (1,10);
         INSERT INTO {schema}.devices VALUES ('device,b%[x]');"
        ))
        .await?;
    let check = async {
        let dir = tempfile::tempdir()?;
        let config = dir.path().join("graph.yaml");
        std::fs::write(
            &config,
            format!(
                r#"
graph: compat
sources:
  pg: {{type: postgres, dsn_env: GRAPHLAKE_TEST_POSTGRES}}
nodes:
  Account: {{source: pg.{schema}.accounts, key: [tenant, id]}}
  Device: {{source: pg.{schema}.devices, key: [id]}}
"#
            ),
        )?;
        let engine = Engine::new(Catalog::load(config)?)?;
        for id in ["Account[1,10]", "Device[device%2Cb%25%5Bx%5D]"] {
            let mut result = engine
                .query(
                    Language::Gremlin,
                    &format!("g.V('{id}').id()"),
                    Default::default(),
                )
                .await?;
            assert_eq!(rows(&mut result).await?, [json!({"value":id})]);
        }
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
async fn cypher_calls_inside_row_subqueries_keep_their_input_scope() -> Result<()> {
    let (_dir, engine, _) = fixture()?;
    for (query, expected) in [
        (
            "MATCH (a:Account) RETURN a.id AS id, EXISTS { WITH 1 AS k CALL (a,k) { MATCH (a)-[:Transfer]->(b) RETURN b.id AS dst ORDER BY dst SKIP 1 LIMIT 1 } WITH dst WHERE dst > a.id+k-1 } AS hit ORDER BY id",
            vec![
                json!({"id":1,"hit":true}),
                json!({"id":2,"hit":true}),
                json!({"id":3,"hit":false}),
            ],
        ),
        (
            "UNWIND [1,1,2,3] AS id MATCH (a:Account) WHERE a.id=id RETURN id, COUNT { CALL (a) { MATCH (a)-[:Transfer]->(b) CALL (b) { WITH 0 AS unused RETURN b.id AS dst } RETURN dst ORDER BY dst SKIP 1 LIMIT 1 } RETURN dst } AS n ORDER BY id",
            vec![
                json!({"id":1,"n":1}),
                json!({"id":1,"n":1}),
                json!({"id":2,"n":1}),
                json!({"id":3,"n":0}),
            ],
        ),
        (
            "MATCH (a:Account) RETURN a.id AS id, COUNT { CALL { WITH a MATCH (a)-[:Transfer]->(b) WITH count(DISTINCT b) AS total RETURN total } WITH total WHERE total=0 } AS empty, COUNT { CALL (a) { MATCH (a)-[:Transfer]->(b) RETURN DISTINCT b.id AS dst } RETURN dst } AS unique ORDER BY id",
            vec![
                json!({"id":1,"empty":0,"unique":2}),
                json!({"id":2,"empty":0,"unique":2}),
                json!({"id":3,"empty":1,"unique":0}),
            ],
        ),
        (
            "RETURN [x IN [0,1,1,2] | COUNT { CALL (x) { UNWIND [1,2] AS y WITH y WHERE y<=x RETURN y } RETURN y }] AS sizes, EXISTS { CALL () { RETURN 1 AS y LIMIT 0 } } AS empty",
            vec![json!({"sizes":[0,1,1,2],"empty":false})],
        ),
        (
            "RETURN [x IN [1,1.0,2] | COUNT { CALL (x) { RETURN x/2 AS y } WITH y WHERE y=0.5 }] AS sizes",
            vec![json!({"sizes":[0,1,0]})],
        ),
        (
            "MATCH (a:Account) RETURN a.id AS id, [x IN [a,a] | COUNT { CALL (x) { MATCH (x)-[:Transfer]->(b) RETURN b.id AS dst ORDER BY dst LIMIT 2 } RETURN dst }] AS sizes ORDER BY id",
            vec![
                json!({"id":1,"sizes":[2,2]}),
                json!({"id":2,"sizes":[2,2]}),
                json!({"id":3,"sizes":[0,0]}),
            ],
        ),
        (
            "UNWIND [1,2] AS x RETURN x, EXISTS { WITH x AS k WHERE EXISTS { CALL (k) { RETURN k AS y } WITH y WHERE y=1 } RETURN k } AS hit ORDER BY x",
            vec![json!({"x":1,"hit":true}), json!({"x":2,"hit":false})],
        ),
        (
            "MATCH (a:Account) WITH a, 2 AS k RETURN a.id AS id, EXISTS { MATCH (a)-[:Transfer]->(b) WHERE EXISTS { CALL (k,b) { RETURN b.id AS dst } WITH dst WHERE dst=k } } AS hit ORDER BY id",
            vec![
                json!({"id":1,"hit":true}),
                json!({"id":2,"hit":false}),
                json!({"id":3,"hit":false}),
            ],
        ),
        (
            "WITH 3 AS x RETURN COUNT { CALL (x) { RETURN x AS y LIMIT 0 } WITH count(*) AS total WHERE total=0 RETURN total } AS n",
            vec![json!({"n":1})],
        ),
    ] {
        let mut result = engine
            .query(
                Language::Cypher,
                query,
                QueryOptions {
                    memory_bytes: 16 * 1024 * 1024,
                    ..Default::default()
                },
            )
            .await
            .with_context(|| query.to_owned())?;
        assert_eq!(
            rows(&mut result).await.with_context(|| query.to_owned())?,
            expected,
            "{query}"
        );
    }

    for query in [
        "WITH 1 AS x RETURN COUNT { CALL () { RETURN x AS leaked } } AS n",
        "WITH 1 AS x RETURN EXISTS { CALL (x) { RETURN 2 AS x } } AS hit",
        "WITH 1 AS x RETURN COUNT { CALL (missing) { RETURN 1 AS y } } AS n",
    ] {
        assert!(
            engine
                .query(Language::Cypher, query, Default::default())
                .await
                .is_err(),
            "{query}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn cypher_lists_exists_and_polymorphic_fields_preserve_semantics() -> Result<()> {
    let (_dir, engine, _) = fixture()?;
    for (query, expected) in [
        (
            "WITH [1,2,3,null] AS xs RETURN xs[0] AS first, xs[-2] AS tail, xs[10] AS missing, xs[-9] AS low, xs[9223372036854775807] AS high, [x IN xs WHERE x > 1 | x * 2] AS filtered",
            vec![json!({"first":1,"tail":3,"filtered":[4,6]})],
        ),
        (
            "WITH [1,2,null] AS xs, [] AS empty RETURN 2 IN xs AS found, 9 IN xs AS unknown, 9 IN [1,2] AS absent, null IN empty AS empty, '1' IN [1] AS typed, 2 IN [1,1+1] AS computed",
            vec![json!({"found":true,"absent":false,"empty":false,"typed":false,"computed":true})],
        ),
        (
            "WITH [{k:1},{k:2},{k:3}] AS xs, 2 AS min RETURN [x IN xs WHERE x.k >= min | {result: x.k}] AS picked",
            vec![json!({"picked":[{"result":2},{"result":3}]})],
        ),
        (
            "WITH [1,'1',null] AS xs RETURN 1 IN xs AS number, '1' IN xs AS text, 7 IN xs AS unknown",
            vec![json!({"number":true,"text":true})],
        ),
        (
            "MATCH (n:Account|Device) WITH n ORDER BY id(n) WITH collect(n) AS items RETURN id(items[0]) AS first, [x IN items WHERE x.id = '1' | id(x)] AS strings",
            vec![json!({"first":"Account[1]","strings":["Device[1]"]})],
        ),
        (
            "MATCH (a:Account) WHERE EXISTS { MATCH (a)-[:Transfer*2..2]->(:Account {id: 3}) } RETURN a.id AS id ORDER BY id",
            vec![json!({"id":1}), json!({"id":2})],
        ),
        (
            "MATCH (a:Account) WHERE EXISTS { MATCH (a)-[:Owns]->() } RETURN a.id AS id ORDER BY id",
            vec![json!({"id":2}), json!({"id":3})],
        ),
        (
            "MATCH (a:Account) RETURN a.id AS id, EXISTS { MATCH (a)-[:Owns]->() } AS owns ORDER BY id",
            vec![
                json!({"id":1,"owns":false}),
                json!({"id":2,"owns":true}),
                json!({"id":3,"owns":true}),
            ],
        ),
        (
            "MATCH (a:Account) RETURN a.id AS id, EXISTS { MATCH (a)-[:Transfer]->(b:Account) WITH b ORDER BY b.id SKIP 1 LIMIT 1 WHERE a.id < b.id } AS hit, COUNT { (a)-[:Transfer]->(:Account) } AS hits ORDER BY id",
            vec![
                json!({"id":1,"hit":true,"hits":3}),
                json!({"id":2,"hit":false,"hits":2}),
                json!({"id":3,"hit":false,"hits":0}),
            ],
        ),
        (
            "MATCH (a:Account) RETURN a.id AS id, COUNT { MATCH (a)-[:Owns]->() WITH count(*) AS hits WHERE hits = 0 RETURN hits } AS empty, COUNT { MATCH (a)-[:Owns]->() RETURN 1 AS x UNION ALL RETURN 1 AS x } AS either ORDER BY id",
            vec![
                json!({"id":1,"empty":1,"either":1}),
                json!({"id":2,"empty":0,"either":2}),
                json!({"id":3,"empty":0,"either":2}),
            ],
        ),
        (
            "MATCH (a:Account) WITH a, [(a)-[:Owns]->(d:Device) | d] AS owners RETURN a.id AS id, [d IN owners | id(d)] AS owners ORDER BY id",
            vec![
                json!({"id":1,"owners":[]}),
                json!({"id":2,"owners":["Device[1]"]}),
                json!({"id":3,"owners":["Device[1]"]}),
            ],
        ),
        (
            "RETURN COUNT { UNWIND [0,1,2] AS x WITH x LIMIT 1 RETURN x } AS rows, [(a:Account {id:99})-[:Owns]->(d:Device) | d.id] AS missing, 'COUNT { }' AS text",
            vec![json!({"rows":1,"missing":[],"text":"COUNT { }"})],
        ),
        (
            "RETURN EXISTS { WITH 1 AS x WHERE x=1 RETURN x } AS hit, EXISTS { WITH 1 AS x WHERE x=0 RETURN x } AS miss, COUNT { RETURN 9 }, {n:COUNT {RETURN null}} AS m",
            vec![json!({"hit":true,"miss":false,"COUNT { RETURN 9 }":1,"m":{"n":1}})],
        ),
        (
            "RETURN [x IN [0,1,1,2] | COUNT { UNWIND [1,2] AS y WITH y WHERE y <= x RETURN y }] AS counts, [x IN [0,1,2] | EXISTS { WITH x AS k WHERE k = 1 RETURN k }] AS hits",
            vec![json!({"counts":[0,1,1,2],"hits":[false,true,false]})],
        ),
        (
            "MATCH (a:Account {id:2}) WITH [p = (a)-[:Owns]->(d:Device) | p] AS paths UNWIND paths AS p RETURN length(p) AS hops",
            vec![json!({"hops":1})],
        ),
        (
            "MATCH (a:Account) OPTIONAL MATCH (a)-[r:Transfer]->(b) WHERE EXISTS { MATCH (b)-[:Owns]->() } RETURN a.id AS id, count(r) AS edges ORDER BY id",
            vec![
                json!({"id":1,"edges":3}),
                json!({"id":2,"edges":1}),
                json!({"id":3,"edges":0}),
            ],
        ),
        (
            "MATCH (a:Account|Device)-[:Owns]->(b:Device|Account) RETURN count(*) AS pairs",
            vec![json!({"pairs":2})],
        ),
        (
            "MATCH (n:Account|Device) WHERE n.id IN [1, '1'] RETURN id(n) AS id, n.id AS value, properties(n).id AS same ORDER BY id",
            vec![
                json!({"id":"Account[1]","value":{"Int64":1},"same":{"Int64":1}}),
                json!({"id":"Device[1]","value":{"Utf8View":"1"},"same":{"Utf8View":"1"}}),
            ],
        ),
        (
            "MATCH (n) WITH id(n) AS id, n.id AS k WHERE k = 1 RETURN id",
            vec![json!({"id":"Account[1]"})],
        ),
        (
            "MATCH (n) WHERE n.id = '1' RETURN n",
            vec![json!({"n":{"label":"Device","key":["1"],"properties":{"id":{"Utf8View":"1"}}}})],
        ),
    ] {
        let mut result = engine
            .query(Language::Cypher, query, Default::default())
            .await
            .with_context(|| query.to_owned())?;
        assert_eq!(
            rows(&mut result).await.with_context(|| query.to_owned())?,
            expected,
            "{query}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn gremlin_predicates_emission_and_typed_group_keys() -> Result<()> {
    let (_dir, engine, _) = fixture()?;
    for (query, expected) in [
        (
            "g.V().group().by('id').by(count())",
            vec![json!({"@Int64:1":1,"@Int64:2":1,"@Int64:3":1,"1":1,"2":1,"3":1,"4":1})],
        ),
        (
            "g.V().groupCount().by('id')",
            vec![json!({"@Int64:1":1,"@Int64:2":1,"@Int64:3":1,"1":1,"2":1,"3":1,"4":1})],
        ),
        (
            "g.V().where(out('Transfer').count().is(gt(1))).id().order()",
            vec![json!("Account[1]"), json!("Account[2]")],
        ),
        (
            "g.V(1).hasLabel('Account').as('a').out('Transfer').as('b').where('a',neq('b')).where(eq('b')).count()",
            vec![json!(3)],
        ),
        (
            "g.V().hasLabel('Account').values('id').mean()",
            vec![json!(2.0)],
        ),
        ("g.V(99).hasLabel('Account').values('id').mean()", vec![]),
        (
            "g.V(1).hasLabel('Account').repeat(__.out('Transfer')).times(2).emit().count()",
            vec![json!(7)],
        ),
        (
            "g.V(1).hasLabel('Account').emit().repeat(__.out('Transfer')).times(2).count()",
            vec![json!(8)],
        ),
        (
            "g.V(1).hasLabel('Account').repeat(__.out('Transfer')).emit(__.has('id',3)).times(2).values('id')",
            vec![json!(3), json!(1), json!(1), json!(3), json!(3)],
        ),
    ] {
        let mut result = engine
            .query(Language::Gremlin, query, Default::default())
            .await
            .with_context(|| query.to_owned())?;
        let mut actual = rows(&mut result).await.with_context(|| query.to_owned())?;
        let mut expected = expected
            .into_iter()
            .map(|value| json!({"value":value}))
            .collect::<Vec<_>>();
        actual.sort_by_key(ToString::to_string);
        expected.sort_by_key(ToString::to_string);
        assert_eq!(actual, expected, "{query}");
    }
    Ok(())
}

#[tokio::test]
async fn cypher_branches_and_comparisons_keep_number_and_text_distinct() -> Result<()> {
    let (_dir, engine, _) = fixture()?;
    let number = json!({"v":{"Int64":1}});
    let text = json!({"v":{"Utf8View":"1"}});
    for (query, expected) in [
        (
            "RETURN 1 AS v UNION RETURN '1' AS v UNION RETURN 1 AS v",
            vec![number.clone(), text.clone()],
        ),
        (
            "RETURN 1 AS v UNION ALL RETURN '1' AS v UNION ALL RETURN 1 AS v",
            vec![number.clone(), number.clone(), text.clone()],
        ),
        (
            "CALL { RETURN 1 AS v UNION RETURN '1' AS v } WITH v WHERE v = 1 RETURN v",
            vec![number.clone()],
        ),
        (
            "CALL { RETURN 1 AS v UNION ALL RETURN '1' AS v } RETURN v UNION ALL RETURN true AS v",
            vec![number.clone(), text.clone(), json!({"v":{"Boolean":true}})],
        ),
        (
            "UNWIND [true,false] AS flag RETURN CASE WHEN flag THEN 1 ELSE '1' END AS v",
            vec![number.clone(), text.clone()],
        ),
        (
            "UNWIND [0,2] AS divisor RETURN CASE WHEN divisor = 0 THEN 'zero' ELSE 6 / divisor END AS v",
            vec![json!({"v":{"Utf8View":"zero"}}), json!({"v":{"Int64":3}})],
        ),
        (
            "WITH CASE WHEN true THEN '1' ELSE 1 END AS v RETURN CASE v WHEN 1 THEN 'number' WHEN '1' THEN 'text' END AS v",
            vec![json!({"v":"text"})],
        ),
        (
            "RETURN CASE WHEN false THEN 1 WHEN null THEN '1' END AS v",
            vec![json!({})],
        ),
        (
            "RETURN 1 = '1' AS eq, 1 <> '1' AS ne, 1 IN ['1'] AS member, 1 < '2' AS ordered, 1 = 1.0 AS numeric, 9007199254740993 = 9007199254740992.0 AS rounded, null = null AS unknown",
            vec![json!({"eq":false,"ne":true,"member":false,"numeric":true,"rounded":false})],
        ),
        (
            "CALL { RETURN 1 AS v UNION RETURN 1.0 AS v UNION RETURN '1' AS v } RETURN count(v) AS count",
            vec![json!({"count":2})],
        ),
        (
            "CALL { RETURN 9007199254740993 AS v UNION RETURN 9007199254740992.0 AS v } RETURN count(v) AS count",
            vec![json!({"count":2})],
        ),
        (
            "MATCH (a:Account {id:'1'}) RETURN count(a) AS matched",
            vec![json!({"matched":0})],
        ),
        (
            "MATCH (a:Account) WHERE a.id = '1' RETURN count(a) AS matched",
            vec![json!({"matched":0})],
        ),
    ] {
        let options = QueryOptions {
            batch_size: 1,
            partitions: 3,
            ..Default::default()
        };
        let mut result = engine
            .query(Language::Cypher, query, options)
            .await
            .with_context(|| query.to_owned())?;
        let mut actual = rows(&mut result).await.with_context(|| query.to_owned())?;
        let mut expected = expected;
        actual.sort_by_key(ToString::to_string);
        expected.sort_by_key(ToString::to_string);
        assert_eq!(actual, expected, "{query}");
    }
    Ok(())
}

#[tokio::test]
async fn gremlin_emitting_cycles_stop_at_their_immediate_limit() -> Result<()> {
    let (_dir, engine, _) = fixture()?;
    for (query, expected) in [
        (
            "g.V('Account[1]').repeat(__.out('Transfer')).emit().id().dedup().not(__.is('Account[1]')).skip(1).limit(1)",
            vec![json!("Account[3]")],
        ),
        (
            "g.V('Account[1]').repeat(__.out('Transfer')).emit().id().limit(1)",
            vec![json!("Account[2]")],
        ),
        (
            "g.V('Account[1]','Device[1]').repeat(__.out('Transfer')).emit().values('id').limit(1).math('_')",
            vec![json!(2.0)],
        ),
        (
            "g.V('Account[1]').repeat(__.out('Transfer')).emit().path().limit(1)",
            vec![
                json!([{"label":"Account","kind":"vertex","key":["1"]},{"label":"Account","kind":"vertex","key":["2"]}]),
            ],
        ),
        (
            "g.V('Account[1]').repeat(__.out('Transfer')).emit().values('id').limit(1)",
            vec![json!(2)],
        ),
        (
            "g.V('Account[1]').repeat(__.out('Transfer')).emit().valueMap('id').limit(1)",
            vec![json!({"id":[2]})],
        ),
        (
            "g.V('Account[1]','Account[3]').filter(__.repeat(__.out('Transfer')).emit()).id()",
            vec![json!("Account[1]")],
        ),
        (
            "g.V('Account[1]','Account[2]').map(__.repeat(__.out('Transfer')).emit().id()).order()",
            vec![json!("Account[1]"), json!("Account[2]")],
        ),
        (
            "g.V('Account[1]').repeat(__.out('Transfer')).emit().has('id',1).id().limit(1)",
            vec![json!("Account[1]")],
        ),
        (
            "g.V('Account[1]').repeat(__.out('Transfer')).emit().id().dedup().limit(3)",
            vec![
                json!("Account[2]"),
                json!("Account[3]"),
                json!("Account[1]"),
            ],
        ),
        (
            "g.V('Account[1]','Account[2]').union(__.repeat(__.out('Transfer')).emit().id().dedup().limit(2))",
            vec![
                json!("Account[2]"),
                json!("Account[3]"),
                json!("Account[1]"),
                json!("Account[3]"),
            ],
        ),
        (
            "g.V('Account[1]').map(__.repeat(__.out('Transfer')).emit().project('key').by(id())).limit(1)",
            vec![json!({"key":"Account[2]"})],
        ),
        (
            "g.V('Account[1]').repeat(__.out('Transfer')).emit().dedup().has('id',1).id().limit(1)",
            vec![json!("Account[1]")],
        ),
        (
            "g.V('Account[1]').repeat(__.out('Transfer')).emit().has('id',2).dedup().id().limit(1)",
            vec![json!("Account[2]")],
        ),
        (
            "g.V('Account[1]','Account[3]').choose(__.repeat(__.out('Transfer')).emit().has('id',2),__.label(),__.id()).order()",
            vec![json!("Account"), json!("Account[3]")],
        ),
        (
            "g.V('Account[1]').coalesce(__.identity(),__.repeat(__.out('Transfer')).emit()).id().limit(1)",
            vec![json!("Account[1]")],
        ),
        (
            "g.V('Account[1]','Account[2]').coalesce(__.has('id',1),__.repeat(__.out('Transfer')).emit().has('id',1).limit(1)).id()",
            vec![json!("Account[1]"), json!("Account[1]")],
        ),
        (
            "g.V(1).hasLabel('Account').repeat(__.out('Transfer')).emit().id().limit(1)",
            vec![json!("Account[2]")],
        ),
        (
            "g.V(1).hasLabel('Account').repeat(__.out('Transfer')).emit().id().range(1,3)",
            vec![json!("Account[2]"), json!("Account[3]")],
        ),
        (
            "g.V(1,2).hasLabel('Account').union(__.repeat(__.out('Transfer')).emit().id().limit(1))",
            vec![json!("Account[2]"), json!("Account[1]")],
        ),
        (
            "g.V(1).hasLabel('Account').repeat(__.out('Transfer')).emit().limit(2).values('id')",
            vec![json!(2), json!(2)],
        ),
        (
            "g.V(1).hasLabel('Account').emit().repeat(__.out('Transfer')).limit(1).values('id')",
            vec![json!(1)],
        ),
        (
            "g.V(1).hasLabel('Account').repeat(__.out('Transfer')).emit(__.has('id',3)).limit(2).values('id')",
            vec![json!(3), json!(3)],
        ),
        (
            "g.V(1,2).hasLabel('Account').union(__.repeat(__.out('Transfer')).emit(__.has('id',3)).limit(2).values('id'))",
            vec![json!(3), json!(3), json!(3), json!(3)],
        ),
        (
            "g.V(1).hasLabel('Account').repeat(__.out('Transfer')).emit().limit(0).count()",
            vec![json!(0)],
        ),
        (
            "g.V(1).hasLabel('Account').repeat(__.out('Transfer')).times(2).emit().dedup().order().by('id',desc).limit(2).values('id')",
            vec![json!(3), json!(2)],
        ),
    ] {
        let options = QueryOptions {
            timeout: std::time::Duration::from_secs(3),
            batch_size: 1,
            partitions: 3,
            ..Default::default()
        };
        let mut result = engine
            .query(Language::Gremlin, query, options)
            .await
            .with_context(|| query.to_owned())?;
        let actual = rows(&mut result).await.with_context(|| query.to_owned())?;
        let expected = expected
            .into_iter()
            .map(|v| json!({"value":v}))
            .collect::<Vec<_>>();
        assert_eq!(actual, expected, "{query}");
    }
    Ok(())
}

#[tokio::test]
async fn cypher_entity_identity_survives_lists_and_unions() -> Result<()> {
    let (dir, _, _) = fixture()?;
    let mut catalog = Catalog::load(dir.path().join("graph.yaml"))?;
    catalog
        .nodes
        .insert("Replica".into(), catalog.nodes["Account"].clone());
    let engine = Engine::new(catalog)?;
    for (query, expected) in [
        (
            "MATCH (a:Account {id:1}), (b:Replica {id:1}) RETURN a = b AS same, a IN [b] AS member, a = properties(a) AS map, a = a AS self",
            vec![json!({"same":false,"member":false,"map":false,"self":true})],
        ),
        (
            "MATCH (a:Account {id:1}), (b:Replica {id:1}) WITH a, collect(b) AS bs RETURN a IN bs AS member, a IN [bs[0],a] AS found, [a,bs[0]] AS values",
            vec![
                json!({"member":false,"found":true,"values":[{"label":"Account","key":["1"],"properties":{"id":1}},{"label":"Replica","key":["1"],"properties":{"id":1}}]}),
            ],
        ),
        (
            "MATCH (a:Account {id:1}) RETURN a AS n UNION ALL MATCH (b:Replica {id:1}) RETURN b AS n",
            vec![
                json!({"n":{"label":"Account","key":["1"],"properties":{"id":1}}}),
                json!({"n":{"label":"Replica","key":["1"],"properties":{"id":1}}}),
            ],
        ),
        (
            "CALL { MATCH (a:Account {id:1}) RETURN a AS n UNION MATCH (b:Replica {id:1}) RETURN b AS n UNION MATCH (c:Account {id:1}) RETURN c AS n } MATCH (m:Account {id:1}) RETURN id(n) AS id, n = m AS same ORDER BY id",
            vec![
                json!({"id":"Account[1]","same":true}),
                json!({"id":"Replica[1]","same":false}),
            ],
        ),
        (
            "CALL { MATCH (a:Account {id:1}) RETURN collect(a) AS ns UNION ALL MATCH (b:Replica {id:1}) RETURN collect(b) AS ns } UNWIND ns AS n RETURN id(n) AS id",
            vec![json!({"id":"Account[1]"}), json!({"id":"Replica[1]"})],
        ),
        (
            "MATCH (a:Account {id:1}), (b:Replica {id:1}) WITH a,b,CASE WHEN a.id = 1 THEN b ELSE a END AS n RETURN id(n) AS id, n = a AS eq, n IN [b] AS member",
            vec![json!({"id":"Replica[1]","eq":false,"member":true})],
        ),
        (
            "MATCH (a:Account {id:1}) OPTIONAL MATCH (a)-[:Transfer]->(b:Account {id:99}) RETURN b AS missing, b{.*} AS missing_map, a IN [b] AS unknown, a{.id} AS present",
            vec![json!({"present":{"id":1}})],
        ),
    ] {
        let mut result = engine
            .query(Language::Cypher, query, Default::default())
            .await
            .with_context(|| query.to_owned())?;
        let mut actual = rows(&mut result).await.with_context(|| query.to_owned())?;
        let mut expected = expected;
        actual.sort_by_key(ToString::to_string);
        expected.sort_by_key(ToString::to_string);
        assert_eq!(actual, expected, "{query}");
    }
    Ok(())
}

#[tokio::test]
async fn cypher_distinct_grouping_and_order_use_logical_values() -> Result<()> {
    let (_dir, engine, _) = fixture()?;
    for (query, expected) in [
        (
            "UNWIND [1,1.0,'1',null] AS x RETURN count(DISTINCT x) AS distinct_count, size(collect(DISTINCT x)) AS collected",
            vec![json!({"distinct_count":2,"collected":2})],
        ),
        (
            "UNWIND [1,1.0,'1',null,null] AS x WITH DISTINCT x RETURN count(*) AS n",
            vec![json!({"n":3})],
        ),
        (
            "UNWIND [1,1.0,'1',null,null] AS x WITH x, count(*) AS n RETURN x = 1 AS numeric, n ORDER BY n, x",
            vec![
                json!({"numeric":false,"n":1}),
                json!({"numeric":true,"n":2}),
                json!({"n":2}),
            ],
        ),
        (
            "UNWIND [2,1,2,1] AS x RETURN x, count(*) AS n ORDER BY n + x",
            vec![json!({"x":1,"n":2}), json!({"x":2,"n":2})],
        ),
        (
            "UNWIND [0,1.0,2,-1.0,-2,9007199254740993,9007199254740992.0,'1',null] AS x WITH x ORDER BY x RETURN collect(x) AS sorted",
            vec![
                json!({"sorted":[{"Utf8View":"1"},{"Int64":-2},{"Float64":-1.0},{"Int64":0},{"Float64":1.0},{"Int64":2},{"Float64":9007199254740992.0},{"Int64":9007199254740993_i64}]}),
            ],
        ),
    ] {
        let mut result = engine
            .query(Language::Cypher, query, Default::default())
            .await
            .with_context(|| query.to_owned())?;
        let actual = rows(&mut result).await.with_context(|| query.to_owned())?;
        assert_eq!(actual, expected, "{query}");
    }
    Ok(())
}

#[tokio::test]
async fn cypher_relationship_disjunction_map_projection_and_list_predicates() -> Result<()> {
    let (_dir, engine, _) = fixture()?;
    for (query, expected) in [
        (
            "MATCH (a:Account {id:1})-[r:Transfer|Invalid]->(b) RETURN type(r) AS kind, count(*) AS n ORDER BY kind",
            vec![
                json!({"kind":"Invalid","n":1}),
                json!({"kind":"Transfer","n":3}),
            ],
        ),
        (
            "MATCH p=(a:Account {id:1})-[:Transfer|Owns*2]->(d:Device) RETURN count(p) AS n",
            vec![json!({"n":3})],
        ),
        (
            "RETURN any(x IN [1,null,2] WHERE x = 2) AS yes, any(x IN [] WHERE x = 2) AS no, any(x IN [1,null] WHERE x = 2) AS unknown, any(x IN null WHERE x = 2) AS absent",
            vec![json!({"yes":true,"no":false})],
        ),
        (
            "MATCH (a:Account {id:1}) WITH a, {a:2,b:3} AS m, 4 AS extra RETURN a{.*, id:7, extra, .missing} AS node, m{.*, .b, a:9} AS map",
            vec![json!({"node":{"id":7,"extra":4},"map":{"a":9,"b":3}})],
        ),
        (
            "MATCH (a:Account) WITH collect(a) AS xs RETURN [x IN xs WHERE EXISTS { MATCH (x)-[:Transfer]->() } | x.id] AS outgoing",
            vec![json!({"outgoing":[1,2]})],
        ),
        (
            "RETURN [x IN [1,4,2] WHERE EXISTS { MATCH (a:Account) WHERE a.id = x } | x] AS present",
            vec![json!({"present":[1,2]})],
        ),
        (
            "MATCH (a:Account {id:1}) RETURN [x IN [a,null] WHERE EXISTS { MATCH (x)-[:Transfer]->() } | id(x)] AS outgoing",
            vec![json!({"outgoing":["Account[1]"]})],
        ),
        (
            "MATCH (a:Account {id:1}), (d:Device {id:'1'}) RETURN [x IN [a,d] WHERE EXISTS { MATCH (x)-[:Transfer|Peer]->() } | id(x)] AS outgoing",
            vec![json!({"outgoing":["Account[1]","Device[1]"]})],
        ),
        (
            "MATCH (a:Account), (dst:Account {id:3}) WITH dst,a ORDER BY a.id WITH dst, collect(a) AS xs RETURN [x IN xs WHERE EXISTS { MATCH (x)-[:Transfer]->(dst) WHERE x.id = 2 } | id(x)] AS outgoing",
            vec![json!({"outgoing":["Account[2]"]})],
        ),
    ] {
        let mut result = engine
            .query(Language::Cypher, query, Default::default())
            .await
            .with_context(|| query.to_owned())?;
        let actual = rows(&mut result).await.with_context(|| query.to_owned())?;
        assert_eq!(actual, expected, "{query}");
    }
    Ok(())
}

#[tokio::test]
async fn gremlin_id_filter_range_and_numeric_sum() -> Result<()> {
    let (_dir, engine, _) = fixture()?;
    for (query, expected) in [
        ("g.V().limit(1).id().count()", vec![json!(1)]),
        ("g.V().range(1,3).id().count()", vec![json!(2)]),
        (
            "g.V().hasId('Account[2]','Device[1]').id().order()",
            vec![json!("Account[2]"), json!("Device[1]")],
        ),
        (
            "g.E().hasId('Transfer[0]').id()",
            vec![json!("Transfer[0]")],
        ),
        (
            "g.V().hasLabel('Account').values('id').order().range(1,2)",
            vec![json!(2)],
        ),
        (
            "g.V().hasLabel('Account').values('id').order().range(1,-1)",
            vec![json!(2), json!(3)],
        ),
        (
            "g.V(1).hasLabel('Account').union(__.out('Transfer').values('id').order().range(1,2))",
            vec![json!(2)],
        ),
        (
            "g.V().hasLabel('Account').values('id').order().skip(1).limit(1)",
            vec![json!(2)],
        ),
        (
            "g.V(1).hasLabel('Account').union(__.out('Transfer').values('id').order().skip(2)).order()",
            vec![json!(3)],
        ),
        (
            "g.V().hasKey('missing','id').not(__.hasLabel('Account')).id().order()",
            vec![
                json!("Device[1]"),
                json!("Device[2]"),
                json!("Device[3]"),
                json!("Device[4]"),
            ],
        ),
        (
            "g.V('Account[1]').valueMap('id').unfold().hasKey('id').count()",
            vec![json!(1)],
        ),
        (
            "g.V('Account[1]','Account[3]').not(__.out('Transfer').path()).id()",
            vec![json!("Account[3]")],
        ),
        (
            "g.V('Account[3]').not(__.out('Transfer').count()).count()",
            vec![json!(0)],
        ),
        (
            "g.V().hasLabel('Account').values('id').sum()",
            vec![json!(6)],
        ),
        (
            "g.V(99).hasLabel('Account').values('id').sum()",
            vec![json!(0)],
        ),
        (
            "g.V(1).hasLabel('Account').union(__.out('Transfer').values('id').sum())",
            vec![json!(7)],
        ),
        (
            "g.V(1).hasLabel('Account').union(__.out('Transfer').values('id'),__.outE('Transfer').values('weight')).sum()",
            vec![json!(15.0)],
        ),
    ] {
        let mut result = engine
            .query(Language::Gremlin, query, Default::default())
            .await
            .with_context(|| query.to_owned())?;
        let actual = rows(&mut result).await.with_context(|| query.to_owned())?;
        assert_eq!(
            actual,
            expected
                .into_iter()
                .map(|value| json!({"value":value}))
                .collect::<Vec<_>>(),
            "{query}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn cypher_mixed_numeric_aggregates_and_projected_maps() -> Result<()> {
    let (_dir, engine, _) = fixture()?;
    for (query, expected) in [
        (
            "WITH 4 AS x RETURN reduce(s=x, x IN [1,2,3] | s*10+x) AS digits, reduce(s=7,x IN [] | s+x) AS empty, reduce(s=0,x IN null | s+x) AS absent",
            vec![json!({"digits":4123,"empty":7})],
        ),
        (
            "UNWIND [3,1,2] AS n WITH n ORDER BY n DESC RETURN reduce(s=0,x IN collect(n)|s*10+x) AS digits, reduce(s=null,x IN [1,2]|coalesce(s,0)+x) AS total",
            vec![json!({"digits":321,"total":3})],
        ),
        (
            "RETURN ReDuCe /* fold */ (`a,b`=0, x IN [1,2] | reduce(t=`a,b`, y IN [x,x+1] | t+y)) AS nested",
            vec![json!({"nested":8})],
        ),
        (
            "RETURN reduce(s=[],x IN [1,'2',3] | s+[x]) AS xs, reduce(s=true,x IN [true,null,false] | s AND x) AS all_values, reduce(s=0,x IN [3,1,2] | CASE WHEN x>s THEN x ELSE s END) AS maximum",
            vec![
                json!({"xs":[{"Int64":1},{"Utf8View":"2"},{"Int64":3}],"all_values":false,"maximum":3}),
            ],
        ),
        (
            "UNWIND [true,false] AS pick WITH pick, CASE WHEN pick THEN [1] ELSE [2.0] END AS xs UNWIND xs AS x RETURN size(xs) AS n, x AS v ORDER BY pick DESC",
            vec![
                json!({"n":1,"v":{"Int64":1}}),
                json!({"n":1,"v":{"Float64":2.0}}),
            ],
        ),
        (
            "RETURN size(coalesce([1],[2.0])) AS n, [x IN coalesce([1],[2.0]) | x + 1] AS added",
            vec![json!({"n":1,"added":[{"Int64":2}]})],
        ),
        (
            "RETURN coalesce('1',2) AS text, coalesce(null,2,'1') AS number, [0,2] < [0,3.0] AS less, [1,null] <= [1,2] AS unknown, [1] < [1,2] AS prefix",
            vec![json!({"text":{"Utf8View":"1"},"number":{"Int64":2},"less":true,"prefix":true})],
        ),
        (
            "UNWIND [9007199254740993,1.0,null] AS x RETURN x+1 AS v ORDER BY v",
            vec![
                json!({"v":{"Float64":2.0}}),
                json!({"v":{"Int64":9007199254740994_i64}}),
                json!({}),
            ],
        ),
        (
            "UNWIND [0,1.0,2,null] AS x RETURN min(x) AS lo,max(x) AS hi,sum(x) AS total,avg(x) AS average",
            vec![json!({"lo":{"Int64":0},"hi":{"Int64":2},"total":3.0,"average":1.0})],
        ),
        (
            "UNWIND [9007199254740993,9007199254740992.0] AS x RETURN max(x) AS high",
            vec![json!({"high":{"Int64":9007199254740993_i64}})],
        ),
        (
            "UNWIND [0,9007199254740993,9007199254740992.0,9007199254740992] AS x RETURN sum(DISTINCT x) AS total,avg(DISTINCT x) AS average",
            vec![json!({"total":18014398509481984.0,"average":6004799503160661.0})],
        ),
        (
            "UNWIND [true,false] AS pick WITH pick,CASE WHEN pick THEN {a:1} ELSE {b:2} END AS m RETURN m.a AS field,m{.a} AS projected,keys(m{.*}) AS fields ORDER BY pick DESC",
            vec![
                json!({"field":1,"projected":{"a":1},"fields":["a"]}),
                json!({"projected":{},"fields":["b"]}),
            ],
        ),
        (
            "RETURN all(x IN [1,2] WHERE x > 0) AS yes,all(x IN [] WHERE x>0) AS empty,all(x IN [1,null] WHERE x > 0) AS unknown,none(x IN [0,null] WHERE x > 0) AS maybe,none(x IN [1,null] WHERE x > 0) AS no",
            vec![json!({"yes":true,"empty":true,"no":false})],
        ),
        (
            "WITH 5 AS hi UNWIND range(hi,1,-2) AS n RETURN n ORDER BY n DESC",
            vec![json!({"n":5}), json!({"n":3}), json!({"n":1})],
        ),
        (
            "MATCH (n:Account|Device) WHERE id(n) = 'Device[1]' RETURN labels(n) AS labels, keys(n) AS keys, keys({missing:null}) AS map_keys",
            vec![json!({"labels":["Device"],"keys":["id"],"map_keys":["missing"]})],
        ),
        (
            "CALL { MATCH (a:Account {id:1}) RETURN a AS value UNION ALL RETURN null AS value } RETURN value ORDER BY value IS NULL",
            vec![json!({"value":{"id":1}}), json!({})],
        ),
        (
            "MATCH (a:Account {id:1}) RETURN EXISTS { MATCH (a)-[r:Transfer]->(b) MATCH (a)-[s:Transfer]->(b) WHERE r = s } AS same, EXISTS { MATCH (a)-[:Transfer]->(b) MATCH (b)-[:Owns]->() } AS owns",
            vec![json!({"same":true,"owns":true})],
        ),
        (
            "MATCH (a:Account) WITH a ORDER BY a.id WITH collect(a) AS xs RETURN [x IN xs WHERE EXISTS { MATCH (x)-[:Transfer]->(b) MATCH (b)-[:Owns]->(d) WHERE d.id = '1' } | x.id] AS found",
            vec![json!({"found":[1,2]})],
        ),
    ] {
        let mut result = engine
            .query(Language::Cypher, query, Default::default())
            .await
            .with_context(|| query.to_owned())?;
        let actual = rows(&mut result).await.with_context(|| query.to_owned())?;
        assert_eq!(actual, expected, "{query}");
    }
    Ok(())
}

#[tokio::test]
async fn cypher_path_lists_bind_entities_in_projections_and_unwind() -> Result<()> {
    let (_dir, engine, _) = fixture()?;
    for (query, expected) in [
        (
            "MATCH p=(a:Account {id:1})-[:Transfer]->(b:Account {id:2}) RETURN reduce(s=0.0,e IN relationships(p)|s+e.weight) AS cost ORDER BY cost",
            vec![json!({"cost":1.0}), json!({"cost":2.0})],
        ),
        (
            "MATCH p=(a:Account {id:2})-[:Owns]->(d:Device) RETURN reduce(s='',n IN nodes(p)|s+id(n)) AS ids",
            vec![json!({"ids":"Account[2]Device[1]"})],
        ),
        (
            "MATCH p=(a:Account {id:2})-[:Owns]->(d:Device) WITH collect(nodes(p)) AS paths RETURN [ns IN paths | [n IN ns | id(n)]] AS ids, [ns IN paths | [n IN ns | n.id]] AS keys",
            vec![
                json!({"ids":[["Account[2]","Device[1]"]],"keys":[[{"Int64":2},{"Utf8View":"1"}]]}),
            ],
        ),
        (
            "MATCH p=(a:Account {id:2})-[:Owns]->(d:Device) WITH collect(p) AS paths RETURN [p IN paths | [n IN nodes(p) | id(n)]] AS ids",
            vec![json!({"ids":[["Account[2]","Device[1]"]]})],
        ),
        (
            "MATCH (a:Account {id:2})-[:Owns]->(d:Device) UNWIND [true,false] AS pick WITH pick, CASE WHEN pick THEN [a] ELSE [d] END AS ns UNWIND ns AS n RETURN id(n) AS id,n.id AS key ORDER BY pick DESC",
            vec![
                json!({"id":"Account[2]","key":{"Int64":2}}),
                json!({"id":"Device[1]","key":{"Utf8View":"1"}}),
            ],
        ),
        (
            "MATCH p=(a:Account {id:2})-[:Owns]->(d:Device) RETURN nodes(p)=[a,d] AS equal, [a,d] IN [nodes(p)] AS member",
            vec![json!({"equal":true,"member":true})],
        ),
        (
            "MATCH p=(a:Account {id:2})-[:Owns]->(d:Device) WITH collect(nodes(p)) AS paths UNWIND paths AS ns UNWIND ns AS n RETURN id(n) AS id, n.id AS key ORDER BY id",
            vec![
                json!({"id":"Account[2]","key":{"Int64":2}}),
                json!({"id":"Device[1]","key":{"Utf8View":"1"}}),
            ],
        ),
        (
            "MATCH p=(a:Account {id:2})-[:Owns]->(d:Device) UNWIND coalesce(nodes(p),[]) AS n RETURN id(n) AS id ORDER BY id",
            vec![json!({"id":"Account[2]"}), json!({"id":"Device[1]"})],
        ),
        (
            "MATCH p=(a:Account {id:2})-[:Owns]->(d:Device) RETURN [i IN [0,1] | [n IN nodes(p)[i..] | id(n)]] AS paths",
            vec![json!({"paths":[["Account[2]","Device[1]"],["Device[1]"]]})],
        ),
        (
            "MATCH p=(a:Account {id:2})-[:Owns]->(b:Device) WITH nodes(p) AS ns UNWIND ns AS n RETURN id(n) AS id, n.id AS key ORDER BY id",
            vec![
                json!({"id":"Account[2]","key":{"Int64":2}}),
                json!({"id":"Device[1]","key":{"Utf8View":"1"}}),
            ],
        ),
        (
            "MATCH p=(a:Account {id:1})-[:Transfer]->(b:Account {id:2}) WITH relationships(p) AS es RETURN [e IN es | e.weight] AS weights, type(es[0]) AS label ORDER BY weights",
            vec![
                json!({"weights":[1.0],"label":"Transfer"}),
                json!({"weights":[2.0],"label":"Transfer"}),
            ],
        ),
        (
            "MATCH p=(a:Account {id:1})-[:Transfer|Owns*2]->(b:Device) RETURN [n IN nodes(p) | id(n)] AS path ORDER BY path",
            vec![
                json!({"path":["Account[1]","Account[2]","Device[1]"]}),
                json!({"path":["Account[1]","Account[2]","Device[1]"]}),
                json!({"path":["Account[1]","Account[3]","Device[1]"]}),
            ],
        ),
        (
            "CALL { MATCH p=(a:Account {id:1})-[:Transfer]->(b:Account {id:3}) RETURN p UNION ALL MATCH p=(a:Device {id:'1'})-[:Peer]->(b:Device {id:'2'}) RETURN p } UNWIND nodes(p) AS n RETURN id(n) AS id ORDER BY id",
            vec![
                json!({"id":"Account[1]"}),
                json!({"id":"Account[3]"}),
                json!({"id":"Device[1]"}),
                json!({"id":"Device[2]"}),
            ],
        ),
    ] {
        let mut result = engine
            .query(Language::Cypher, query, Default::default())
            .await
            .with_context(|| query.to_owned())?;
        assert_eq!(
            rows(&mut result).await.with_context(|| query.to_owned())?,
            expected,
            "{query}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn cypher_correlated_subqueries_preserve_outer_rows() -> Result<()> {
    let (_dir, engine, _) = fixture()?;
    for (query, expected) in [
        (
            "UNWIND [0,2,3,null] AS cutoff MATCH (a:Account {id:1}) CALL (a,cutoff) { MATCH (a)-[r:Transfer]->(b) WITH b,r WHERE b.id>cutoff RETURN b.id AS x ORDER BY r.weight DESC LIMIT 1 } RETURN cutoff,x ORDER BY cutoff",
            vec![json!({"cutoff":0,"x":3}), json!({"cutoff":2,"x":3})],
        ),
        (
            "UNWIND [1,1,2,3] AS id MATCH (a:Account) WHERE a.id=id CALL (a) { OPTIONAL MATCH (a)-[:Transfer]->(b) RETURN b.id AS x,count(b) AS c ORDER BY c DESC,x LIMIT 1 } RETURN id,x,c ORDER BY id",
            vec![
                json!({"id":1,"x":2,"c":2}),
                json!({"id":1,"x":2,"c":2}),
                json!({"id":2,"x":1,"c":1}),
                json!({"id":3,"c":0}),
            ],
        ),
        (
            "UNWIND [1,0] AS x CALL (x) { RETURN 1/x AS v UNION ALL RETURN 1/x AS v } RETURN v SKIP 1 LIMIT 1",
            vec![json!({"v":1})],
        ),
        (
            "UNWIND [1,0] AS x CALL (x) { WITH x RETURN 1/x AS v ORDER BY v LIMIT 1 } RETURN v LIMIT 1",
            vec![json!({"v":1})],
        ),
        (
            "UNWIND [1,2] AS x CALL { WITH x RETURN x AS v UNION ALL RETURN 0 AS v } RETURN x,v ORDER BY x,v",
            vec![
                json!({"x":1,"v":0}),
                json!({"x":1,"v":1}),
                json!({"x":2,"v":0}),
                json!({"x":2,"v":2}),
            ],
        ),
        (
            "UNWIND [1,2] AS x CALL (x) { CALL () { RETURN 10 AS add } RETURN x+add AS v } RETURN x,v ORDER BY x",
            vec![json!({"x":1,"v":11}), json!({"x":2,"v":12})],
        ),
        (
            "MATCH (a:Account {id:1}) UNWIND [9,1,2] AS cutoff WITH a,cutoff ORDER BY cutoff DESC CALL (a,cutoff) { MATCH (a)-[r:Transfer]->(b) WITH r,b WHERE b.id>cutoff RETURN r.weight AS w ORDER BY w DESC LIMIT 2 } RETURN cutoff,w SKIP 1 LIMIT 2",
            vec![json!({"cutoff":1,"w":5.0}), json!({"cutoff":1,"w":2.0})],
        ),
        (
            "MATCH (a:Account {id:1}) RETURN [x IN [0,1] | [y IN [1,2,3] WHERE EXISTS { MATCH (a)-[:Transfer]->(b) WHERE b.id=x+y } | y]] AS found",
            vec![json!({"found":[[2,3],[1,2]]})],
        ),
        (
            "MATCH (a:Account {id:1}) RETURN [x IN [1,2,3] | [x IN range(x,x) WHERE EXISTS { MATCH (a)-[:Transfer]->(b) WHERE b.id=x } | x]] AS found",
            vec![json!({"found":[[],[2],[3]]})],
        ),
        (
            "MATCH (a:Account) WITH a ORDER BY a.id WITH collect(a) AS xs RETURN [x IN xs | [y IN range(x.id,x.id+1) WHERE EXISTS { MATCH (x)-[:Transfer]->(b) WHERE b.id=y } | y]] AS found",
            vec![json!({"found":[[2],[3],[]]})],
        ),
        (
            "UNWIND [1,1,2,3] AS id MATCH (a:Account) WHERE a.id=id CALL (a) { MATCH (a)-[r:Transfer]->(b) RETURN r.weight AS w ORDER BY w DESC SKIP 1 LIMIT 1 } RETURN id,w ORDER BY id,w",
            vec![
                json!({"id":1,"w":2.0}),
                json!({"id":1,"w":2.0}),
                json!({"id":2,"w":0.5}),
            ],
        ),
        (
            "MATCH (a:Account {id:1}) CALL { WITH a MATCH (a)-[r:Transfer]->(b) RETURN r.weight AS w ORDER BY w DESC LIMIT 2 } RETURN collect(w) AS weights",
            vec![json!({"weights":[5.0,2.0]})],
        ),
        (
            "RETURN [x IN [1,1.0,null] WHERE EXISTS { MATCH (a:Account {id:1}) WHERE x / 2 > 0.25 OR x IS NULL } | x] AS found",
            vec![json!({"found":[{"Float64":1.0},null]})],
        ),
        (
            "UNWIND [1,-1] AS cutoff RETURN cutoff, EXISTS { MATCH ()-[:Transfer]->() WHERE cutoff > 0 } AS found ORDER BY cutoff",
            vec![
                json!({"cutoff":-1,"found":false}),
                json!({"cutoff":1,"found":true}),
            ],
        ),
        (
            "MATCH (a:Account) UNWIND [1,2,3] AS cutoff WITH a, cutoff WHERE EXISTS { MATCH (a)-[:Transfer]->(b) MATCH (b)-[:Owns]->(d) WHERE b.id > cutoff } RETURN a.id AS id, cutoff ORDER BY id, cutoff",
            vec![
                json!({"id":1,"cutoff":1}),
                json!({"id":1,"cutoff":2}),
                json!({"id":2,"cutoff":1}),
                json!({"id":2,"cutoff":2}),
            ],
        ),
        (
            "UNWIND [1,1,2,3] AS id MATCH (a:Account) WHERE a.id=id CALL (a) { MATCH (a)-[:Transfer]->(b) RETURN count(*) AS cnt, count(DISTINCT b) AS distinct_cnt, sum(b.id) AS total, collect(b.id) AS ids } RETURN id,cnt,distinct_cnt,total,size(ids) AS size ORDER BY id",
            vec![
                json!({"id":1,"cnt":3,"distinct_cnt":2,"total":7,"size":3}),
                json!({"id":1,"cnt":3,"distinct_cnt":2,"total":7,"size":3}),
                json!({"id":2,"cnt":2,"distinct_cnt":2,"total":4,"size":2}),
                json!({"id":3,"cnt":0,"distinct_cnt":0,"total":0,"size":0}),
            ],
        ),
        (
            "UNWIND [1,2,3,4] AS id OPTIONAL MATCH (a:Account) WHERE a.id=id CALL { WITH a MATCH (a)-[:Transfer]->(b) RETURN count(b) AS cnt, min(b.id) AS low } RETURN id,cnt,low ORDER BY id",
            vec![
                json!({"id":1,"cnt":3,"low":2}),
                json!({"id":2,"cnt":2,"low":1}),
                json!({"id":3,"cnt":0}),
                json!({"id":4,"cnt":0}),
            ],
        ),
        (
            "UNWIND [1,1,3] AS id MATCH (a:Account) WHERE a.id=id CALL (a) { MATCH (a)-[r:Transfer]->(b:Account {id:2}) RETURN r.weight AS w } RETURN id,w ORDER BY id,w",
            vec![
                json!({"id":1,"w":1.0}),
                json!({"id":1,"w":1.0}),
                json!({"id":1,"w":2.0}),
                json!({"id":1,"w":2.0}),
            ],
        ),
        (
            "MATCH (a:Account {id:3}) CALL (a) { MATCH (a)-[:Transfer]->(b) RETURN sum(1/(b.id-3)) AS v } RETURN v",
            vec![json!({"v":0})],
        ),
    ] {
        let mut result = engine
            .query(Language::Cypher, query, Default::default())
            .await
            .with_context(|| query.to_owned())?;
        assert_eq!(
            rows(&mut result).await.with_context(|| query.to_owned())?,
            expected,
            "{query}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn cypher_correlated_call_spills_wide_results_and_honors_outer_limit() -> Result<()> {
    let (dir, _, _) = fixture()?;
    let payload = "x".repeat(256 * 1024);
    let batch = RecordBatch::try_from_iter([
        ("id", Arc::new(Int64Array::from(vec![1, 2, 3])) as _),
        (
            "payload",
            Arc::new(StringArray::from(vec![None, Some(payload.as_str()), None])) as _,
        ),
    ])?;
    std::fs::write(dir.path().join("accounts.parquet"), parquet(&batch)?)?;
    let mut catalog = Catalog::load(dir.path().join("graph.yaml"))?;
    catalog.nodes.get_mut("Account").unwrap().properties = vec!["payload".into()];
    let engine = Engine::new(catalog)?;
    let options = QueryOptions {
        memory_bytes: 16 * 1024 * 1024,
        spill_bytes: 128 * 1024 * 1024,
        ..Default::default()
    };
    for projection in ["WITH b", "WITH DISTINCT b"] {
        let query = format!(
            "MATCH (a:Account {{id:1}}) UNWIND range(1,128) AS k
             CALL (a) {{ MATCH (a)-[:Transfer]->(b) {projection}
                          RETURN b.payload AS payload ORDER BY b.id LIMIT 1 }}
             RETURN payload IS NOT NULL AS ok"
        );
        let mut result = engine
            .query(
                Language::Cypher,
                &format!("{query} LIMIT 128"),
                options.clone(),
            )
            .await?;
        assert_eq!(rows(&mut result).await?, vec![json!({"ok":true}); 128]);
        assert!(result.metrics().reads.snapshot_spilled_bytes > 0);

        let mut result = engine
            .query(
                Language::Cypher,
                &format!("{query} LIMIT 1"),
                QueryOptions {
                    spill_bytes: 0,
                    ..options.clone()
                },
            )
            .await?;
        assert_eq!(rows(&mut result).await?, [json!({"ok":true})]);
    }
    let batch = RecordBatch::try_from_iter([
        ("id", Arc::new(Int64Array::from(vec![1, 2, 3])) as _),
        (
            "payload",
            Arc::new(StringArray::from(vec!["head", payload.as_str(), "tail"])) as _,
        ),
    ])?;
    std::fs::write(dir.path().join("accounts.parquet"), parquet(&batch)?)?;
    let mut result = engine
        .query(
            Language::Cypher,
            "MATCH (b:Account) UNWIND range(1,24) AS k
             WITH b.payload AS p ORDER BY b.id,k DESC
             WITH collect(p) AS ps RETURN size(ps) AS n,ps[0] AS first,ps[-1] AS last",
            QueryOptions {
                memory_bytes: 32 * 1024 * 1024,
                ..options
            },
        )
        .await?;
    assert_eq!(
        rows(&mut result).await?,
        [json!({"n":72,"first":"head","last":"tail"})]
    );
    assert!(result.metrics().spilled_bytes > 0);
    let mut result = engine
        .query(
            Language::Cypher,
            "MATCH (b:Account) UNWIND range(1,24) AS k
             WITH b.payload AS p ORDER BY b.id DESC,k
             RETURN collect(DISTINCT p) AS ps",
            QueryOptions {
                memory_bytes: 8 * 1024 * 1024,
                spill_bytes: 128 * 1024 * 1024,
                batch_size: 8,
                ..Default::default()
            },
        )
        .await?;
    assert_eq!(
        rows(&mut result).await?,
        [json!({"ps":["tail",payload,"head"]})]
    );
    assert!(result.metrics().spilled_bytes > 0);
    Ok(())
}

#[tokio::test]
async fn gremlin_typed_comparison_order_and_traversal_steps() -> Result<()> {
    let (_dir, engine, _) = fixture()?;
    for (query, expected) in [
        (
            "g.V('Account[2]').outE('Owns').as('r').select('r').otherV().id()",
            vec![json!("Device[1]")],
        ),
        (
            "g.V('Account[2]').out().order().by('id',desc).limit(1).id()",
            vec![json!("Device[1]")],
        ),
        ("g.V().has('id',1).id().order()", vec![json!("Account[1]")]),
        (
            "g.V().values('id').is(within(1,'2')).count()",
            vec![json!(2)],
        ),
        (
            "g.V('Account[1]').union(__.out('Transfer').values('id'),__.outE('Transfer').values('weight')).order().math('_')",
            vec![
                json!(1.0),
                json!(2.0),
                json!(2.0),
                json!(2.0),
                json!(3.0),
                json!(5.0),
            ],
        ),
        (
            "g.V('Account[1]').union(__.out('Transfer').values('id'),__.outE('Transfer').values('weight')).min().math('_')",
            vec![json!(1.0)],
        ),
        (
            "g.V('Account[1]').union(__.out('Transfer').values('id'),__.outE('Transfer').values('weight')).max().math('_')",
            vec![json!(5.0)],
        ),
        ("g.V('Account[99]').values('id').min()", vec![]),
        (
            "g.E('Owns[0]').bothV().id().order()",
            vec![json!("Account[2]"), json!("Device[1]")],
        ),
        (
            "g.V('Device[1]').inE('Owns').otherV().id().order()",
            vec![json!("Account[2]"), json!("Account[3]")],
        ),
        (
            "g.V('Account[2]').outE('Owns').otherV().id()",
            vec![json!("Device[1]")],
        ),
        ("g.V().filter(has('id',1)).id()", vec![json!("Account[1]")]),
        (
            "g.V('Account[1]','Account[2]').map(out('Transfer').values('id').order()).order()",
            vec![json!(1), json!(2)],
        ),
        (
            "g.V('Account[2]').math('(_ + 1)^2').by('id')",
            vec![json!(9.0)],
        ),
        (
            "g.V('Account[2]').as('a').out('Owns').as('b').math('a + b').by('id').by(count())",
            vec![json!(3.0)],
        ),
    ] {
        let mut result = engine
            .query(Language::Gremlin, query, Default::default())
            .await
            .with_context(|| query.to_owned())?;
        let actual = rows(&mut result).await.with_context(|| query.to_owned())?;
        let expected = expected
            .into_iter()
            .map(|value| json!({"value":value}))
            .collect::<Vec<_>>();
        assert_eq!(actual, expected, "{query}");
    }
    Ok(())
}
