# GraphLake

**Read-only graph analytics across databases and lakehouse tables.**

Build a labeled graph over existing tables, query it with Cypher, SQL, or Gremlin, and explore the results in a browser. GraphLake runs as an embedded Rust engine, a standalone Linux service, or a Coordinator with distributed Workers.

[**Download v1.0.0**](https://github.com/SamuelSupe/graphlake/releases/tag/v1.0.0) · [Quickstart](docs/quickstart.md) · [Deployment](docs/deployment.md) · [中文技术参考](docs/reference.zh-CN.md)

![GraphLake query workbench showing three accounts and four transfers from the Parquet demo](docs/workbench.png)

## What you can do

| | |
| --- | --- |
| **Model** | Map nodes and relationships to one or more tables, with composite keys, property mappings, filters, business-time rules, and validation. Publish a draft or roll back to an archived version. |
| **Query** | Use Cypher, SQL, or Gremlin. Set a timeout, inspect the model and source versions, cancel a query, or export complete results as Arrow IPC. |
| **Explore** | Inspect node and relationship properties, expand and page through neighbors, search across batches, and continue loading a truncated graph preview. |
| **Analyze** | Run bounded traversals, shortest paths, PageRank, weakly connected components, community detection, and Node2Vec. Reuse persisted CSR indexes for supported workloads. |
| **Operate** | Manage users, service accounts, and model-level roles. Collect audit events, Prometheus metrics, and OTLP signals. Restore an off-site checkpoint and verify a known business query before enabling traffic. |

## Install on Linux

The GitHub release ships **ARM64 (`aarch64`)** and **AMD64 (`x86_64`)** packages for Debian 12 and compatible glibc-based Linux systems. Each package contains `graphlake`, `graphlake-admin`, the compiled browser UI, and deployment assets for systemd and HAProxy.

1. Run `uname -m` and download the matching `.tar.gz` and `.tar.gz.sha256` from the release page.
2. Verify the checksum and extract the package. Each archive contains a `BUILD-INFO` file with its commit, source digest, and target architecture.
3. Use the [standalone deployment guide](docs/deployment.md#standalone-service) for installation, administrator setup, and the first health check.

For local development and a runnable example, follow the [quickstart](docs/quickstart.md). It builds a graph from two small Parquet files and runs this query through the CLI and the browser:

```cypher
MATCH (a:Account)-[:Transfer]->(b:Account)-[t:Transfer]->(c:Account)
WHERE a.tenant_id = 1 AND a.account_id = 10
RETURN c.account_id AS account, count(*) AS paths, sum(t.amount) AS amount
```

The example returns account `30`, two paths, and a total amount of `600`.

## Data sources and interfaces

- **Direct connectors** — PostgreSQL, MySQL, Parquet, Iceberg, Delta Lake, Snowflake, BigQuery, ClickHouse, Trino, Spanner, and SAP HANA.
- **Through Trino** — SQL Server, Oracle, Hive, Hudi, MongoDB, Elasticsearch, and Redshift. The corresponding Trino catalog supplies access to the source.
- **Clients** — Browser and HTTP API, Bolt and Gremlin WebSocket, command line, embedded Rust, and source-built Python and C bindings. Explicit Bolt transactions are available for PostgreSQL and Parquet.

See the [technical reference](docs/reference.zh-CN.md#图映射与连接器) for source configuration, query semantics, the API, and the bindings.

## Deployment modes

```mermaid
flowchart LR
    Client["Browser · HTTP · Bolt · Gremlin"]
    Proxy["HTTPS / TLS ingress"]
    Control["Coordinator"]
    Workers["Query Workers"]
    Sources["Databases and lakehouse tables"]
    State["etcd and S3-compatible storage"]

    Client --> Proxy --> Control
    Control --> Workers --> Sources
    Control <--> State
    Workers <--> State
```

Use one `graphlake-admin` process for a standalone deployment. A distributed deployment adds Workers for supported scans, joins, aggregations, and graph operations. Coordinator failover uses etcd for leadership and S3-compatible storage for published control state. Both modes serve read-only queries over data owned by the underlying sources.

## Documentation

| Guide | Covers |
| --- | --- |
| [Quickstart](docs/quickstart.md) | Build from source, run the Parquet demo, and open the browser workbench |
| [Deployment](docs/deployment.md) | Linux installation, TLS, upgrade and rollback, monitoring, and recovery |
| [Cluster reference](deploy/cluster.md) | Coordinator and Worker configuration, distributed execution, failover, and recovery manifests |
| [Monitoring reference](deploy/monitoring.md) | Service, source, ingress, telemetry, and backup probes |
| [Technical reference](docs/reference.zh-CN.md) | Modeling, connectors, query behavior, algorithms, APIs, and bindings |
| [Changelog](CHANGELOG.md) | The v1.0.0 release baseline and runtime boundaries |
