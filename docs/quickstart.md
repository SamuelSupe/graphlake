# GraphLake quickstart

Use the built-in Parquet demo to try GraphLake without provisioning a database. The demo generates three accounts and four transfers in a new directory.

## Build and query the demo

Install Rust 1.95 or newer and a C/C++ toolchain. For the browser workbench, also install Node.js 24. Run these commands from the repository root.

```sh
demo_root=$(mktemp -d)
demo_dir="$demo_root/catalog"
cargo run --locked --example demo -- "$demo_dir"

cargo run --locked -- --catalog "$demo_dir/graph.yaml" query \
  'MATCH (a:Account)-[:Transfer]->(b:Account)-[t:Transfer]->(c:Account)
   WHERE a.tenant_id = 1 AND a.account_id = 10
   RETURN c.account_id AS account, count(*) AS paths, sum(t.amount) AS amount'
```

Expected output:

```json
{"account":30,"paths":2,"amount":600}
```

The generated [mapping template](../examples/finance.yaml) illustrates how table columns become node keys, properties, and relationship endpoints. The demo writes its own Parquet-based mapping to `graph.yaml`.

## Open the workbench

```sh
npm --prefix web ci
npm --prefix web run build

cargo run --locked --features admin --bin graphlake-admin -- \
  --catalog-dir "$demo_dir" auth init

cargo run --locked --features admin --bin graphlake-admin -- \
  --catalog-dir "$demo_dir" --assets-dir web/dist
```

Choose a password when prompted. Open [http://127.0.0.1:7337](http://127.0.0.1:7337/) and sign in as `admin`. The valid model is adopted on startup.

1. Open **Graph Models** (图模型) and select `graph` to inspect the mapping.
2. Open **Query** (查询), choose Cypher, and run the example above.
3. To explore graph objects, run the following query, then open the **Graph** (图) result tab and select a node or a relationship.

```cypher
MATCH p = (:Account)-[:Transfer]->(:Account)
RETURN p
```

Use [Deployment](deployment.md) to move from a local demo to a Linux service.
