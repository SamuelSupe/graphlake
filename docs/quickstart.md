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

The model list supports name/ID search and **All**, **Published**, **Unpublished**, and **Needs attention** filters. Source, node, and edge counts share one structure column. Publication status and draft validation are shown separately: a failed draft check does not mean the active version is unpublished. **Needs attention** includes failed draft checks and version storage errors. Use a model's **Query** action to open its published version directly (requires query permission); models with a version storage error do not offer this action. **Clear filters** restores the complete list.

Opening a model and returning preserves each workspace's search, filter, and scroll position for the current page session. In the model toolbar, **More tools** (更多工具) contains manual validation, version history, and YAML. New, unpublished models show four steps: add a source, define nodes and optional edges, save the draft, then validate and publish. Missing source tables, node keys, or edge endpoints lead back to the relevant configuration before saving.

The query editor highlights Cypher, SQL, and Gremlin and completes labels, relationship types, keys, and properties from the published model. Use **Complete** (补全) or Ctrl+Space for suggestions, Cmd/Ctrl+Enter to run, and Tab to leave the editor. Each language keeps its own query text. **Query options** (查询选项) groups Cypher parameters, timeout, full-result capture, and available CSR selection. It indicates configured values and opens when required parameters are missing or timeout is invalid. Loading history or a template returns focus to the editor, or to the parameter field when values are required. Query scope remains a separate panel; templates and history sit together below the results. In **Version history**, **View and compare** shows a model change summary and highlighted YAML additions/deletions; expand the full YAML comparison when needed.

Administrators can open **Runtime health** (运行健康) from the model list or **Runtime configuration → Health checks**. Check all published models or one model, inspect source and Worker results, and follow the configuration action for a failure. Results include a check time and are refreshed on demand; an unchecked model is not reported as healthy. Draft validation is displayed separately. Status and actions remain visible; failures show a next step, while source details expand on demand.

Creation, editing, source browsing, query scope, YAML, and version history open in right-side panels. The heading and action bar stay visible while the content scrolls. Source browsing separates table selection from field/type/nullability preview; version history separates the release list from the selected comparison. Panels stack their sections on small screens. Short confirmations, such as taking a model offline, remain centered. Runtime health gives model status the main column and telemetry a secondary column.

```cypher
MATCH p = (:Account)-[:Transfer]->(:Account)
RETURN p
```

Use [Deployment](deployment.md) to move from a local demo to a Linux service.
