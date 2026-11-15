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

To add another source, open **Data Sources** (数据源), choose a connector card, and fill in its connection settings or file/table mapping. Click **Add to draft** (添加到草稿), then save, validate, and publish. Filter connector cards by category or keyword, and search existing sources by name or type. Existing source cards offer **Configure** (配置), with saving and status feedback inside the dialog, and **Browse tables and columns** (浏览表与字段). Closing configuration keeps unsaved edits in the current draft; cancelling a new source leaves the model unchanged. Credentials are referenced by server environment variable names.

Use [Deployment](deployment.md) to move from a local demo to a Linux service.

## Query CSV and JSON Lines

The checked-in [text-file example](../examples/text/graph.yaml) needs no external service:

```sh
cargo run --locked -- --catalog examples/text/graph.yaml query \
  'MATCH (a:Account)-[t:Transfer]->(b:Account)
   RETURN a.name AS sender, b.name AS recipient, t.amount AS amount ORDER BY sender'
```

It returns Alice → Bob for `12.25` and Bob → Carol for `25.50`. Columns have explicit Arrow types; CSV column names and order must match the file header. See the [connector reference](reference.zh-CN.md#图映射与连接器) for SQLite, HTTP/S3 locations, and supported types.


### 扩展数据源

在数据源卡片中可直接选择 Doris、StarRocks、TiDB、OceanBase（MySQL 模式）、MariaDB、SQL Server（直连）、Oracle（直连）、MongoDB（直连）、DuckDB 和 Databricks SQL。SQL Server、Oracle、MongoDB 的 Trino 卡片保留，用于已有 Trino 部署。

对象存储卡片支持 S3、MinIO、OSS、TOS、COS、Azure Blob 和 GCS。选择服务和文件格式，填写桶或容器以及凭据环境变量，再添加表映射；展开「浏览桶内对象」，按前缀读取列表并把文件加入映射。Parquet 自动读取列类型，CSV/JSON Lines 需声明列。配置使用服务端路径和服务端环境变量。Oracle 直连还需服务端安装 Oracle Instant Client。

参见[连接器读取边界](reference.zh-CN.md#扩展数据库与对象存储)与[扩展配置示例](../examples/extended-sources.yaml)。Kafka 和 CDC 不在此轮接入范围。
