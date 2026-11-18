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

Select the GraphLake logo in the console navigation to return to the graph model list.

The console opens in English by default. Choose **English** or **简体中文** on the sign-in page or in the main navigation. On small screens, tap the navigation button beside GraphLake to find the language selector and workspace links. GraphLake reloads the current page and remembers the language in this browser. If your current workspace has unsaved changes, the browser prompts you before leaving.

1. Open **Graph Models** (图模型) and select `graph` to inspect the mapping.
2. Open **Query** (查询), choose Cypher, and run the example above.
3. To explore graph objects, run the following query, then open the **Graph** (图) result tab and select a node or a relationship.

In a wide Query workspace, the editor and saved queries sit to the left of the results. Narrower layouts place the results below the editor.

The model library opens in card view. On narrow screens, the status selector sits beside the search field. Select **List view** to compare models in a table. The four totals summarize accessible models and their source, node-label, and edge-type definitions across models. They stay visible while you filter the library. The model list supports name/ID search and **All**, **Published**, **Unpublished**, and **Needs attention** filters. In list view, source, node-label, and edge-type counts share one structure column. Publication status and draft validation are shown separately: a failed draft check does not mean the active version is unpublished. **Needs attention** includes failed draft checks and version storage errors. Use a model's **Query** action to open its published version directly (requires query permission); models with a version storage error do not offer this action. **Clear filters** restores the complete list.

Use **Back to model list** to return to the current workspace’s directory. Search, filters, view, and scroll position are preserved for the current page session. In the model toolbar, **More tools** (更多工具) opens a popover for manual validation, business task setup, and version history. Select an action, click outside, or press Escape to dismiss it. For an unpublished model, the progress control shows the current step. Expand it to see all four steps: add a source, define nodes and optional edges, save the draft, then preview and publish. Missing source tables, node keys, or edge endpoints lead back to the relevant configuration before saving.

Use the navigation row above the model name to return to the directory or switch models in the same workspace. In **Graph Models**, select the pencil next to the name to rename the draft. Switch between **Canvas** (画布) and **YAML** in the model toolbar. Both views edit the same draft, and the canvas keeps its layout and zoom when you return. **Auto** lays out connected types together and adjusts spacing to the available canvas width. The mapping panel opens beside the canvas when space permits and beneath it in narrower workspaces. On small screens, **Browse model** opens the node, edge, and local-table lists available to you.

The editor checks YAML after a typing pause. When a syntax error includes a line number, choose **Go to line** to select it in the editor. **Validate schema** checks the current text against the model and data source schema. Use **Import YAML into draft** above the text to replace it with a file, or **Download draft YAML** to export it. **Save draft** stores your changes, and **Preview and publish** shows the business validation report for a saved draft. Confirm publication in the preview.

The query editor highlights Cypher, SQL, and Gremlin and completes labels, relationship types, keys, and properties from the published model. Use **Complete** (补全) or Ctrl+Space for suggestions, Cmd/Ctrl+Enter to run, and Tab to leave the editor. Each language keeps its own query text. **Query options** (查询选项) groups Cypher parameters, timeout, full-result capture, and available CSR selection. It indicates configured values and opens when required parameters are missing or timeout is invalid. Loading history or a template returns focus to the editor, or to the parameter field when values are required. Query scope opens in its own dialog. Templates and history sit together below the results. In **Version history**, **View and compare** shows a model change summary and highlighted YAML additions/deletions; expand the full YAML comparison when needed.

Administrators can open **Runtime health** (运行健康) from the model list or **Runtime Settings → Health Checks**. Check all published models or one model, inspect source and Worker results, and follow the configuration action for a failure. Results include a check time and are refreshed on demand; an unchecked model is not reported as healthy. Draft validation is displayed separately. Status and actions remain visible; failures show a next step, while source details expand on demand.

Create or rename a model, choose a node or relationship, and edit query scope in centered dialogs. In the connector picker, search or filter by category and select a card to configure it. Source browsing, model YAML, version history, local tables, and access editors open in side panels. Table selection and field preview sit side by side when the panel is wide enough. Version history uses the same layout for releases and their differences. Long forms scroll between the heading and action bar. Deletion and recovery confirmations open over the current view. Use Cancel to return to it.

```cypher
MATCH p = (:Account)-[:Transfer]->(:Account)
RETURN p
```

To add another source, open **Data Sources** (数据源), select **Add data source** on the right of the model header, choose a connector, and fill in its connection settings or file/table mapping. Click **Add to draft** (添加到草稿), then save, validate, and publish. Filter the connector picker by category and keyword, and search existing sources by name or type. **Back to connectors** restores the picker filters and keeps the current form if you choose the same connector again. Each configured source row offers **Configure** (配置), with saving and status feedback inside the dialog, and **Browse tables and columns** (浏览表与字段). Closing configuration keeps unsaved edits in the current draft; cancelling a new source leaves the model unchanged. Credentials are referenced by server environment variable names.

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
