# Changelog

## v1.1.5

- Redesign the console around the GraphLake blue palette. Align page titles, actions and dialogs across the model editor, data sources, query workspace and administration pages on desktop and narrow screens.
- Keep directory filters, view, scroll position and keyboard focus when returning from a model. On a temporary refresh failure, keep the previous results available and identify them as the last loaded list.
- Preserve inputs when retrying model creation, browsing source metadata and saving query templates. When loading a template, restore explicitly saved parameters or focus the parameter field if values are required.
- Keep telemetry and MCP drafts through failed reloads. Show revision conflicts in the form and keep background health updates from overwriting current edits.

## v1.1.4

- Roll large model version histories into immutable pages while preserving published revisions, rollback and backups.
- Correct multi-start Cypher traversal and binary graph identities. Push eligible ALL and NONE shortest-path predicates into traversal, filter candidate rows before correlated path predicates, and use a set join for eligible unbounded correlated CALLs with broad frontiers, including nested calls. Push down eligible Gremlin loop limits.
- Bound Cypher expression generation, driver rows and Bolt spools. Compact CSR spill runs during ingestion, cap batch rows before sorting CSR properties, and stop retrying partitions that report exhausted storage.
- Retry failed full reconciliations without advancing their schedule. Reclaim legacy single-file snapshots during incremental loads and limit retired local parts while readers pin old generations.
- Confirm ambiguous Worker task claims, reduce etcd polling, and reuse managed snapshot GC references. Batch OpenDAL deletes and move remaining MCP draft reads off the async runtime threads.

## v1.1.3

- Stream eligible CSV and JSON Lines previews after validating business keys for the pinned source version. Revalidate when the file version changes.
- Estimate nested array allocations from the relevant slices so shared Arrow buffers do not exhaust the query budget. Keep rejecting expansions that exceed the budget.
- Read CSR identities in vertex order for distributed PageRank and weakly connected components, and index large blocks for component representative lookups.
- Limit Coordinator storage probes to one in flight per process, including after a probe timeout.
- Add an opt-in OpenDAL backend for cluster S3 storage and harden managed snapshot history and recovery.
- Edit graph models in YAML and on the canvas with bidirectional synchronization. Improve business task and fixed scenario editing.

- Keep driver traffic off the etcd leadership confirmation path, and reclaim eligible managed local snapshots even when a refresh retires another generation.
- Reuse graph and connection state across reads, bound slow downloads and automatic retries, and preserve indexed local refreshes for batches larger than 1,024 rows.
- MCP `get_schema` returns published structure by default. Pass `include_types: true` for live type resolution.

## v1.1.2

- Use the same GraphLake logo on the sign-in page, console navigation, and browser tab.
- Refresh the English Cypher workbench and local-table sync screenshots in the README.

## v1.1.1

- Keep both branches of a paged Gremlin traversal on the same input. Allow Gremlin queries to use published CSR snapshots and combine equivalent traversers in eligible count queries without losing multiplicity.
- Reuse stable Cypher correlated `CALL` plans and preserve aggregate `ORDER BY` expressions. Use the live graph for SCD2 validity evaluated against the current clock, or build a CSR with an explicit historical time.
- Use PostgreSQL indexes for eligible frontier and text key lookups while preserving exact comparisons. Reuse Trino timestamp schema information, Delta snapshots and S3 clients, and decode SQLite rows directly into Arrow builders.
- Add an opt-in SQL Server `datetime2` microsecond truncation setting. The default preserves strict precision checks and supports the full date range.
- Apply configured execution budgets across foreground queries, data validation, sync, CSR builds and Worker stages. Keep bounded Coordinator capacity for schema probes and graph exploration. Reuse verified model archives and retry transient etcd read failures.
- Retire CSR pointers for inactive model revisions and skip block inventories for live generations. Preserve leased query and catalog receipt references during cleanup.
- Keep concurrent object-version checks compatible with the Python traversal and graph analytics bindings.
- Default the console to English and improve responsive workflows.

- Add published-model observation, reconciliation and CSR checks, free-space alerts, and S3 snapshot GC metrics to the production monitoring setup.
- Preserve healthy legacy local-table state on startup so a readers-first rollout can return to the previous release before activating new writers.
- Gate Debian 12 release artifacts on exact-archive smoke, real-service regressions, rolling upgrade and rollback, concurrent sync and recovery, and C/Python smoke. Tie the artifacts to the regression receipt and fixture versions.

## v1.1.0

- Load configured source tables into managed Parquet copies and refresh them manually or on a schedule. Incremental refresh reads from a typed watermark and replaces rows by primary key.
- Publish local snapshots with atomic manifest replacement and recover interrupted loads on restart. Reuse unchanged row groups, compact delta files, and reclaim affected base partitions. A full refresh can rebuild an indexed snapshot with missing data parts while checking the committed schema.
- Prune local data files by signed primary-key ranges and filter before reading wide payload pages. Compact dense deletion masks as ranges while accepting existing position lists.

- Add two evidence-backed MCP tasks for offboarding and alert investigations. The workbench configures tasks and fixed fixtures, previews per-scenario findings and evidence before publish and rollback, and shows a separate business validation status. The server reruns the checks and rejects stale previews.

- Show datasource brand SVGs on connector cards and object-storage provider settings.

- Add independently configurable query and modeling MCP services. Query supports read-only graph queries. Modeling supports source-scoped model creation, validation, draft updates and optional publication. The original endpoint remains compatible. Reuse user roles, tenant scopes, optimistic revisions, HA receipts and audit logging.
- Add optional business semantics to model YAML and the workbench. MCP `get_schema` exposes the published declarations, mapped field types and SCD2 rules. Model validation checks semantic field references, units and evidence time types.
- Add a standalone AI sidebar with a three-step Agent setup guide, copyable query and modeling prompts, personal MCP tokens bound to live user permissions, model capability previews and optional connection diagnostics. The administrator-only MCP Service tab in Runtime Settings controls service availability, global capabilities and query limits.
- Add an administrator Overview with service readiness, query and HTTP trends, model checks, telemetry and backup status.
- Let modelers reposition nodes and connect endpoints on the canvas. The edge drawer carries the selected endpoints into the draft.
- Remove the alert-rule editor from Runtime Settings. Prometheus rules are managed in the deployment configuration.
- Use a single DuckDB scan for narrow integer Top-N projections where a second row-ID lookup adds scan work.

- Reduce DuckDB LIMIT and aggregate latency with table-local primary-key checks, and keep small snapshots in memory within the query budget.

- Read only referenced columns for eligible SQL queries and push ordered LIMIT and scalar COUNT/MIN/MAX into DuckDB and PostgreSQL when the source proves business-key uniqueness.

- Push safe integer and null predicates into direct SQL and warehouse reads, prune Iceberg scans, project MongoDB schema samples, and reduce range requests for remote CSV and JSON Lines.

- Add direct SQL Server, Oracle, MongoDB, DuckDB and Databricks SQL sources, plus Doris, StarRocks, TiDB, OceanBase MySQL mode and MariaDB adapters. Existing named Trino configurations remain compatible.
- Add per-source object-storage configuration and object listing for S3-compatible services, Azure Blob and GCS, with Parquet/CSV/JSON Lines table mappings.

- Datasource usability: category filters, source search, required connection fields with inline feedback, and configuration dialogs with local save/conflict feedback and a persistent action area.

- Card-based datasource workspace with searchable connectors, direct connection and table configuration before adding to a draft, and per-source configuration and schema browsing.

- Native CSV and JSON Lines sources with explicit schemas, local/HTTP/S3 file lists, pinned object versions, bounded materialization, and workbench table/column editing.
- Native read-only SQLite sources with a shared read transaction per query, typed rows, predicate pushdown, cancellation, and workbench browsing.
- Named SQL Server and Oracle source configurations through existing Trino catalogs.

## v1.0.0

The first stable GraphLake release contains the embedded engine, command line, Linux administration service, and browser workbench.

- **Graph modeling** — Multi-table mappings, composite keys, source schema and data validation, business-time filters, draft publication, history, rollback, and retirement.
- **Query and exploration** — Read-only Cypher, SQL, and Gremlin, with query history, resumable neighborhoods, original-execution exports, and Arrow IPC.
- **Graph execution** — Bounded traversal, shortest paths, persisted and distributed CSR, graph algorithms, distributed Parquet scans, joins, aggregation, and spill.
- **Access and operations** — Model-level roles, service accounts, audit logs, client protocol probes, Prometheus metrics, and configurable OTLP reporting.
- **Availability and recovery** — etcd-fenced Coordinator failover, verified control-state backups, and a target-site UI for PostgreSQL and Parquet restore, query acceptance, and cutover.
- **Linux distribution** — Debian 12-compatible ARM64 and AMD64 archives, versioned source archives, checksums, and deployment assets.

Explicit Bolt transactions support PostgreSQL and Parquet. The cluster manual describes which query plans can run across Workers. The recovery UI imports a prepared off-site checkpoint. Generate and synchronize the checkpoint as part of your backup schedule.
