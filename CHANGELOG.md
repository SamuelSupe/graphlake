# Changelog

## v1.1.0

- Load configured source tables into managed Parquet copies and refresh them manually or on a schedule. Incremental refresh reads from a typed watermark and replaces rows by primary key.
- Publish local snapshots with atomic manifest replacement and recover interrupted loads on restart. Reuse unchanged row groups, compact delta files, and reclaim affected base partitions. A full refresh can rebuild an indexed snapshot with missing data parts while checking the committed schema.
- Prune local data files by signed primary-key ranges and filter before reading wide payload pages. Compact dense deletion masks as ranges while accepting existing position lists.

- Show datasource brand SVGs on connector cards and object-storage provider settings.

- Add independently configurable query and modeling MCP services. Query supports read-only graph queries. Modeling supports source-scoped model creation, validation, draft updates and optional publication. The original endpoint remains compatible. Reuse user roles, tenant scopes, optimistic revisions, HA receipts and audit logging.
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
