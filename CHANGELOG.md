# Changelog

## Unreleased

- Add independently configurable query and modeling MCP services. Query supports read-only graph queries. Modeling supports source-scoped model creation, validation, draft updates and optional publication. The original endpoint remains compatible. Reuse user roles, tenant scopes, optimistic revisions, HA receipts and audit logging.
- Add a standalone AI sidebar with a three-step Agent setup guide, copyable query and modeling prompts, personal MCP tokens bound to live user permissions, model capability previews and optional connection diagnostics. The administrator-only MCP Service tab in Runtime Settings controls service availability, global capabilities and query limits.
- Add an administrator Overview with service readiness, query and HTTP trends, model checks, telemetry and backup status.
- Let modelers reposition nodes and connect endpoints on the canvas. The edge drawer carries the selected endpoints into the draft.
- Remove the alert-rule editor from Runtime Settings. Prometheus rules are managed in the deployment configuration.

## v1.0.0

The first stable GraphLake release contains the embedded engine, command line, Linux administration service, and browser workbench.

- **Graph modeling** — Multi-table mappings, composite keys, source schema and data validation, business-time filters, draft publication, history, rollback, and retirement.
- **Query and exploration** — Read-only Cypher, SQL, and Gremlin, with query history, resumable neighborhoods, original-execution exports, and Arrow IPC.
- **Graph execution** — Bounded traversal, shortest paths, persisted and distributed CSR, graph algorithms, distributed Parquet scans, joins, aggregation, and spill.
- **Access and operations** — Model-level roles, service accounts, audit logs, client protocol probes, Prometheus metrics, and configurable OTLP reporting.
- **Availability and recovery** — etcd-fenced Coordinator failover, verified control-state backups, and a target-site UI for PostgreSQL and Parquet restore, query acceptance, and cutover.
- **Linux distribution** — Debian 12-compatible ARM64 and AMD64 archives, versioned source archives, checksums, and deployment assets.

Explicit Bolt transactions support PostgreSQL and Parquet. The cluster manual describes which query plans can run across Workers. The recovery UI imports a prepared off-site checkpoint. Generate and synchronize the checkpoint as part of your backup schedule.
