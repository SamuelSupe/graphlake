# Changelog

## Unreleased

- Add a configurable MCP server for read-only queries, source-scoped model creation, validation, draft updates and optional publication. Reuse user roles, tenant scopes, optimistic revisions, HA receipts and audit logging.
- Add a standalone AI sidebar with a compact three-step Agent setup guide, copyable query/modeling prompts, personal MCP tokens bound to live user permissions, per-model capability previews and optional connection diagnostics. Reveal token management, JSON configuration and permission details on demand; keep global service settings administrator-only and collapsed by default.

## v1.0.0

The first stable GraphLake release contains the embedded engine, command line, Linux administration service, and browser workbench.

- **Graph modeling** — Multi-table mappings, composite keys, source schema and data validation, business-time filters, draft publication, history, rollback, and retirement.
- **Query and exploration** — Read-only Cypher, SQL, and Gremlin, with query history, resumable neighborhoods, original-execution exports, and Arrow IPC.
- **Graph execution** — Bounded traversal, shortest paths, persisted and distributed CSR, graph algorithms, distributed Parquet scans, joins, aggregation, and spill.
- **Access and operations** — Model-level roles, service accounts, audit logs, client protocol probes, Prometheus metrics, and configurable OTLP reporting.
- **Availability and recovery** — etcd-fenced Coordinator failover, verified control-state backups, and a target-site UI for PostgreSQL and Parquet restore, query acceptance, and cutover.
- **Linux distribution** — Debian 12-compatible ARM64 and AMD64 archives, versioned source archives, checksums, and deployment assets.

Explicit Bolt transactions support PostgreSQL and Parquet. The cluster manual describes which query plans can run across Workers. The recovery UI imports a prepared off-site checkpoint. Generate and synchronize the checkpoint as part of your backup schedule.
