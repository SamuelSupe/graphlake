#!/usr/bin/env bash
set -euo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/production-fixtures.sh"

curl --fail --silent --show-error --retry 20 --retry-connrefused \
  --user "default:$GRAPHLAKE_TEST_CLICKHOUSE_PASSWORD" "$GRAPHLAKE_TEST_CLICKHOUSE_URI" \
  --data-binary @- <<'SQL'
CREATE DATABASE IF NOT EXISTS graphlake
SQL
while IFS= read -r -d ';' sql; do
  curl --fail --silent --show-error \
    --user "default:$GRAPHLAKE_TEST_CLICKHOUSE_PASSWORD" "$GRAPHLAKE_TEST_CLICKHOUSE_URI" \
    --data-binary "$sql"
done <<'SQL'
CREATE TABLE IF NOT EXISTS graphlake.nodes (id Int64, name LowCardinality(String), amount Decimal(12, 2), posted Nullable(Date))
ENGINE = MergeTree ORDER BY id;
ALTER TABLE graphlake.nodes ADD COLUMN IF NOT EXISTS only_insert Int64 EPHEMERAL 0;
TRUNCATE TABLE graphlake.nodes;
INSERT INTO graphlake.nodes
SELECT toInt64(number + 1), concat('node-', toString(number + 1)), toDecimal64(number, 2) + 0.25,
       if(number % 2 = 0, toDate('2026-01-02'), NULL)
FROM numbers(3000);
CREATE USER IF NOT EXISTS readonly_reader IDENTIFIED BY 'graphlake-test-password' SETTINGS readonly = 1;
GRANT SELECT ON graphlake.nodes TO readonly_reader;
CREATE TABLE IF NOT EXISTS graphlake.stream_nodes (id Int64, name String)
ENGINE = MergeTree ORDER BY id;
TRUNCATE TABLE graphlake.stream_nodes;
INSERT INTO graphlake.stream_nodes
SELECT toInt64(number), concat(toString(number), repeat('x', 128)) FROM numbers(200000);
GRANT SELECT ON graphlake.stream_nodes TO readonly_reader;
SQL

prepare_graphlake_bucket
export GRAPHLAKE_TEST_STORAGE_ENDPOINT=$AWS_ENDPOINT GRAPHLAKE_TEST_STORAGE_BUCKET=$GRAPHLAKE_TEST_BUCKET
export GRAPHLAKE_TEST_STORAGE_KEY=$AWS_ACCESS_KEY_ID GRAPHLAKE_TEST_STORAGE_SECRET=$AWS_SECRET_ACCESS_KEY

run_fixture() {
  local target=$1 test=$2 log
  local args=(--test "$target")
  if [[ $target == lib ]]; then args=(--lib); fi
  log=$(mktemp)
  if cargo test --locked --features admin "${args[@]}" "$test" -- --exact --ignored 2>&1 | tee "$log" \
      && grep -q '^test result: ok\. 1 passed;' "$log"; then
    rm -f "$log"
  else
    rm -f "$log"
    return 1
  fi
}

run_fixture engine sources::postgres_snapshot_and_parquet_s3_join
run_fixture engine sources::postgres_pushdown_preserves_null_keys_and_filtered_order
run_fixture engine sources::postgres_special_values_preserve_float_nulls_and_report_unrepresentable_columns
run_fixture engine sources::text_sources_s3_keep_versions_and_read_new_objects_on_next_query
run_fixture engine sources::object_storage_sources_use_connection_credentials_for_parquet_csv_and_jsonl
run_fixture engine query_model::cypher_correlated_call_reads_keys_and_columns_and_reuses_batches_postgres
run_fixture engine query_model::mapped_postgres_and_parquet_share_the_same_scoped_graph
run_fixture engine projected::gremlin_qualified_ids_cross_postgres_node_labels
run_fixture engine warehouses::clickhouse_reads_arrow_with_a_locked_readonly_profile
run_fixture engine direct_sources::mysql_single_scan_is_lazy_and_releases_the_read_connection
run_fixture lib source::warehouse::clickhouse::tests::single_scan_yields_before_eof_and_preserves_arrow
run_fixture lib source::warehouse::clickhouse::tests::preview_schema_excludes_ephemeral_columns
run_fixture lib admin::cluster::control::tests::stale_leadership_cannot_replace_committed_state
GRAPHLAKE_TEST_RETENTION_STORAGE="s3://$GRAPHLAKE_TEST_BUCKET" \
  run_fixture lib admin::cluster::retention::tests::sweep_keeps_current_state_jobs_and_leased_receipts
run_fixture admin local_tables::local_tables_postgres_upsert_and_cancel
run_fixture admin local_tables::mysql_local_table_reads_only_timestamp_boundary_and_changes
run_fixture admin admin_persists_validates_and_queries_a_real_cross_source_model
run_fixture admin cluster_queries_survive_coordinator_restart_and_worker_loss
run_fixture admin local_tables::revocation_reconciliation_and_failed_csr_refresh_are_visible

scratch=$(mktemp -d)
python3 tests/warehouse_fixture.py "$scratch/endpoint" &
warehouse_pid=$!
trap 'kill "$warehouse_pid" 2>/dev/null || true; wait "$warehouse_pid" 2>/dev/null || true; rm -rf -- "$scratch"' EXIT
for _ in {1..100}; do
  [[ -s "$scratch/endpoint" ]] && break
  sleep .05
done
export GRAPHLAKE_TEST_WAREHOUSE_URI GRAPHLAKE_TEST_WAREHOUSE_TOKEN=graphlake-fixture
GRAPHLAKE_TEST_WAREHOUSE_URI=$(cat "$scratch/endpoint")
for test in warehouses_join_time_travel_pages_and_decode_exact_types \
    warehouses_cancel_stalled_jobs_after_the_query_deadline trino_checks_cursor_origin_and_decodes_typed_pages \
    named_trino_connectors_qualify_the_catalog_and_preserve_results \
    trino_pushes_key_filters_and_spills_large_snapshots_within_budget \
    databricks_preserves_pages_types_and_rejects_incomplete_results databricks_cancels_on_query_timeout; do
  run_fixture engine "warehouses::$test"
done
if [[ ${GRAPHLAKE_TEST_DELTA_FIXTURE:-} ]]; then
  run_fixture engine phase3::delta_time_travel_preserves_partitions_removals_and_deletion_vectors
  run_fixture engine phase3::mysql_snapshot_joins_delta_and_preserves_exact_types
fi

# Match the PostgreSQL server major version when producing and applying the custom archive.
if [[ -x /usr/lib/postgresql/16/bin/pg_dump ]]; then
  export GRAPHLAKE_TEST_PG_DUMP=${GRAPHLAKE_TEST_PG_DUMP:-/usr/lib/postgresql/16/bin/pg_dump}
  export GRAPHLAKE_TEST_PG_RESTORE=${GRAPHLAKE_TEST_PG_RESTORE:-/usr/lib/postgresql/16/bin/pg_restore}
fi
PYTHONDONTWRITEBYTECODE=1 python3 tests/local_backup_recovery.py "${CARGO_TARGET_DIR:-target}/debug/graphlake-admin"
PYTHONDONTWRITEBYTECODE=1 python3 tests/managed_local_tables.py "${CARGO_TARGET_DIR:-target}/debug/graphlake-admin"
