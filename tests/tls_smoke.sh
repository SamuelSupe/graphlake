#!/usr/bin/env bash
set -euo pipefail

: "${GRAPHLAKE_TEST_PG_TLS:?set the PostgreSQL TLS DSN}"
: "${GRAPHLAKE_TEST_PG_TLS_WRONG_HOST:?set the PostgreSQL DSN with an unmatched host}"
: "${GRAPHLAKE_TEST_MYSQL_TLS:?set the MySQL TLS DSN}"
: "${GRAPHLAKE_TEST_MYSQL_TLS_WRONG_HOST:?set the MySQL DSN with an unmatched host}"

binary=${1:?pass the GraphLake executable}
ca=${2:?pass the test CA certificate}
dir=$(mktemp -d)
trap 'rm -rf "$dir"' EXIT
cp "$ca" "$dir/ca.pem"
cat > "$dir/postgres.yaml" <<'YAML'
graph: postgres_tls
sources:
  db: {type: postgres, dsn_env: GRAPHLAKE_TEST_TLS_DSN}
nodes:
  Account: {source: db.public.accounts, key: [id]}
YAML
cat > "$dir/mysql.yaml" <<'YAML'
graph: mysql_tls
sources:
  db: {type: mysql, dsn_env: GRAPHLAKE_TEST_TLS_DSN, ca_file: ca.pem}
nodes:
  Account: {source: db.graphlake.accounts, key: [id]}
YAML
sed 's/, ca_file: ca.pem//' "$dir/mysql.yaml" > "$dir/mysql-untrusted.yaml"

query() {
    GRAPHLAKE_TEST_TLS_DSN="$2" "$binary" --catalog "$dir/$1.yaml" \
        query --language sql 'SELECT id FROM "Account"'
}

reject() {
    if query "$1" "$2" >"$dir/out" 2>"$dir/error"; then
        echo "Unexpected TLS success for $1" >&2
        exit 1
    fi
    if ! rg -q "$3" "$dir/error"; then
        cat "$dir/error" >&2
        exit 1
    fi
}

reject postgres "$GRAPHLAKE_TEST_PG_TLS" UnknownIssuer
export SSL_CERT_FILE="$dir/ca.pem"
test "$(query postgres "$GRAPHLAKE_TEST_PG_TLS")" = '{"id":7}'
reject postgres "$GRAPHLAKE_TEST_PG_TLS_WRONG_HOST" 'certificate not valid for name'

reject mysql-untrusted "$GRAPHLAKE_TEST_MYSQL_TLS" UnknownIssuer
test "$(query mysql "$GRAPHLAKE_TEST_MYSQL_TLS")" = '{"id":7}'
reject mysql "$GRAPHLAKE_TEST_MYSQL_TLS_WRONG_HOST" 'certificate not valid for name'
echo "PostgreSQL and MySQL TLS smoke passed"
