#!/usr/bin/env bash
# Source this file before running fixture-backed acceptance scripts.
fixture_host=${GRAPHLAKE_FIXTURE_HOST:-127.0.0.1}
export GRAPHLAKE_TEST_POSTGRES="postgresql://postgres:graphlake-test-password@$fixture_host:${GRAPHLAKE_FIXTURE_POSTGRES_PORT:-25432}/postgres?sslmode=disable&application_name=graphlake-validation"
export GRAPHLAKE_ADMIN_TEST_PG_DSN="$GRAPHLAKE_TEST_POSTGRES"
export GRAPHLAKE_TEST_MYSQL="mysql://root:graphlake-test-password@$fixture_host:${GRAPHLAKE_FIXTURE_MYSQL_PORT:-23306}/mysql"
export GRAPHLAKE_TEST_CLICKHOUSE_URI="http://$fixture_host:${GRAPHLAKE_FIXTURE_CLICKHOUSE_PORT:-28123}"
export GRAPHLAKE_TEST_CLICKHOUSE_PASSWORD=graphlake-test-password
export GRAPHLAKE_TEST_ETCD="http://$fixture_host:${GRAPHLAKE_FIXTURE_ETCD_PORT:-22379}"
export GRAPHLAKE_TEST_BUCKET=graphlake-ci
export AWS_ENDPOINT="http://$fixture_host:${GRAPHLAKE_FIXTURE_S3_PORT:-29000}"
export AWS_REGION=us-east-1
export AWS_ALLOW_HTTP=true
export AWS_ACCESS_KEY_ID=graphlake-ci
export AWS_SECRET_ACCESS_KEY=graphlake-test-password

prepare_graphlake_bucket() {
  local bucket
  bucket="$AWS_ENDPOINT/$GRAPHLAKE_TEST_BUCKET"
  if ! curl --fail --silent --head --aws-sigv4 'aws:amz:us-east-1:s3' \
    --user "$AWS_ACCESS_KEY_ID:$AWS_SECRET_ACCESS_KEY" "$bucket" >/dev/null; then
    curl --fail --silent --show-error --retry 20 --retry-connrefused \
      --aws-sigv4 'aws:amz:us-east-1:s3' --user "$AWS_ACCESS_KEY_ID:$AWS_SECRET_ACCESS_KEY" \
      --request PUT "$bucket"
  fi
}
