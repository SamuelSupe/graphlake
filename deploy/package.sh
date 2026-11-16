#!/bin/sh
set -eu

if [ "$#" -ne 2 ]; then
    printf '%s\n' 'Usage: sh deploy/package.sh RELEASE_TAG OUTPUT_DIRECTORY' >&2
    exit 2
fi
tag=$1
case "$tag" in
    ''|[!a-zA-Z0-9]*|*[!a-zA-Z0-9._-]*) printf '%s\n' 'Invalid release tag' >&2; exit 2 ;;
esac
mkdir -p -- "$2"
output=$(cd -- "$2" && pwd)
root=$(cd -- "$(dirname -- "$0")/.." && pwd)
cd -- "$root"
if [ "$(uname -s)" != Linux ]; then
    printf '%s\n' 'Run on Linux for the deployment target architecture' >&2
    exit 2
fi
export LC_ALL=C TZ=UTC
# Keep tar's staging tree off a potentially shared output mount.
stage=$(mktemp -d)
publish=
trap 'rm -rf -- "$stage"; if [ -n "$publish" ]; then rm -rf -- "$publish"; fi' EXIT
base_commit=$(git rev-parse HEAD)
receipt=${GRAPHLAKE_REGRESSION_EVIDENCE:-}
if [ -n "$receipt" ]; then
    grep -Fxq "git_base=$base_commit" "$receipt"
    grep -E '^[0-9a-f]{64}  (Cargo.lock|web/package-lock.json|tests/production-compose.yml)$' "$receipt" > "$stage/regression-inputs"
    test "$(wc -l < "$stage/regression-inputs")" -eq 3
    sha256sum -c "$stage/regression-inputs" >&2
fi
archive_source() {
    git ls-files -c -o --exclude-standard -z > "$stage/source-files"
    sort -zu "$stage/source-files" > "$stage/source-files-sorted"
    tar --null --verbatim-files-from --no-recursion --mtime=@0 \
        --owner=0 --group=0 --numeric-owner -cf "$1" -T "$stage/source-files-sorted"
}
archive_source "$stage/source.tar"
source_sha=$(sha256sum "$stage/source.tar" | cut -d ' ' -f1)
source_short=$(printf '%.12s' "$source_sha")
export GRAPHLAKE_BUILD_VERSION="$tag+src.$source_short"
target=${GRAPHLAKE_BUILD_TARGET:-$(rustc -vV | sed -n 's/^host: //p')}
case "$target" in
    aarch64-unknown-linux-gnu) arch=aarch64; machine=AArch64 ;;
    x86_64-unknown-linux-gnu) arch=x86_64; machine='Advanced Micro Devices X86-64' ;;
    *) printf 'Unsupported release target %s\n' "$target" >&2; exit 2 ;;
esac
binary_origin=prebuilt
if [ -z "${GRAPHLAKE_BIN_DIR:-}" ]; then
    binary_origin=packaging_build
    if [ -n "${GRAPHLAKE_BUILD_TARGET:-}" ]; then
        cargo build --locked --release --features admin --bins --target "$target"
        GRAPHLAKE_BIN_DIR=${CARGO_TARGET_DIR:-target}/$target/release
    else
        cargo build --locked --release --features admin --bins
        GRAPHLAKE_BIN_DIR=${CARGO_TARGET_DIR:-target}/release
    fi
fi
if [ -z "${GRAPHLAKE_WEB_DIST:-}" ]; then
    npm --prefix web ci
    npm --prefix web run build
    GRAPHLAKE_WEB_DIST=web/dist
fi
archive_source "$stage/source-after.tar"
if ! cmp -s "$stage/source.tar" "$stage/source-after.tar" || [ "$(git rev-parse HEAD)" != "$base_commit" ]; then
    printf '%s\n' 'GraphLake source changed during packaging' >&2
    exit 1
fi
name=graphlake-$tag-$source_short-linux-$arch
source_name=graphlake-$tag-$source_short-source.tar.gz
install -d -m 0755 "$stage/$name/bin" "$stage/$name/web"
for binary in graphlake graphlake-admin; do
    file=$GRAPHLAKE_BIN_DIR/$binary
    if [ "$(readelf -h "$file" | sed -n 's/^ *Machine: *//p')" != "$machine" ] ||
        ! grep -aFq -- "$GRAPHLAKE_BUILD_VERSION" "$file"; then
        printf 'Mismatched architecture or source version in %s\n' "$file" >&2
        exit 1
    fi
done
glibc_required=$(readelf --version-info "$GRAPHLAKE_BIN_DIR/graphlake" \
    "$GRAPHLAKE_BIN_DIR/graphlake-admin" | grep -o 'GLIBC_[0-9][0-9.]*' | sort -V | tail -1)
if [ -z "$glibc_required" ] ||
    [ "$(printf '%s\n' GLIBC_2.36 "$glibc_required" | sort -V | tail -1)" != GLIBC_2.36 ]; then
    printf 'Debian 12 release requires glibc 2.36 or older; found %s\n' "$glibc_required" >&2
    exit 1
fi
install -m 0755 "$GRAPHLAKE_BIN_DIR/graphlake" "$GRAPHLAKE_BIN_DIR/graphlake-admin" "$stage/$name/bin/"
install -m 0644 "$GRAPHLAKE_WEB_DIST/index.html" "$stage/$name/web/"
cp -R -- "$GRAPHLAKE_WEB_DIST/assets" "$stage/$name/web/"
install -m 0644 deploy/graphlake.service deploy/graphlake-backup.service deploy/graphlake-backup.timer deploy/graphlake-monitor.service deploy/monitor.json deploy/monitoring.md deploy/prometheus-rules.yaml deploy/telemetry.yaml deploy/README.md deploy/cluster.md deploy/haproxy.cfg deploy/keepalived.conf "$stage/$name/"
install -m 0755 deploy/graphlake-ingress-ready deploy/graphlake-monitor "$stage/$name/"
{
    printf 'build=%s\ngit_base=%s\nsource_tar=%s\nsource_tar_sha256_uncompressed=%s\ntarget=%s\nbinary_origin=%s\n' \
        "$GRAPHLAKE_BUILD_VERSION" "$base_commit" "$source_name" "$source_sha" "$target" "$binary_origin"
    rustc -vV
    cargo --version
    printf 'glibc_required=%s\n' "$glibc_required"
    printf 'cargo_lock_sha256=%s\nfixture_compose_sha256=%s\n' \
        "$(sha256sum Cargo.lock | cut -d ' ' -f1)" \
        "$(sha256sum tests/production-compose.yml | cut -d ' ' -f1)"
    if [ -n "$receipt" ]; then
        printf 'regression_receipt_sha256=%s\n' "$(sha256sum "$receipt" | cut -d ' ' -f1)"
    fi
    cd "$stage/$name"
    find . -type f ! -name BUILD-INFO -print0 | sort -z | xargs -0 sha256sum
} > "$stage/$name/BUILD-INFO"
gzip -n < "$stage/source.tar" > "$stage/$source_name"
tar --sort=name --mtime=@0 --owner=0 --group=0 --numeric-owner \
    -C "$stage" -czf "$stage/$name.tar.gz" "$name"
(
    cd -- "$stage"
    sha256sum "$name.tar.gz" > "$name.tar.gz.sha256"
    sha256sum "$source_name" > "$source_name.sha256"
)
publish=$(mktemp -d -- "$output/.graphlake-publish.XXXXXX")
cp -- "$stage/$name.tar.gz" "$stage/$name.tar.gz.sha256" \
    "$stage/$source_name" "$stage/$source_name.sha256" "$publish/"
mv -- "$publish/$name.tar.gz" "$publish/$name.tar.gz.sha256" \
    "$publish/$source_name" "$publish/$source_name.sha256" "$output/"
printf '%s\n' "$output/$name.tar.gz"
