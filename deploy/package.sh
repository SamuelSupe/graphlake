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
if [ -z "${GRAPHLAKE_BIN_DIR:-}" ]; then
    cargo build --locked --release --features admin --bins
    GRAPHLAKE_BIN_DIR=${CARGO_TARGET_DIR:-target}/release
fi
if [ -z "${GRAPHLAKE_WEB_DIST:-}" ]; then
    npm --prefix web ci
    npm --prefix web run build
    GRAPHLAKE_WEB_DIST=web/dist
fi
name=graphlake-$tag-linux-$(uname -m)
stage=$(mktemp -d -- "$output/.graphlake-package.XXXXXX")
trap 'rm -rf -- "$stage"' EXIT
install -d -m 0755 "$stage/$name/bin" "$stage/$name/web"
install -m 0755 "$GRAPHLAKE_BIN_DIR/graphlake" "$GRAPHLAKE_BIN_DIR/graphlake-admin" "$stage/$name/bin/"
install -m 0644 "$GRAPHLAKE_WEB_DIST/index.html" "$stage/$name/web/"
cp -R -- "$GRAPHLAKE_WEB_DIST/assets" "$stage/$name/web/"
install -m 0644 deploy/graphlake.service deploy/README.md "$stage/$name/"
tar --owner=0 --group=0 --numeric-owner -C "$stage" -czf "$stage/$name.tar.gz" "$name"
(
    cd -- "$stage"
    sha256sum "$name.tar.gz" > "$name.tar.gz.sha256"
)
mv -- "$stage/$name.tar.gz" "$stage/$name.tar.gz.sha256" "$output/"
printf '%s\n' "$output/$name.tar.gz"
