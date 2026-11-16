#!/usr/bin/env bash
set -euo pipefail

root=$(mktemp -d)
trap 'rm -rf -- "$root"' EXIT
python=${PYTHON:-python3}
target=${CARGO_TARGET_DIR:-target}
"$python" -c 'import pyarrow; print("PyArrow", pyarrow.__version__)'
PYO3_PYTHON="$python" PYO3_BUILD_EXTENSION_MODULE=1 cargo build --locked -p graphlake-c -p graphlake-python
cargo run --locked --example demo -- "$root/fixture"
cc -I bindings/c tests/c_smoke.c -L "$target/debug" -lgraphlake_c -o "$root/c-smoke"
LD_LIBRARY_PATH="$target/debug:${LD_LIBRARY_PATH:-}" "$root/c-smoke" "$root/fixture/graph.yaml"
ln -s "$(realpath "$target/debug/libgraphlake.so")" "$root/graphlake.so"
PYTHONPATH="$root" "$python" tests/python_smoke.py "$root/fixture/graph.yaml"
