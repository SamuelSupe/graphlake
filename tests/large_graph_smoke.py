"""Verify scan, CSR refresh, offline WCC and PageRank on a million-edge graph."""
import argparse
import json
import math
import subprocess
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

RING_VERTICES = 100_000
DEGREE = 10
VERTICES = RING_VERTICES + 1
EDGES = RING_VERTICES * DEGREE


def prepare(directory):
    directory.mkdir(parents=True)
    pq.write_table(pa.table({"id": pa.array(range(VERTICES), type=pa.int64())}),
                   directory / "vertices.parquet")
    schema = pa.schema([(key, pa.int64()) for key in ("id", "src", "dst")])
    with pq.ParquetWriter(directory / "edges.parquet", schema) as writer:
        for first in range(0, RING_VERTICES, 10_000):
            ids = range(first * DEGREE, (first + 10_000) * DEGREE)
            writer.write_table(pa.Table.from_arrays([
                pa.array(ids, type=pa.int64()),
                pa.array((key // DEGREE for key in ids), type=pa.int64()),
                pa.array(((key // DEGREE + key % DEGREE + 1) % RING_VERTICES for key in ids),
                         type=pa.int64()),
            ], schema=schema))
    (directory / "graph.yaml").write_text("""graph: million_edges
sources:
  lake:
    type: parquet
    tables:
      vertices: [vertices.parquet]
      edges: [edges.parquet]
nodes:
  Vertex:
    source: lake.vertices
    key: [id]
edges:
  Link:
    source: lake.edges
    key: [id]
    from: {node: Vertex, columns: [src]}
    to: {node: Vertex, columns: [dst]}
""")


def verify(binary, directory):
    cli = [str(binary), "--catalog", str(directory / "graph.yaml"),
           "--cache-dir", str(directory / "csr"), "--cache-scope", "large-graph-smoke",
           "--memory-mib", "768", "--timeout-ms", "120000", "--diagnostics"]

    def run(name, *args):
        output = directory / f"{name}.jsonl"
        with output.open("w") as stream:
            process = subprocess.run(cli + list(args), text=True, stdout=stream,
                                     stderr=subprocess.PIPE)
        if process.returncode:
            raise RuntimeError(process.stderr)
        if name == "refresh":
            return json.loads(output.read_text())
        return json.loads(process.stderr.splitlines()[-1])["metrics"]

    def rows(name):
        with (directory / f"{name}.jsonl").open() as stream:
            for line in stream:
                yield json.loads(line)

    def wcc(name, strategy):
        metrics = run(name, "analyze", "--edge", "Link", "--strategy", strategy, "weak-components")
        assert metrics["algorithm"]["components"] == 2
        keys = set()
        for row in rows(name):
            key = row["id"]
            keys.add(key)
            assert row["__graphlake_component"]["id"] == (RING_VERTICES if key == RING_VERTICES else 0)
        assert keys == set(range(VERTICES)) and metrics["output_rows"] == VERTICES
        return metrics

    report = {"scan": wcc("scan", "scan"), "refresh": run("refresh", "refresh", "--edge", "Link")}
    refresh = report["refresh"]
    assert (refresh["vertices"], refresh["edges"]) == (VERTICES, EDGES)
    for filename in ("vertices.parquet", "edges.parquet"):
        path = directory / filename
        path.rename(path.with_suffix(".offline"))

    report["cached"] = wcc("cached", "cached")
    report["pagerank"] = run("pagerank", "analyze", "--edge", "Link", "--strategy", "cached", "pagerank")
    rank = report["pagerank"]
    assert rank["algorithm"]["converged"] and rank["output_rows"] == VERTICES
    assert math.isclose(math.fsum(row["__graphlake_rank"] for row in rows("pagerank")), 1.0,
                        rel_tol=1e-10)
    for name in ("cached", "pagerank"):
        assert report[name]["csr_generation"] == refresh["generation"]
        assert report[name]["reads"]["requests"] == 0
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("graphlake", type=Path)
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    prepare(args.directory)
    verify(args.graphlake.resolve(), args.directory.resolve())
