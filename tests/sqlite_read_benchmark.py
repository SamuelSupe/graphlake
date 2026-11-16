"""Compare row counts and query time using two GraphLake binaries and one SQLite fixture."""

import argparse
import json
import math
import sqlite3
import statistics
import subprocess
import tempfile
from pathlib import Path


def run(binary, catalog, query):
    result = subprocess.run(
        [binary, "--catalog", catalog, "--diagnostics", "query", "--language", "sql", query],
        check=True, capture_output=True, text=True,
    )
    rows = [json.loads(line) for line in result.stdout.splitlines()]
    metrics = json.loads(result.stderr.splitlines()[-1])["metrics"]
    reads = metrics["reads"]
    return rows, {
        "elapsed_ms": metrics["elapsed_ms"],
        "received_rows": reads["sqlite_rows"],
        "arrow_bytes": reads["sqlite_arrow_bytes"],
        "sqlite_fullscan_steps": reads.get("sqlite_fullscan_steps"),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("baseline", type=lambda value: Path(value).resolve())
    parser.add_argument("candidate", type=lambda value: Path(value).resolve())
    parser.add_argument("--runs", type=int, default=12)
    parser.add_argument("--require-read-budget", action="store_true")
    args = parser.parse_args()
    if args.runs < 5:
        parser.error("--runs must be at least 5")

    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        with sqlite3.connect(root / "accounts.db") as db:
            db.executescript("""
                CREATE TABLE accounts(id INTEGER PRIMARY KEY, status TEXT COLLATE NOCASE) STRICT;
                WITH RECURSIVE n(id) AS (VALUES(1) UNION ALL SELECT id+1 FROM n WHERE id<100000)
                  INSERT INTO accounts SELECT id,
                    CASE id WHEN 99998 THEN 'BLOCKED' WHEN 99999 THEN 'blocked' ELSE 'active' END
                  FROM n;
            """)
        catalog = root / "graph.yaml"
        catalog.write_text("""
graph: reads
sources:
  db: {type: sqlite, path: accounts.db}
nodes:
  Account: {source: db.accounts, key: [id], properties: [status]}
""")
        for query, expected in [
            ('SELECT id FROM "Account" WHERE id = 99999 LIMIT 1', 99999),
            ("""SELECT id FROM "Account" WHERE status = 'blocked' LIMIT 1""", 99999),
            ('SELECT id FROM "Account" LIMIT 1', None),
            ('SELECT id FROM "Account" ORDER BY id DESC LIMIT 1', 100000),
        ]:
            samples = {"baseline": [], "candidate": []}
            builds = [("baseline", args.baseline), ("candidate", args.candidate)]
            for i in range(args.runs + 1):
                for name, binary in builds if i % 2 == 0 else reversed(builds):
                    rows, metrics = run(binary, catalog, query)
                    assert len(rows) == 1, (query, name, rows)
                    assert rows[0]["id"] == expected if expected else 1 <= rows[0]["id"] <= 100000
                    if args.require_read_budget and name == "candidate":
                        assert metrics["received_rows"] == 1, (query, metrics)
                    if i:
                        samples[name].append(metrics)
            summary = {}
            for name, group in samples.items():
                elapsed = sorted(sample["elapsed_ms"] for sample in group)
                summary[name] = {
                    **group[0],
                    "p50_ms": statistics.median(elapsed),
                    "p95_ms": elapsed[math.ceil(len(elapsed) * .95) - 1],
                    "samples_ms": elapsed,
                }
                del summary[name]["elapsed_ms"]
            print(json.dumps({"query": query, **summary}))


if __name__ == "__main__":
    main()
