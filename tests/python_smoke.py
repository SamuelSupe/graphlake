"""Run the native Python binding against the catalog made by the demo example."""

import json
import sys
from pathlib import Path
from tempfile import TemporaryDirectory

import graphlake
import pyarrow as pa


def records(query):
    return pa.Table.from_batches(list(query), schema=query.schema).to_pylist()


def main():
    catalog = Path(sys.argv[1])
    with TemporaryDirectory() as cache:
        engine = graphlake.Engine(catalog, cache_dir=cache, cache_scope="python-smoke")
        with engine.query(
            'SELECT count(*) AS transfers FROM "Transfer"', language="sql"
        ) as query:
            assert records(query) == [{"transfers": 4}]
            assert query.versions and query.metrics()["output_rows"] == 1
        with engine.query("MATCH (a:Account) WHERE a.account_id = $id RETURN a.account_id AS id",
                          parameters={"id": 20}) as query:
            assert records(query) == [{"id": 20}]
        scoped = graphlake.Engine(catalog, logical_partition=json.dumps([
            {"column": "tenant_id", "op": "eq", "value": 2}
        ]))
        with scoped.query("g.E().hasLabel('Transfer').count()", language="gremlin") as query:
            assert records(query) == [{"value": 0}]
        strict = graphlake.Engine(catalog, require_consistent_snapshot=True)
        with strict.query('SELECT count(*) AS n FROM "Account"', language="sql") as query:
            assert records(query) == [{"n": 3}]
        try:
            strict.query('SELECT count(*) AS n FROM "Transfer"', language="sql")
        except RuntimeError as error:
            assert "requires one consistent snapshot" in str(error)
        else:
            raise AssertionError("strict query accepted independently pinned objects")
        generation = engine.refresh("Transfer")["generation"]
        with engine.traverse("Transfer", ["1", "10"], 3, strategy="cached") as query:
            assert [row["account_id"] for row in records(query)] == [10, 20, 30]
            assert query.metrics()["csr_generation"] == generation
            assert query.metrics()["reads"]["requests"] == 0
        with engine.analyze("Transfer", "pagerank") as query:
            ranks = records(query)
            assert len(ranks) == 3
            assert abs(sum(row["__graphlake_rank"] for row in ranks) - 1.0) < 1e-10
            assert query.metrics()["algorithm"]["converged"]
        with engine.route(
            ["Transfer"], "Account", ["1", "10"], "Account", ["1", "30"],
            weight="amount", k=2,
        ) as query:
            paths = records(query)
            assert [row["cost"] for row in paths] == [400.0, 500.0]
            assert [row["cost_exact"] for row in paths] == ["400", "500"]
            assert paths[0]["vertices"][-1] == {"label": "Account", "key": ["1", "30"]}
        with engine.analyze_projection(
            ["Transfer"], "node2vec", settings='{"dimensions": 8}'
        ) as query:
            embeddings = records(query)
            assert len(embeddings) == 3
            assert all(len(row["__graphlake_embedding"]) == 8 for row in embeddings)
        query = engine.query('SELECT * FROM "Account"', language="sql")
        query.cancel()
        try:
            next(query)
        except RuntimeError as error:
            assert "cancelled" in str(error)
        else:
            raise AssertionError("cancelled query produced a result")
        query.close()
        assert list(query) == []

        remaining = engine.query('SELECT count(*) AS n FROM "Account"', language="sql")
        del engine
        assert records(remaining) == [{"n": 3}]
    print("Python Arrow streaming, cancellation and cached graph operations passed")


if __name__ == "__main__":
    main()
