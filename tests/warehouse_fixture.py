"""Loopback fixture for Snowflake, BigQuery and Trino query protocols."""
import argparse
import gzip
import json
import re
import uuid
from collections import Counter
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from time import sleep
from urllib.parse import parse_qs, urlsplit

SNAPSHOT = "2026-11-10T09:00:00.123456Z"
BILLING = "/bigquery/v2/projects/billing-test"
STATEMENTS = "/api/v2/statements"
TRINO = "/v1/statement"
ACCOUNTS = [
    (1, 1, "active", "15.25", "1794301200123456"),
    (1, 2, "active", None, "1794301200123456"),
    (1, 3, "locked", "9.01", "1794301200123456"),
    (2, 2, "other", "1.00", "1794301200123456"),
]
TRANSFERS = [
    (1, 10, 1, 2, 10), (1, 11, 1, 2, 20), (1, 12, 2, 1, 30),
    (1, 13, 2, 3, 40), (1, 14, 1, 1, 50), (1, 15, 1, 9, 60),
    (2, 10, 2, 2, 70),
]


def projection(sql, quote, names, rows):
    fields = re.findall(f"{quote}([^ {quote}]+){quote}", sql.split(" FROM ")[0])
    return fields, [
        [None if row[index] is None else str(row[index]) for index in map(names.index, fields)]
        for row in rows
    ]


class WarehouseHandler(BaseHTTPRequestHandler):
    jobs = {}
    trino_parametric_datetime = set()
    cancelled = Counter()

    def request_parts(self):
        assert self.headers["Authorization"] == "Bearer graphlake-fixture"
        assert self.headers["Accept"] == "application/json"
        assert self.headers["User-Agent"].startswith("GraphLake/")
        url = urlsplit(self.path)
        return url.path, parse_qs(url.query)

    def send_json(self, body, status=200, compressed=False):
        content = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        if compressed:
            assert "gzip" in self.headers["Accept-Encoding"]
            content = gzip.compress(content)
            self.send_header("Content-Encoding", "gzip")
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        self.wfile.write(content)

    def do_POST(self):
        path, params = self.request_parts()
        if path.endswith("/cancel"):
            job = path.split("/")[-2]
            assert "stalled" in self.jobs[job]
            self.cancelled["snowflake" if path.startswith(STATEMENTS) else "bigquery"] += 1
            self.send_json({})
            return

        raw = self.rfile.read(int(self.headers["Content-Length"]))
        if path == TRINO:
            assert self.headers["X-Trino-User"] == "graph-reader"
            assert self.headers["X-Trino-Session"].startswith("query_max_run_time=")
            job = str(uuid.uuid4())
            sql = raw.decode()
            self.jobs[job] = sql
            if "PARAMETRIC_DATETIME" in self.headers.get("X-Trino-Client-Capabilities", "").split(","):
                self.trino_parametric_datetime.add(job)
            origin = self.headers["Host"].replace("127.0.0.1", "localhost") if "foreign" in sql else self.headers["Host"]
            self.send_json({"id": job, "data": None,
                            "nextUri": f"http://{origin}{TRINO}/executing/{job}/1"})
            return
        body = json.loads(raw)
        if path == STATEMENTS:
            assert params["async"] == ["true"] and params["nullable"] == ["true"]
            uuid.UUID(params["requestId"][0])
            assert body["warehouse"] == "WH_TEST" and body["role"] == "READ_TEST"
            assert body["timeout"] > 0 and body["parameters"]["multi_statement_count"] == "1"
            job = str(uuid.uuid4())
            self.jobs[job] = body["statement"]
            self.send_json({"statementHandle": job}, 202)
        else:
            assert path == BILLING + "/jobs"
            config = body["configuration"]
            query = config["query"]
            assert query["useLegacySql"] is False and query["maximumBytesBilled"] == "1000000"
            assert int(config["jobTimeoutMs"]) > 0
            ref = body["jobReference"]
            assert ref["location"] == "US" and ref["projectId"] == "billing-test"
            assert ref["jobId"].startswith("graphlake_")
            self.jobs[ref["jobId"]] = query["query"]
            self.send_json({"jobReference": ref, "status": {"state": "RUNNING"}})

    def do_GET(self):
        if self.path == "/fixture/cancelled":
            self.send_json(self.cancelled)
            return
        path, params = self.request_parts()
        if path.startswith(TRINO + "/"):
            _, job, page = path.rsplit("/", 2)
            self.trino_result(self.jobs[job], job, page)
            return
        job = path.split("/")[-1]
        sql = self.jobs[job]
        if path.startswith(STATEMENTS + "/"):
            self.snowflake_result(sql, job, params)
        else:
            assert path.startswith(BILLING + "/queries/")
            self.bigquery_result(sql, params)

    def do_DELETE(self):
        path, _ = self.request_parts()
        assert path.startswith(TRINO + "/executing/")
        assert "stalled" in self.jobs[path.split("/")[-2]]
        self.cancelled["trino"] += 1
        self.send_json({})

    def trino_result(self, sql, job, page):
        if "stalled" in sql:
            sleep(2)
            return
        if '"large_accounts"' in sql:
            return self.trino_large_result(sql, job, int(page))
        assert any(f'FROM "lake"."test"."{table}" ' in sql + " "
                   for table in ("accounts", "accounts.2026.11"))
        names, rows = projection(
            sql, '"', ("id", "amount", "recorded", "payload", "_id"),
            [(9007199254740993, "12.25", "2026-01-02 03:04:05.123456789 Asia/Singapore", "AQI=", "AAAAAAAAAAAAAAAB"),
             (9007199254740994, None, "2026-01-01 19:04:05.123456789 UTC", "AwQ=", "AAAAAAAAAAAAAAAC")],
        )
        result = {"id": job}
        parametric = job in self.trino_parametric_datetime
        if page == "1":
            types = {"id": "bigint", "amount": "decimal(18,2)",
                     "recorded": "timestamp(9) with time zone" if parametric else "timestamp with time zone",
                     "payload": "varbinary", "_id": "ObjectId"}
            result["columns"] = [{"name": name, "type": types[name]} for name in names]
            result["nextUri"] = f"{TRINO}/executing/{job}/2"
        row = rows[int(page) - 1]
        if "id" in names:
            index = names.index("id")
            row[index] = int(row[index])
        if "recorded" in names and not parametric:
            index = names.index("recorded")
            row[index] = row[index].replace(".123456789 ", ".123 ")
        result["data"] = [row]
        self.send_json(result)

    def trino_large_result(self, sql, job, page):
        names = re.findall(r'"([^"]+)"', sql.split(" FROM ")[0])
        types = {"id": "bigint", "name": "varchar"}
        result = {"id": job}
        if page == 1:
            result["columns"] = [{"name": name, "type": types[name]} for name in names]
        if sql.endswith(" WHERE FALSE"):
            self.send_json(result)
            return
        match = re.search(r' WHERE "id" = ([0-9]+)$', sql)
        if match:
            ids = [int(match.group(1))]
        else:
            start = 1 + (page - 1) * 128
            ids = range(start, start + 128)
            if page < 64:
                result["nextUri"] = f"{TRINO}/executing/{job}/{page + 1}"
        result["data"] = [
            [dict(id=i, name="x" * 192)[name] for name in names] for i in ids
        ]
        self.send_json(result)

    def snowflake_result(self, sql, job, params):
        if "stalled" in sql:
            self.send_json({"statementHandle": job}, 202)
            return
        if "CURRENT_TIMESTAMP()" in sql:
            columns = [{"name": "snapshot_time", "type": "text", "nullable": False}]
            rows = [[SNAPSHOT]]
            cut = 1
        else:
            assert f"""FROM "db"."sales"."transfers" AT (TIMESTAMP => '{SNAPSHOT}'::TIMESTAMP_TZ)""" in sql
            names, rows = projection(sql, '"', ("tenant", "id", "src", "dst", "amount"), TRANSFERS)
            columns = [
                {"name": name, "type": "fixed", "precision": 18, "scale": 0, "nullable": False}
                for name in names
            ]
            if sql.endswith(" WHERE FALSE"):
                rows = []
            cut = min(2, len(rows))
        if params.get("partition") == ["1"]:
            self.send_json({"data": rows[cut:]}, compressed=True)
            return
        counts = [cut] if cut == len(rows) else [cut, len(rows) - cut]
        self.send_json({
            "statementHandle": job,
            "resultSetMetaData": {
                "numRows": len(rows), "rowType": columns,
                "partitionInfo": [{"rowCount": count} for count in counts],
            },
            "data": rows[:cut],
        })

    def bigquery_result(self, sql, params):
        assert params["location"] == ["US"]
        assert params["formatOptions.useInt64Timestamp"] == ["true"]
        if "stalled" in sql:
            self.send_json({"jobComplete": False})
            return
        if "CURRENT_TIMESTAMP()" in sql:
            columns = [{"name": "snapshot_time", "type": "STRING", "mode": "REQUIRED"}]
            rows = [[SNAPSHOT]]
        else:
            assert f"FROM `data-test.sales.accounts` FOR SYSTEM_TIME AS OF TIMESTAMP '{SNAPSHOT}'" in sql
            names, rows = projection(sql, "`", ("tenant", "id", "status", "balance", "created"), ACCOUNTS)
            types = {"status": "STRING", "balance": "NUMERIC", "created": "TIMESTAMP"}
            columns = [{"name": name, "type": types.get(name, "INT64")} for name in names]
            for column in columns:
                if column["name"] == "balance":
                    column.update(precision="38", scale="2")
            if sql.endswith(" WHERE FALSE"):
                rows = []

        token = params.get("pageToken")
        assert token in (None, ["page-2"])
        page = rows[2:] if token else rows[:2]
        result = {"jobComplete": True, "rows": [{"f": [{"v": value} for value in row]} for row in page]}
        if token is None:
            result.update(totalRows=str(len(rows)), schema={"fields": columns})
            if len(page) < len(rows):
                result["pageToken"] = "page-2"
        self.send_json(result)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("endpoint_file", type=Path)
    args = parser.parse_args()
    with ThreadingHTTPServer(("127.0.0.1", 0), WarehouseHandler) as server:
        args.endpoint_file.write_text(f"http://127.0.0.1:{server.server_port}\n")
        server.serve_forever()
