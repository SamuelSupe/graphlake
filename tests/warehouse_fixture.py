"""Loopback fixture for Snowflake, BigQuery, Trino and Databricks query protocols."""
import argparse
import gzip
import json
import operator
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
DATABRICKS = "/api/2.0/sql/statements"
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


def warehouse_projection(sql, quote, names, rows):
    if sql.endswith((" WHERE FALSE", " LIMIT 0")):
        rows = []
    elif " WHERE " in sql:
        operators = {"=": operator.eq, "<>": operator.ne, "<": operator.lt,
                     "<=": operator.le, ">": operator.gt, ">=": operator.ge}
        for term in sql.rsplit(" WHERE ", 1)[1].split(" AND "):
            null_check = re.fullmatch(rf'{quote}([a-z_]+){quote} IS (NOT )?NULL', term)
            if null_check:
                name, negate = null_check.groups()
                index = names.index(name)
                rows = [row for row in rows if (row[index] is not None) == bool(negate)]
                continue
            match = re.fullmatch(rf'{quote}([a-z_]+){quote} (<=|>=|<>|=|<|>) (-?\d+)', term)
            assert match, f"unexpected warehouse predicate: {term}"
            name, op, value = match.groups()
            index = names.index(name)
            rows = [row for row in rows if row[index] is not None and operators[op](row[index], int(value))]
    return projection(sql, quote, names, rows)


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
            if not path.startswith(DATABRICKS):
                assert "stalled" in self.jobs[job]
            self.cancelled["databricks" if path.startswith(DATABRICKS) else "snowflake" if path.startswith(STATEMENTS) else "bigquery"] += 1
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
        if path == DATABRICKS:
            assert body["warehouse_id"] == "WH_TEST" and body["catalog"] == "main"
            assert body["format"] == "JSON_ARRAY" and body["disposition"] == "INLINE"
            job = str(uuid.uuid4())
            self.jobs[job] = body["statement"]
            self.send_json({"statement_id": job, "status": {"state": "PENDING"}})
            return
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
        if path.startswith(DATABRICKS + "/"):
            parts = path[len(DATABRICKS) + 1:].split("/")
            job = parts[0]
            self.databricks_result(self.jobs[job], job, int(parts[-1]) if len(parts) > 1 else None)
            return
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

    def databricks_result(self, sql, job, page):
        if "stalled" in sql:
            self.send_json({"statement_id": job, "status": {"state": "RUNNING"}})
            return
        types = {"tenant": "LONG", "id": "LONG", "status": "STRING", "balance": "DECIMAL", "created": "TIMESTAMP"}
        if sql.startswith("SELECT schema_name AS name FROM `main`.information_schema.schemata"):
            fields, rows = ["name"], [["sales"]]
        elif sql.startswith("SELECT table_name AS name FROM `main`.information_schema.tables"):
            assert "table_schema='sales'" in sql
            fields, rows = ["name"], [["accounts"]]
        else:
            assert "FROM `main`.`sales`." in sql
            names = ["tenant", "id", "status", "balance", "created"]
            if sql.startswith("SELECT * FROM "):
                sql = sql.replace("*", ", ".join(f"`{name}`" for name in names), 1)
            fields, rows = warehouse_projection(sql, "`", names, ACCOUNTS)
        types["name"] = "STRING"
        columns = [{"name": field, "type_name": types[field], "type_precision": 18, "type_scale": 2} for field in fields]
        if "created" in fields:
            for row in rows:
                row[fields.index("created")] = SNAPSHOT
        index = page or 0
        selected = rows[index * 2:(index + 1) * 2]
        chunk = {"chunk_index": index, "row_offset": index * 2, "row_count": len(selected), "data_array": selected}
        if (index + 1) * 2 < len(rows):
            link = f"{DATABRICKS}/{job}/result/chunks/{index + 1}?row_offset={(index + 1) * 2}"
            if "foreign" in sql:
                link = "http://localhost:1" + link
            chunk["next_chunk_internal_link"] = link
        if page is not None:
            self.send_json(chunk)
            return
        self.send_json({"statement_id": job, "status": {"state": "SUCCEEDED"},
                        "manifest": {"schema": {"columns": columns}, "total_row_count": len(rows) + (1 if "incomplete" in sql else 0), "truncated": "truncated" in sql}, "result": chunk})

    def trino_result(self, sql, job, page):
        if "stalled" in sql:
            sleep(2)
            return
        if '"large_accounts"' in sql:
            return self.trino_large_result(sql, job, int(page))
        assert any(f'FROM "lake"."test"."{table}" ' in sql + " "
                   for table in ("accounts", "accounts.2026.11", "dst_fold"))
        utc = """at_timezone("recorded", 'UTC') AS "recorded" """.strip()
        normal_sql = sql.replace(utc, '"recorded"')
        names, rows = projection(
            normal_sql, '"', ("id", "amount", "recorded", "payload", "_id"),
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
        if sql.endswith(" WHERE FALSE"):
            result.pop("nextUri", None)
            self.send_json(result)
            return
        row = rows[int(page) - 1]
        if "recorded" in names:
            index = names.index("recorded")
            if '"dst_fold"' in sql:
                row[index] = (f"2026-11-01 0{int(page) + 4}:30:00.123456789 UTC" if utc in sql
                              else "2026-11-01 01:30:00.123456789 America/New_York")
            elif utc in sql:
                row[index] = "2026-01-01 19:04:05.123456789 UTC"
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
            names, rows = warehouse_projection(sql, '"', ("tenant", "id", "src", "dst", "amount"), TRANSFERS)
            columns = [
                {"name": name, "type": "fixed", "precision": 18, "scale": 0, "nullable": False}
                for name in names
            ]
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
            names, rows = warehouse_projection(sql, "`", ("tenant", "id", "status", "balance", "created"), ACCOUNTS)
            types = {"status": "STRING", "balance": "NUMERIC", "created": "TIMESTAMP"}
            columns = [{"name": name, "type": types.get(name, "INT64")} for name in names]
            for column in columns:
                if column["name"] == "balance":
                    column.update(precision="38", scale="2")

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
