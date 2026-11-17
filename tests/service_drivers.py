"""Run against the published payments fixture with neo4j and gremlinpython installed."""

import asyncio
import os
import json
from base64 import b64encode
from contextlib import AsyncExitStack, ExitStack
import socket
import sys
from urllib.parse import urlsplit
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from aiohttp import ClientSession
from gremlin_python.driver.client import Client
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.driver.protocol import GremlinServerError
from gremlin_python.driver.serializer import GraphSONSerializersV3d0, GraphBinarySerializersV1
from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.traversal import P
from neo4j import GraphDatabase, Query
from neo4j.exceptions import Neo4jError
from neo4j.graph import Node, Relationship, Path
from gremlin_python.structure.graph import Vertex, Edge, Path as GremlinPath


def assert_rejected(action, error_type, check):
    try:
        action()
    except error_type as error:
        assert check(error), error
    else:
        raise AssertionError("request unexpectedly succeeded")



def check_bolt_handshake_capacity(bolt, user, token):
    address = urlsplit(bolt)
    # Bolt 5.1 negotiates HELLO and LOGON separately.
    handshake = bytes.fromhex("6060b01700000105000000000000000000000000")
    hello = bytes.fromhex("b101a18a") + b"user_agent" + bytes.fromhex("85") + b"audit"
    with GraphDatabase.driver(bolt, auth=(user, token)) as driver, ExitStack() as stack:
        session = stack.enter_context(driver.session(database="payments"))
        tx = stack.enter_context(session.begin_transaction())
        assert tx.run("RETURN 1 AS n").single()["n"] == 1
        for _ in range(16):
            conn = stack.enter_context(socket.create_connection((address.hostname, address.port), timeout=2))
            reader = stack.enter_context(conn.makefile("rb"))
            conn.sendall(handshake)
            assert reader.read(4) == bytes.fromhex("00000105")
            conn.sendall(len(hello).to_bytes(2, "big") + hello + b"\x00\x00")
            size = int.from_bytes(reader.read(2), "big")
            assert reader.read(size).startswith(bytes.fromhex("b170"))
            assert reader.read(2) == b"\x00\x00"
        # All regular handshakes are stalled. A ready client can authenticate
        # through the slot with a shorter deadline.
        with driver.session(database="payments") as fresh:
            assert fresh.run("RETURN 2 AS n").single()["n"] == 2
        assert tx.run("RETURN 3 AS n").single()["n"] == 3
        tx.rollback()


async def check_gremlin_handshake_capacity(ws, user, token, established):
    def check_queries():
        ready = Client(ws, "g", username=user, password=token,
                       message_serializer=GraphSONSerializersV3d0())
        try:
            query = "g.V().hasLabel('Merchant').limit(1).count()"
            assert ready.submit(query).all().result() == [1]
            assert established.submit(query).all().result() == [1]
        finally:
            ready.close()

    async with ClientSession() as session, AsyncExitStack() as stack:
        for _ in range(16):
            await stack.enter_async_context(session.ws_connect(ws))
        await asyncio.to_thread(check_queries)


def main(http, bolt, revision):
    token = os.environ["GRAPHLAKE_DRIVER_TOKEN"]
    user = os.environ.get("GRAPHLAKE_DRIVER_USER", "graphlake")
    ws = http.replace("http://", "ws://", 1) + "/gremlin"
    with GraphDatabase.driver(bolt, auth=(user, "wrong-token")) as driver:
        assert_rejected(driver.verify_connectivity, Neo4jError,
                        lambda err: err.code.endswith("Unauthorized"))
    with GraphDatabase.driver(bolt, auth=(user, token)) as driver:
        records, _, _ = driver.execute_query("RETURN 42 AS n", database_="payments", routing_="r")
        assert records[0]["n"] == 42
        with driver.session(database="payments", fetch_size=1) as session:
            statement = ("MATCH (:CustomerAccount)-[p:Payment]->(:Merchant) "
                         "WHERE p.payment_id = $id RETURN p.payment_id AS id, p.amount AS amount")
            result = session.run(Query(statement, timeout=5), id=2000)
            assert result.data() == [{"id": 2000, "amount": "55.000"}]
            assert result.consume().metadata["model_revision"] == revision
            assert session.run("RETURN $id AS id", id=9007199254740993).single()["id"] == 9007199254740993
            assert session.run("UNWIND range(1,3) AS n RETURN n ORDER BY n").value() == [1, 2, 3]
            assert session.run("CALL dbms.components() YIELD name RETURN name").value() == ["GraphLake"]
            fields = session.run(
                "CALL db.schema.relTypeProperties() YIELD relType, propertyName, propertyTypes "
                "WHERE relType = 'Payment' AND propertyName = 'amount' RETURN propertyTypes"
            ).value()
            assert fields == [["Decimal"]], fields
            mixed = session.run("UNWIND [1,'1'] AS n RETURN n").value()
            assert len(mixed) == 2 and 1 in mixed and "1" in mixed, mixed
            assert_rejected(lambda: session.run("CREATE (:Forbidden)").consume(), Neo4jError,
                            lambda err: err.code.startswith("Neo.ClientError."))
            assert session.execute_read(lambda tx: tx.run(statement, id=2000).single()["amount"]) == "55.000"
            with session.begin_transaction() as tx:
                first = tx.run("UNWIND [1,2,3] AS n RETURN n ORDER BY n")
                second = tx.run("RETURN 4 AS n")
                assert second.value() == [4]
                assert first.value() == [1, 2, 3]
                tx.rollback()
            row = session.run(
                "MATCH p=(a:CustomerAccount)-[r:Payment]->(b:Merchant) "
                "WHERE r.payment_id = 2000 RETURN a,r,b,p"
            ).single()
            assert isinstance(row["a"], Node) and "CustomerAccount" in row["a"].labels
            assert isinstance(row["r"], Relationship) and row["r"]["amount"] == "55.000"
            assert isinstance(row["b"], Node)
            assert row["r"].start_node == row["a"] and row["r"].end_node == row["b"]
            assert isinstance(row["p"], Path) and row["p"].relationships == (row["r"],)
            assert row["p"].nodes == (row["a"], row["b"])
        with driver.session(database="payments", fetch_size=16) as session:
            rows = list(session.run(
                "MATCH p=(a:CustomerAccount)-[r:Payment]->(b:Merchant) "
                "RETURN r.payment_id AS id,p ORDER BY id"
            ))
            assert [item["id"] for item in rows] == sorted(item["id"] for item in rows)
            assert all(isinstance(item["p"], Path) and item["p"].relationships[0]["payment_id"] == item["id"]
                       for item in rows), rows
            assert any(item["id"] == 2000 for item in rows)

        with driver.session(database="payments", fetch_size=257) as session:
            assert sum(1 for _ in session.run("UNWIND range(0,10000) AS n RETURN n")) == 10001

    check_bolt_handshake_capacity(bolt, user, token)

    client = Client(ws, "g", username=user, password="wrong-token",
                    message_serializer=GraphSONSerializersV3d0())
    try:
        assert_rejected(lambda: client.submit("g.V().limit(1)").all().result(),
                        GremlinServerError, lambda err: err.status_code == 401)
    finally:
        client.close()
    client = Client(ws, "g", username=user, password=token,
                    message_serializer=GraphSONSerializersV3d0())
    try:
        result = client.submit("g.V().hasLabel('Merchant').limit(1).count()")
        assert result.all().result() == [1]
        assert result.status_attributes["model_revision"] == revision
        asyncio.run(check_gremlin_handshake_capacity(ws, user, token, client))
        streamed = client.submit("g.V().hasLabel('CustomerAccount').limit(302).id()")
        values = streamed.all().result()
        assert len(values) == len(set(values)) and values
        if os.environ.get("GRAPHLAKE_TEST_STREAM_ROWS"):
            assert len(values) >= int(os.environ["GRAPHLAKE_TEST_STREAM_ROWS"]), len(values)
        assert streamed.status_attributes["model_revision"] == revision
        assert_rejected(lambda: client.submit("g.addV('Forbidden')").all().result(),
                        GremlinServerError, lambda err: err.status_code >= 400)
    finally:
        client.close()
    conn = DriverRemoteConnection(ws, "g", username=user, password=token,
                                  message_serializer=GraphSONSerializersV3d0())
    try:
        g = traversal().with_remote(conn)
        result = (g.V().has_label("CustomerAccount").out_e("Payment").has("payment_id", 2000)
                  .filter_(__.in_v().has_label("Merchant")).values("payment_id").to_list())
        assert result == [2000], result
        ids = g.V().limit(3).id_().to_list()
        assert len(ids) == 3 and all(isinstance(value, str) for value in ids), ids
        vertices = g.V().has_label("CustomerAccount").limit(1).to_list()
        assert isinstance(vertices[0], Vertex), vertices
        edges = g.E().has_label("Payment").has("payment_id", 2000).to_list()
        assert isinstance(edges[0], Edge) and edges[0].outV.label == "CustomerAccount", edges
        paths = g.V().has_label("CustomerAccount").out_e("Payment").has("payment_id", 2000).in_v().path().to_list()
        assert isinstance(paths[0], GremlinPath) and isinstance(paths[0].objects[1], Edge), paths
    finally:
        conn.close()

    binary = DriverRemoteConnection(ws, "g", username=user, password=token,
                                    message_serializer=GraphBinarySerializersV1())
    try:
        g = traversal().with_remote(binary)
        assert g.V().limit(1).count().to_list() == [1]
        paths = (g.V().has_label("CustomerAccount").out_e("Payment")
                 .has("payment_id", P.eq(2000)).in_v().path().to_list())
        assert len(paths) == 1 and isinstance(paths[0], GremlinPath), paths
        first, edge, last = paths[0].objects
        assert isinstance(first, Vertex) and isinstance(edge, Edge) and isinstance(last, Vertex)
        assert edge.outV.id == first.id and edge.inV.id == last.id and edge.label == "Payment", paths
    finally:
        binary.close()

    request = Request(http + "/api/v1/models", headers={"Authorization": f"Bearer {token}"})
    assert_rejected(lambda: urlopen(request, timeout=5), HTTPError, lambda err: err.code == 401)
    if user != "graphlake":
        basic = "Basic " + b64encode(f"{user}:{token}".encode()).decode()
        def get(path):
            return urlopen(Request(http + path, headers={"Authorization": basic}), timeout=5)
        with get("/api/v1/models") as response:
            assert [model["id"] for model in json.load(response)] == ["payments"]
        assert_rejected(lambda: get("/api/v1/models/hidden/schema/" + revision), HTTPError,
                        lambda error: error.code == 403)
        assert_rejected(lambda: get("/api/v1/models/payments"), HTTPError,
                        lambda error: error.code == 403)
        with GraphDatabase.driver(bolt, auth=(user, token)) as driver:
            with driver.session(database="hidden") as session:
                assert_rejected(lambda: session.run("RETURN 1").consume(), Neo4jError,
                                lambda error: error.code.endswith("Forbidden"))
        client = Client(ws, "hidden", username=user, password=token,
                        message_serializer=GraphSONSerializersV3d0())
        try:
            assert_rejected(lambda: client.submit("g.V().limit(1)").all().result(),
                            GremlinServerError, lambda error: error.status_code == 403)
        finally:
            client.close()
    print("Bolt and Gremlin clients, transactions, native graph values, batches and authorization passed")


if __name__ == "__main__":
    main(*sys.argv[1:])
