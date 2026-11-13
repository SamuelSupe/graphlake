"""Build partitioned Delta versions for removal, deletion-vector, and protocol tests."""

import argparse
from base64 import z85encode
import json
import os
from pathlib import Path
from struct import pack
import time
from urllib.parse import urlparse
from uuid import uuid4

from deltalake import DeltaTable, write_deltalake
import pyarrow as pa
import pyarrow.fs as fs
import pyarrow.parquet as pq
from pyroaring import BitMap64


def commit(table, version, actions):
    log = table / "_delta_log" / f"{version:020}.json"
    log.write_text("".join(json.dumps(action) + "\n" for action in actions))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("output", type=Path)
    parser.add_argument("s3_prefix")
    args = parser.parse_args()
    args.output.mkdir()
    table = args.output / "delta"
    write_deltalake(table, pa.table({
        "tenant": pa.array([1, 1, 1, 1, 1, 1, 2], pa.int32()),
        "id": [10, 11, 12, 13, 14, 15, 10],
        "src": [1, 1, 2, 2, 1, 1, 2],
        "dst": [2, 2, 1, 3, 1, 9, 2],
        "amount": [10, 20, 30, 40, 50, 60, 70],
    }), partition_by=["tenant"])
    initial = [json.loads(line) for line in (table / "_delta_log/00000000000000000000.json").read_text().splitlines()]
    [add] = [action["add"] for action in initial if action.get("add", {}).get("partitionValues") == {"tenant": "1"}]
    [metadata] = [action["metaData"] for action in initial if "metaData" in action]
    DeltaTable(table).delete("tenant = 2")

    ids = pq.ParquetFile(table / add["path"]).read(columns=["id"])["id"].to_pylist()
    positions = [pos for pos, value in enumerate(ids) if value in (10, 11)]
    bitmap = pack("<I", 1681511377) + BitMap64(positions).serialize()
    protocol = {
        "minReaderVersion": 3, "minWriterVersion": 7,
        "readerFeatures": ["deletionVectors"], "writerFeatures": ["deletionVectors"],
    }
    metadata["configuration"]["delta.enableDeletionVectors"] = "true"
    commit(table, 2, [
        {"protocol": protocol},
        {"metaData": metadata},
        {"remove": {
            "path": add["path"], "deletionTimestamp": int(time.time() * 1000),
            "dataChange": True, "extendedFileMetadata": True,
            "partitionValues": add["partitionValues"], "size": add["size"],
        }},
        {"add": {**add, "deletionVector": {
            "storageType": "i", "pathOrInlineDv": z85encode(bitmap).decode(),
            "sizeInBytes": len(bitmap), "cardinality": len(positions),
        }}},
    ])

    location = f"{args.s3_prefix.rstrip('/')}/{uuid4().hex}"
    endpoint = urlparse(os.environ["AWS_ENDPOINT"])
    storage = fs.S3FileSystem(
        access_key=os.environ["AWS_ACCESS_KEY_ID"], secret_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region=os.environ["AWS_REGION"], scheme=endpoint.scheme, endpoint_override=endpoint.netloc,
    )
    for file in sorted(table.rglob("*")):
        if file.is_file():
            target = location.removeprefix("s3://") + "/" + file.relative_to(table).as_posix()
            with storage.open_output_stream(target) as output:
                output.write(file.read_bytes())
    unsupported = {
        **protocol,
        "readerFeatures": ["deletionVectors", "graphlakeTestUnsupported"],
        "writerFeatures": ["deletionVectors", "graphlakeTestUnsupported"],
    }
    commit(table, 3, [{"protocol": unsupported}])
    (args.output / "fixture.json").write_text(json.dumps({"local": str(table.absolute()), "s3": location}))


if __name__ == "__main__":
    main()
