"""Create an Iceberg v2 validation table with real positional and equality delete files."""

import argparse
import json
import os
from pathlib import Path
import time
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.catalog import load_catalog
from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.manifest import (
    DataFile,
    DataFileContent,
    FileFormat,
    ManifestContent,
    ManifestEntry,
    ManifestEntryStatus,
    ManifestWriterV2,
    write_manifest_list,
)
from pyiceberg.table.refs import SnapshotRefType
from pyiceberg.table.snapshots import Operation, Snapshot, Summary
from pyiceberg.table.update import AddSnapshotUpdate, SetSnapshotRefUpdate
from pyiceberg.typedef import Record


class DeleteManifest(ManifestWriterV2):
    def content(self):
        return ManifestContent.DELETES

    @property
    def _meta(self):
        return {**super()._meta, "content": "deletes"}


def write_delete(table, suffix, content, rows, equality_ids=None):
    path = f"{table.location()}/data/{suffix}-{uuid4()}.parquet"
    with table.io.new_output(path).create() as output:
        pq.write_table(rows, output)
    return DataFile.from_args(
        content=content,
        file_path=path,
        file_format=FileFormat.PARQUET,
        partition=Record(),
        record_count=len(rows),
        file_size_in_bytes=len(table.io.new_input(path)),
        equality_ids=equality_ids,
    )


def build_table(catalog, warehouse):
    catalog.create_namespace_if_not_exists("phase2")
    table = catalog.create_table(
        f"phase2.transfers_{uuid4().hex}",
        schema=pa.schema([
            ("tenant", pa.int32()), ("id", pa.int64()), ("src", pa.int64()),
            ("dst", pa.int64()), ("amount", pa.int64()),
        ]),
        location=f"{warehouse.rstrip('/')}/transfers-{uuid4()}",
        properties={"format-version": "2"},
    )
    table.append(pa.Table.from_arrays(
        [
            pa.array([1, 1, 1, 1, 1, 1, 2], pa.int32()),
            pa.array([10, 11, 12, 13, 14, 15, 10]),
            pa.array([1, 1, 2, 2, 1, 1, 2]),
            pa.array([2, 2, 1, 3, 1, 9, 2]),
            pa.array([10, 20, 30, 40, 50, 60, 70]),
        ],
        schema=schema_to_pyarrow(table.schema()),
    ))
    before = table.current_snapshot()
    [task] = list(table.scan().plan_files())
    position_schema = pa.schema([
        pa.field("file_path", pa.string(), False, {b"PARQUET:field_id": b"2147483546"}),
        pa.field("pos", pa.int64(), False, {b"PARQUET:field_id": b"2147483545"}),
    ])
    position = pa.Table.from_arrays(
        [pa.array([task.file.file_path]), pa.array([0], pa.int64())], schema=position_schema,
    )
    equality = table.schema().select("tenant", "id")
    equality_rows = pa.Table.from_arrays(
        [pa.array([1], pa.int32()), pa.array([11], pa.int64())],
        schema=schema_to_pyarrow(equality, include_field_ids=True),
    )
    deletes = [
        write_delete(table, "position", DataFileContent.POSITION_DELETES, position),
        write_delete(table, "equality", DataFileContent.EQUALITY_DELETES, equality_rows,
                     [field.field_id for field in equality.fields]),
    ]
    snapshot_id = table.metadata.new_snapshot_id()
    sequence = before.sequence_number + 1
    metadata = f"{table.location()}/metadata"
    manifest_path = f"{metadata}/{uuid4()}-deletes.avro"
    with DeleteManifest(table.spec(), table.schema(), table.io.new_output(manifest_path), snapshot_id, "null") as writer:
        for delete in deletes:
            writer.add(ManifestEntry.from_args(
                status=ManifestEntryStatus.ADDED,
                snapshot_id=snapshot_id,
                data_file=delete,
            ))
    manifest = writer.to_manifest_file()
    manifest_list = f"{metadata}/snap-{snapshot_id}-{uuid4()}.avro"
    with write_manifest_list(2, table.io.new_output(manifest_list), snapshot_id,
                             before.snapshot_id, sequence, "null") as writer:
        writer.add_manifests([*before.manifests(table.io), manifest])
    snapshot = Snapshot(
        snapshot_id=snapshot_id, parent_snapshot_id=before.snapshot_id,
        sequence_number=sequence, timestamp_ms=int(time.time() * 1000),
        manifest_list=manifest_list, summary=Summary(operation=Operation.DELETE),
        schema_id=table.schema().schema_id,
    )
    with table.transaction() as tx:
        tx._apply((
            AddSnapshotUpdate(snapshot=snapshot),
            SetSnapshotRefUpdate(ref_name="main", type=SnapshotRefType.BRANCH, snapshot_id=snapshot_id),
        ))
    return table, before.snapshot_id, snapshot_id


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("output", type=Path)
    parser.add_argument("warehouse")
    parser.add_argument("--rest-uri", required=True)
    args = parser.parse_args()
    props = {
        "s3.endpoint": os.environ["AWS_ENDPOINT"],
        "s3.region": os.environ["AWS_REGION"],
        "s3.access-key-id": os.environ["AWS_ACCESS_KEY_ID"],
        "s3.secret-access-key": os.environ["AWS_SECRET_ACCESS_KEY"],
        "warehouse": args.warehouse,
    }
    catalog = load_catalog("fixture", type="rest", uri=args.rest_uri, **props)
    table, previous, current = build_table(catalog, args.warehouse)
    args.output.write_text(json.dumps({
        "uri": args.rest_uri, "warehouse": args.warehouse,
        "table": ".".join(table.name()), "previous": previous, "current": current,
    }))


if __name__ == "__main__":
    main()
