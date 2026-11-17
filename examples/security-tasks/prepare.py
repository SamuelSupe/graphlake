"""Prepare a disposable GraphLake model and fixture policy in a new directory."""
import argparse
import json
from pathlib import Path
import shutil
import sqlite3


def ident(value):
    return '"' + value.replace('"', '""') + '"'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    directory = parser.parse_args().directory
    directory.mkdir(parents=True)
    example = Path(__file__).resolve().parent
    shutil.copyfile(example / "model.yaml", directory / "security.yaml")
    shutil.copyfile(example / "fixture.json", directory / ".tasks-security.json")
    suite = json.loads((example / "fixture.json").read_text())
    with sqlite3.connect(directory / "security.sqlite") as db:
        for reference, table in suite["tables"].items():
            name = ident(reference.split(".", 1)[1])
            columns = [column["name"] for column in table["columns"]]
            declarations = ", ".join(
                ident(column["name"]) + (" INTEGER" if column["data_type"] == "Int64" else " TEXT")
                for column in table["columns"]
            )
            placeholders = ", ".join("?" for _ in columns)
            db.execute(f"CREATE TABLE {name} ({declarations})")
            db.executemany(
                f"INSERT INTO {name} VALUES ({placeholders})",
                ([row[column] for column in columns] for row in table["rows"]),
            )


if __name__ == "__main__":
    main()
