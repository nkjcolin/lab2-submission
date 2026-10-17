"""Load published Spark CSV result parts into SQLite atomically."""

from __future__ import annotations

import argparse
import csv
import os
import re
import sqlite3
from collections.abc import Iterator, Sequence
from pathlib import Path


REQUIRED_COLUMNS = ("user_id", "followers", "followees")
NONNEGATIVE_INTEGER = re.compile(r"[0-9]+")
SQLITE_INTEGER_MAX = 2**63 - 1
DEFAULT_RESULT_DIR = "/data/results"
DEFAULT_DB_PATH = "/data/state/users.db"


class ResultImportError(ValueError):
    """Raised when published analysis results do not satisfy the CSV contract."""


def _part_files(result_dir: Path) -> list[Path]:
    if not result_dir.is_dir():
        raise ResultImportError(f"result directory does not exist: {result_dir}")

    files = sorted(path for path in result_dir.glob("part-*.csv") if path.is_file())
    if not files:
        raise ResultImportError(f"no part-*.csv files found in {result_dir}")
    return files


def _parse_value(value: str | None, column: str, part_file: Path, line: int) -> int:
    if value is None or NONNEGATIVE_INTEGER.fullmatch(value) is None:
        raise ResultImportError(
            f"{part_file}:{line}: {column} must be a non-negative integer"
        )

    number = int(value)
    if number > SQLITE_INTEGER_MAX:
        raise ResultImportError(
            f"{part_file}:{line}: {column} exceeds SQLite's signed integer range"
        )
    return number


def _rows(part_files: Sequence[Path]) -> Iterator[tuple[int, int, int]]:
    for part_file in part_files:
        try:
            handle = part_file.open(newline="", encoding="utf-8")
        except OSError as exc:
            raise ResultImportError(f"could not read {part_file}: {exc}") from exc

        with handle:
            reader = csv.DictReader(handle)
            if tuple(reader.fieldnames or ()) != REQUIRED_COLUMNS:
                found = ",".join(reader.fieldnames or ()) or "<missing>"
                expected = ",".join(REQUIRED_COLUMNS)
                raise ResultImportError(
                    f"{part_file}: expected CSV header {expected}; found {found}"
                )

            for line, row in enumerate(reader, start=2):
                if None in row:
                    raise ResultImportError(f"{part_file}:{line}: row has extra columns")
                yield (
                    _parse_value(row["user_id"], "user_id", part_file, line),
                    _parse_value(row["followers"], "followers", part_file, line),
                    _parse_value(row["followees"], "followees", part_file, line),
                )


def load_results(
    result_dir: str | Path,
    db_path: str | Path,
    *,
    expected_rows: int | None = None,
) -> int:
    """Replace ``user_counts`` with every row from the published CSV parts.

    Validation and the table replacement occur before the transaction commits. If
    reading, validation, insertion, or the table swap fails, SQLite rolls the whole
    transaction back and the previously published table remains available.
    """

    result_path = Path(result_dir)
    database_path = Path(db_path)
    part_files = _part_files(result_path)
    database_path.parent.mkdir(parents=True, exist_ok=True)

    connection = sqlite3.connect(database_path)
    try:
        connection.execute("BEGIN IMMEDIATE")
        connection.execute("DROP TABLE IF EXISTS user_counts_staging")
        connection.execute(
            """
            CREATE TABLE user_counts_staging (
                user_id INTEGER PRIMARY KEY,
                followers INTEGER NOT NULL CHECK (followers >= 0),
                followees INTEGER NOT NULL CHECK (followees >= 0)
            )
            """
        )

        imported = 0
        batch: list[tuple[int, int, int]] = []
        for row in _rows(part_files):
            batch.append(row)
            if len(batch) == 1000:
                connection.executemany(
                    "INSERT INTO user_counts_staging VALUES (?, ?, ?)", batch
                )
                imported += len(batch)
                batch.clear()
        if batch:
            connection.executemany(
                "INSERT INTO user_counts_staging VALUES (?, ?, ?)", batch
            )
            imported += len(batch)

        if expected_rows is not None and imported != expected_rows:
            raise ResultImportError(
                f"imported {imported} rows; analysis reported {expected_rows} users"
            )

        connection.execute("DROP TABLE IF EXISTS user_counts")
        connection.execute("ALTER TABLE user_counts_staging RENAME TO user_counts")
        connection.commit()
        return imported
    except Exception:
        if connection.in_transaction:
            connection.rollback()
        raise
    finally:
        connection.close()


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--results",
        default=os.environ.get("RESULT_DIR", DEFAULT_RESULT_DIR),
        help="directory containing Spark part-*.csv files (default: RESULT_DIR)",
    )
    parser.add_argument(
        "--database",
        default=os.environ.get("DB_PATH", DEFAULT_DB_PATH),
        help="SQLite database path (default: DB_PATH)",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    imported = load_results(args.results, args.database)
    print(f"Imported {imported} users from {args.results} into {args.database}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
