import csv
import sqlite3
import tempfile
import unittest
from pathlib import Path

from backend.load_results import ResultImportError, load_results


HEADER = ["user_id", "followers", "followees"]


def write_part(path: Path, rows: list[list[object]], header: list[str] = HEADER) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(header)
        writer.writerows(rows)


def database_rows(path: Path) -> list[tuple[int, int, int]]:
    with sqlite3.connect(path) as connection:
        return connection.execute(
            "SELECT user_id, followers, followees FROM user_counts ORDER BY user_id"
        ).fetchall()


class ResultLoaderTest(unittest.TestCase):
    def test_imports_every_part_and_ignores_non_part_csv_files(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            results = root / "results"
            results.mkdir()
            write_part(results / "part-00001.csv", [[10, 4, 2]])
            write_part(results / "part-00000.csv", [[2, 0, 1]])
            write_part(results / "summary.csv", [[999, 999, 999]])
            database = root / "state" / "users.db"

            imported = load_results(results, database)

            self.assertEqual(imported, 2)
            self.assertEqual(database_rows(database), [(2, 0, 1), (10, 4, 2)])
            with sqlite3.connect(database) as connection:
                columns = connection.execute("PRAGMA table_info(user_counts)").fetchall()
            self.assertEqual([column[1] for column in columns], HEADER)
            self.assertEqual(columns[0][5], 1, "user_id must be the primary key")

    def test_invalid_reload_preserves_previous_complete_table(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            results = root / "results"
            results.mkdir()
            part = results / "part-00000.csv"
            database = root / "users.db"
            write_part(part, [[7, 8, 9]])
            load_results(results, database)

            write_part(part, [[11, 2, 3], [12, -1, 4]])
            with self.assertRaisesRegex(ResultImportError, "non-negative integer"):
                load_results(results, database)

            self.assertEqual(database_rows(database), [(7, 8, 9)])

    def test_duplicate_user_ids_preserve_previous_table(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            results = root / "results"
            results.mkdir()
            database = root / "users.db"
            write_part(results / "part-00000.csv", [[1, 2, 3]])
            load_results(results, database)

            write_part(results / "part-00001.csv", [[1, 3, 4]])
            with self.assertRaises(sqlite3.IntegrityError):
                load_results(results, database)

            self.assertEqual(database_rows(database), [(1, 2, 3)])

    def test_rejects_missing_parts_and_non_exact_header(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            results = root / "results"
            results.mkdir()
            with self.assertRaisesRegex(ResultImportError, r"no part-\*\.csv"):
                load_results(results, root / "users.db")

            write_part(
                results / "part-00000.csv",
                [[1, 2, 3]],
                header=["user_id", "followees", "followers"],
            )
            with self.assertRaisesRegex(ResultImportError, "expected CSV header"):
                load_results(results, root / "users.db")

    def test_expected_row_mismatch_preserves_previous_table(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            results = root / "results"
            results.mkdir()
            database = root / "users.db"
            write_part(results / "part-00000.csv", [[7, 8, 9]])
            load_results(results, database)

            write_part(results / "part-00000.csv", [[10, 1, 2], [11, 3, 4]])
            with self.assertRaisesRegex(ResultImportError, "analysis reported 3 users"):
                load_results(results, database, expected_rows=3)

            self.assertEqual(database_rows(database), [(7, 8, 9)])


if __name__ == "__main__":
    unittest.main()
