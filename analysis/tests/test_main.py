import csv
import tempfile
import unittest
from pathlib import Path

from pyspark.sql import SparkSession

from analysis.main import AnalysisStats, run_analysis


class ToyGraphAnalysisTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = (
            SparkSession.builder.master("local[2]")
            .appName("toy-graph-analysis-test")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def test_directed_edges_cleaning_counts_and_atomic_replacement(self) -> None:
        rows = [
            "1 2",
            "1 2",  # duplicate ordered edge
            "2 1",  # reverse edge remains distinct
            "2 3",
            "4 2",  # user 4 has zero followers
            "3 3",  # self-follow excluded, user retained
            "9 9",  # self-only user retained with zero counts
            "bad row",
            "-1 2",
            "1 2 3",
            "",
            "9223372036854775808 2",  # larger than a signed Spark long
        ]

        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            input_path = root / "toy.txt"
            output_path = root / "results"
            input_path.write_text("\n".join(rows), encoding="utf-8")
            output_path.mkdir()
            (output_path / "stale.csv").write_text("old output", encoding="utf-8")

            stats = run_analysis(self.spark, str(input_path), str(output_path))

            self.assertEqual(
                stats,
                AnalysisStats(
                    raw_rows=12,
                    malformed_rows=5,
                    self_follows=2,
                    repeated_non_self_edges=1,
                    distinct_non_self_edges=4,
                    users=5,
                    followers_sum=4,
                    followees_sum=4,
                ),
            )
            self.assertFalse((output_path / "stale.csv").exists())

            part_files = sorted(output_path.glob("part-*.csv"))
            self.assertTrue(part_files, "Spark must publish one or more CSV part files")
            records = {}
            for part_file in part_files:
                with part_file.open(newline="", encoding="utf-8") as handle:
                    for row in csv.DictReader(handle):
                        self.assertEqual(set(row), {"user_id", "followers", "followees"})
                        records[int(row["user_id"])] = (
                            int(row["followers"]),
                            int(row["followees"]),
                        )

            self.assertEqual(
                records,
                {
                    1: (1, 1),
                    2: (2, 2),
                    3: (1, 0),
                    4: (0, 1),
                    9: (0, 0),
                },
            )


if __name__ == "__main__":
    unittest.main()
