"""Clean a Twitter relationship dataset and calculate per-user network totals.

The input is a text file in which each line has the form
``follower_id followee_id`` and means that the first user follows the second.
The PySpark analysis performs the following steps:

1. Parse exactly two non-negative integer IDs from each line and reject blank,
   malformed, negative, extra-column, and 64-bit-overflowing values.
2. Record how many valid rows are self-follows, then exclude those self-edges
   from the relationship graph.
3. Deduplicate directed edges. Repeated ``A -> B`` rows count once, while
   ``A -> B`` and ``B -> A`` remain two different relationships.
4. Preserve every valid user ID, including users that occur only in a removed
   self-follow, so users with no remaining edges receive zero counts.
5. Count followers by grouping on ``followee_id`` and count followees by
   grouping on ``follower_id``.
6. Verify that the sum of all follower counts and the sum of all followee
   counts both equal the number of distinct non-self relationships.
7. Publish headered CSV part files atomically, keeping the previous valid output
   if computation or publication fails, and optionally write a JSON statistics
   summary using the same complete-before-replace approach.

The resulting CSV contains ``user_id``, ``followers``, and ``followees`` for
every valid user. ``AnalysisStats`` separately reports the raw, rejected,
self-follow, duplicate, distinct-edge, user, and aggregate count totals.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import tempfile
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Sequence

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, functions as F


LOGGER = logging.getLogger("twitter-analysis")
# Capture exactly two non-negative integer IDs while allowing surrounding and
# separating whitespace. Values that do not fit in a Spark ``long`` are later
# rejected by ``try_cast``.
ROW_PATTERN = r"^\s*([0-9]+)\s+([0-9]+)\s*$"


@dataclass(frozen=True)
class AnalysisStats:
    raw_rows: int
    malformed_rows: int
    self_follows: int
    repeated_non_self_edges: int
    distinct_non_self_edges: int
    users: int
    followers_sum: int
    followees_sum: int


def _parse_rows(lines: DataFrame) -> DataFrame:
    """Parse lines without a Python UDF and mark invalid/overflowing IDs."""
    # Spark SQL expressions remain inside Spark's optimized execution engine;
    # using a Python UDF here would add unnecessary serialization overhead.
    follower_text = F.regexp_extract("value", ROW_PATTERN, 1)
    followee_text = F.regexp_extract("value", ROW_PATTERN, 2)

    parsed = (
        lines.withColumn("_matches", F.col("value").rlike(ROW_PATTERN))
        .withColumn("follower_id", follower_text.try_cast("long"))
        .withColumn("followee_id", followee_text.try_cast("long"))
    )
    return parsed.withColumn(
        "is_valid",
        F.col("_matches")
        & F.col("follower_id").isNotNull()
        & F.col("followee_id").isNotNull(),
    )


def _publish_csv(results: DataFrame, output: str) -> None:
    """Publish a completed Spark CSV directory while preserving old output on failure."""
    output_uri = output.rstrip("/")
    if not output_uri:
        raise ValueError("output path must not be empty or the filesystem root")

    token = uuid.uuid4().hex
    temporary_uri = f"{output_uri}.tmp-{token}"
    backup_uri = f"{output_uri}.backup-{token}"
    jvm = results.sparkSession.sparkContext._jvm
    hadoop_conf = results.sparkSession.sparkContext._jsc.hadoopConfiguration()
    output_path = jvm.org.apache.hadoop.fs.Path(output_uri)
    temporary_path = jvm.org.apache.hadoop.fs.Path(temporary_uri)
    backup_path = jvm.org.apache.hadoop.fs.Path(backup_uri)
    filesystem = output_path.getFileSystem(hadoop_conf)
    moved_old_output = False

    try:
        # Write the complete new dataset under a unique temporary name first.
        # The published output is untouched if Spark fails during this write.
        filesystem.delete(temporary_path, True)
        results.write.mode("errorifexists").option("header", True).csv(temporary_uri)

        # Hadoop filesystems rename directories atomically. Move any current
        # result aside, promote the completed temporary result, then remove the
        # backup only after the promotion succeeds.
        if filesystem.exists(output_path):
            filesystem.delete(backup_path, True)
            if not filesystem.rename(output_path, backup_path):
                raise RuntimeError(f"could not move existing output to {backup_uri}")
            moved_old_output = True

        if not filesystem.rename(temporary_path, output_path):
            raise RuntimeError(f"could not publish temporary output to {output_uri}")

        if moved_old_output:
            filesystem.delete(backup_path, True)
    except Exception:
        # A failed promotion restores the previous valid result whenever it had
        # already been moved to the backup location.
        filesystem.delete(temporary_path, True)
        if moved_old_output and not filesystem.exists(output_path):
            if not filesystem.rename(backup_path, output_path):
                LOGGER.error("Failed to restore previous output from %s", backup_uri)
        raise


def run_analysis(spark: SparkSession, input_path: str, output_path: str) -> AnalysisStats:
    """Run the analysis, validate its invariants, and publish the CSV result."""
    # Several actions reuse the parsed data. Persisting it on disk avoids
    # reparsing the source for every count while keeping JVM memory pressure low.
    parsed = _parse_rows(spark.read.text(input_path)).persist(StorageLevel.DISK_ONLY)

    try:
        # Calculate the total and rejected row counts in a single Spark action.
        row_totals = parsed.agg(
            F.count(F.lit(1)).alias("raw_rows"),
            F.sum(F.when(~F.col("is_valid"), 1).otherwise(0)).alias("malformed_rows"),
        ).first()
        raw_rows = int(row_totals.raw_rows)
        malformed_rows = int(row_totals.malformed_rows or 0)

        if malformed_rows:
            LOGGER.warning("Rejected %d malformed row(s); showing up to 20", malformed_rows)
            parsed.filter(~F.col("is_valid")).select("value").show(20, truncate=False)

        valid_pairs = parsed.filter("is_valid").select("follower_id", "followee_id").persist(
            StorageLevel.DISK_ONLY
        )
        try:
            # Count self-follows before filtering them so the cleaning process
            # remains auditable in the returned statistics.
            relationship_totals = valid_pairs.agg(
                F.count(F.lit(1)).alias("valid_rows"),
                F.sum(
                    F.when(F.col("follower_id") == F.col("followee_id"), 1).otherwise(0)
                ).alias("self_follows"),
            ).first()
            valid_rows = int(relationship_totals.valid_rows)
            self_follows = int(relationship_totals.self_follows or 0)

            # Build the user universe from both columns before removing
            # self-follows. This retains a user who appears only in a self-edge,
            # whose final follower and followee values should both be zero.
            users = (
                valid_pairs.select(F.col("follower_id").alias("user_id"))
                .union(valid_pairs.select(F.col("followee_id").alias("user_id")))
                .distinct()
            )
            # Relationships are directed: A->B and B->A are different edges.
            # Exact duplicate rows are collapsed after self-follows are removed.
            edges = (
                valid_pairs.filter(F.col("follower_id") != F.col("followee_id"))
                .distinct()
                .persist(StorageLevel.DISK_ONLY)
            )
            try:
                distinct_edges = edges.count()
                # Every valid non-self row is either the first occurrence of a
                # directed edge or a repeated occurrence of that same edge.
                repeated_edges = valid_rows - self_follows - distinct_edges

                # Group by the destination to count followers and by the source
                # to count followees. Both outputs are normalized to ``user_id``
                # so they can be joined to the complete user universe.
                followers = edges.groupBy("followee_id").count().select(
                    F.col("followee_id").alias("user_id"), F.col("count").alias("followers")
                )
                followees = edges.groupBy("follower_id").count().select(
                    F.col("follower_id").alias("user_id"), F.col("count").alias("followees")
                )
                results = (
                    users.join(followers, "user_id", "left")
                    .join(followees, "user_id", "left")
                    # Users absent from one side of the graph have a zero count,
                    # not a missing value.
                    .fillna(0, subset=["followers", "followees"])
                    .select(
                        F.col("user_id").cast("long"),
                        F.col("followers").cast("long"),
                        F.col("followees").cast("long"),
                    )
                    .persist(StorageLevel.DISK_ONLY)
                )
                try:
                    totals = results.agg(
                        F.count(F.lit(1)).alias("users"),
                        F.sum("followers").alias("followers_sum"),
                        F.sum("followees").alias("followees_sum"),
                    ).first()
                    user_count = int(totals.users)
                    followers_sum = int(totals.followers_sum or 0)
                    followees_sum = int(totals.followees_sum or 0)

                    # Every directed edge contributes exactly once to the global
                    # follower total and once to the global followee total.
                    if followers_sum != distinct_edges or followees_sum != distinct_edges:
                        raise RuntimeError(
                            "count invariant failed: "
                            f"edges={distinct_edges}, followers_sum={followers_sum}, "
                            f"followees_sum={followees_sum}"
                        )

                    # Publish only after all accounting invariants have passed.
                    _publish_csv(results, output_path)
                finally:
                    # Release persisted intermediates even when validation or
                    # publication raises an exception.
                    results.unpersist()
            finally:
                edges.unpersist()
        finally:
            valid_pairs.unpersist()
    finally:
        parsed.unpersist()

    stats = AnalysisStats(
        raw_rows=raw_rows,
        malformed_rows=malformed_rows,
        self_follows=self_follows,
        repeated_non_self_edges=repeated_edges,
        distinct_non_self_edges=distinct_edges,
        users=user_count,
        followers_sum=followers_sum,
        followees_sum=followees_sum,
    )
    LOGGER.info(
        "Analysis complete: raw_rows=%d malformed_rows=%d self_follows=%d "
        "repeated_non_self_edges=%d distinct_non_self_edges=%d users=%d "
        "followers_sum=%d followees_sum=%d",
        stats.raw_rows,
        stats.malformed_rows,
        stats.self_follows,
        stats.repeated_non_self_edges,
        stats.distinct_non_self_edges,
        stats.users,
        stats.followers_sum,
        stats.followees_sum,
    )
    return stats


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, help="Spark-readable input text file")
    parser.add_argument("--output", required=True, help="Published Spark CSV directory")
    parser.add_argument(
        "--master",
        default=None,
        help="Optional Spark master (for example, local[*]); defaults to Spark configuration",
    )
    parser.add_argument(
        "--stats-output",
        default=None,
        help="optional local JSON path for the completed AnalysisStats record",
    )
    return parser.parse_args(argv)


def _write_stats(path: str, stats: AnalysisStats) -> None:
    """Write statistics by replacing the destination with a complete temp file."""
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        dir=destination.parent, prefix=f".{destination.name}.", suffix=".tmp"
    )
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(asdict(stats), handle, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        # os.replace is atomic on the same filesystem, so readers never observe
        # a partially written statistics document.
        os.replace(temporary_name, destination)
    except Exception:
        try:
            os.unlink(temporary_name)
        except FileNotFoundError:
            pass
        raise


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    builder = SparkSession.builder.appName("twitter-follower-analysis")
    if args.master:
        builder = builder.master(args.master)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        stats = run_analysis(spark, args.input, args.output)
        if args.stats_output:
            _write_stats(args.stats_output, stats)
    finally:
        # This process is one ECS/local job. Stopping Spark here also terminates
        # its JVM so no analysis compute remains active between requested runs.
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

