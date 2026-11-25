"""Apache Hudi maintenance (compaction, cleaning, time-travel, incremental reads, and Day-2 Gold seed)."""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession

from lakehouse.hudi_tables import hudi_table_exists, seed_gold_tables
from lakehouse.spark_hudi_session import get_lakehouse_paths, get_spark_session


def _register_hudi_table(spark: SparkSession, table_path: str) -> str:
    """Register an external Hudi path in the Spark catalog so SQL procedures can target it."""
    table_name = table_path.rstrip("/").split("/")[-1]
    spark.sql(f"CREATE TABLE IF NOT EXISTS `{table_name}` USING hudi LOCATION '{table_path}'")
    return table_name


def show_commits(spark: SparkSession, table_path: str, limit: int = 10) -> DataFrame:
    """List recent Apache Hudi commits on a table."""
    table_name = _register_hudi_table(spark, table_path)
    commits_df = spark.sql(f"CALL show_commits(table => '{table_name}', limit => {limit})")
    commits_df.show(truncate=False)
    return commits_df


def run_compaction(spark: SparkSession, table_path: str, max_delta_commits: int = 1) -> None:
    """Schedule and execute compaction for a Merge-on-Read Hudi table."""
    if not hudi_table_exists(spark, table_path):
        print(f"[Hudi] Skipping compaction: table not found at {table_path}")
        return
    print(f"[Hudi] Scheduling & running compaction on {table_path}...")
    compaction_opts = (
        f"hoodie.compact.inline.max.delta.commits={max_delta_commits},"
        "hoodie.write.concurrency.mode=single_writer,"
        "hoodie.write.lock.provider=org.apache.hudi.client.transaction.lock.InProcessLockProvider"
    )
    scheduled = spark.sql(
        f"CALL run_compaction(op => 'schedule', path => '{table_path}', options => '{compaction_opts}')"
    )
    rows = scheduled.collect()
    scheduled.show(truncate=False)
    if rows:
        spark.sql(
            f"CALL run_compaction(op => 'run', path => '{table_path}', options => '{compaction_opts}')"
        ).show(truncate=False)
    else:
        print("[Hudi] No pending compaction instants to execute.")


def run_clean(spark: SparkSession, table_path: str, retain_commits: int = 5) -> None:
    """Clean obsolete Hudi file versions while retaining the latest commits."""
    if not hudi_table_exists(spark, table_path):
        print(f"[Hudi] Skipping clean: table not found at {table_path}")
        return
    table_name = _register_hudi_table(spark, table_path)
    print(f"[Hudi] Cleaning {table_path} (table={table_name}, retain_commits={retain_commits})...")
    clean_opts = (
        "hoodie.write.concurrency.mode=single_writer,"
        "hoodie.write.lock.provider=org.apache.hudi.client.transaction.lock.InProcessLockProvider"
    )
    spark.sql(
        f"CALL run_clean(table => '{table_name}', retain_commits => {retain_commits}, options => '{clean_opts}')"
    ).show(truncate=False)


def query_incremental(spark: SparkSession, table_path: str, begin_instant: str = "000") -> DataFrame:
    """Read only records changed since a Hudi commit instant."""
    df = (
        spark.read.format("hudi")
        .option("hoodie.datasource.query.type", "incremental")
        .option("hoodie.datasource.read.begin.instanttime", begin_instant)
        .load(table_path)
    )
    print(f"[Hudi] Incremental rows since instant '{begin_instant}': {df.count()}")
    return df


def query_time_travel(spark: SparkSession, table_path: str, as_of_instant: str) -> DataFrame:
    """Query the state of a Hudi table at a historical commit timestamp."""
    df = (
        spark.read.format("hudi")
        .option("as.of.instant", as_of_instant)
        .load(table_path)
    )
    print(f"[Hudi] Time-travel rows at instant '{as_of_instant}': {df.count()}")
    return df


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="IL-07 Apache Hudi Lakehouse maintenance.")
    parser.add_argument(
        "--action",
        choices=["all", "compact", "clean", "commits", "incremental", "time-travel", "seed-gold"],
        default="all",
        help="Maintenance operation to run.",
    )
    parser.add_argument("--table-path", default=None, help="Target Hudi path (defaults to Silver tickets_hudi).")
    parser.add_argument("--retain-commits", type=int, default=5, help="Commits to retain when cleaning.")
    parser.add_argument("--begin-instant", default="000", help="Start instant for incremental reads.")
    parser.add_argument("--as-of-instant", default=None, help="Target instant for time-travel queries.")
    parser.add_argument("--seed-rows", type=int, default=500, help="Number of rows for seed-gold.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    """CLI entrypoint for Airflow weekly DAG and manual lakehouse maintenance."""
    args = parse_args(argv)
    paths = get_lakehouse_paths()
    target = args.table_path or paths["silver_tickets"]

    spark = get_spark_session(app_name="IL07-Hudi-Maintenance")
    try:
        if args.action in {"all", "compact"}:
            run_compaction(spark, target)
        if args.action in {"all", "clean"}:
            run_clean(spark, target, retain_commits=args.retain_commits)
        if args.action in {"all", "commits"} and hudi_table_exists(spark, target):
            show_commits(spark, target)
        if args.action == "incremental" and hudi_table_exists(spark, target):
            query_incremental(spark, target, begin_instant=args.begin_instant).show(10, truncate=False)
        if args.action == "time-travel":
            if not args.as_of_instant:
                raise ValueError("--as-of-instant is required for --action time-travel")
            query_time_travel(spark, target, as_of_instant=args.as_of_instant).show(10, truncate=False)
        if args.action == "seed-gold":
            counts = seed_gold_tables(spark, num_tickets=args.seed_rows)
            print(f"[Hudi] Seeded Gold tables: {counts}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
