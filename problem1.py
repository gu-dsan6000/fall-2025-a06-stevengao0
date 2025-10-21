#!/usr/bin/env python3
import argparse
import os
from pathlib import Path
from textwrap import dedent

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    regexp_extract, col, input_file_name, to_timestamp, rand, trim, length
)

def build_spark(master_url: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName("DSAN6000-Problem1-LogLevelDistribution")
        .master(master_url)
        .config("spark.sql.files.ignoreCorruptFiles", "true")
        .config("spark.sql.files.ignoreMissingFiles", "true")
        .getOrCreate()
    )

def main():
    parser = argparse.ArgumentParser(
        description="Problem 1: Log Level Distribution on Spark logs"
    )
    parser.add_argument("master_url", help="spark://<MASTER_PRIVATE_IP>:7077 or local[*]")
    parser.add_argument("--net-id", required=True, help="Your NetID")
    parser.add_argument("--output-dir", default=str(Path.home() / "spark-cluster"),
                        help="Output dir on master (default: ~/spark-cluster)")
    args = parser.parse_args()

    bucket_prefix = f"s3://{args.net_id}-assignment-spark-cluster-logs/data/"

    out_dir = Path(os.path.expanduser(args.output_dir))
    out_dir.mkdir(parents=True, exist_ok=True)
    out_counts = out_dir / "problem1_counts.csv"
    out_sample = out_dir / "problem1_sample.csv"
    out_summary = out_dir / "problem1_summary.txt"

    spark = build_spark(args.master_url)

    logs_df = (
        spark.read
        .option("recursiveFileLookup", "true")
        .text(bucket_prefix)
        .withColumn("file_path", input_file_name())
    )

    total_lines = logs_df.count()

    parsed = logs_df.select(
        regexp_extract(col("value"), r"^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})", 1).alias("timestamp_raw"),
        regexp_extract(col("value"), r"\b(INFO|WARN|ERROR|DEBUG)\b", 1).alias("log_level"),
        regexp_extract(col("value"), r"(?:INFO|WARN|ERROR|DEBUG)\s+([^:]+):", 1).alias("component"),
        col("value").alias("log_entry"),
        col("file_path")
    ).withColumn("timestamp", to_timestamp(col("timestamp_raw"), "yy/MM/dd HH:mm:ss"))

    has_level = parsed.where(length(trim(col("log_level"))) > 0)
    total_with_levels = has_level.count()

    counts_df = has_level.groupBy("log_level").count().orderBy(col("count").desc())
    sample_df = has_level.select("log_entry", "log_level").orderBy(rand()).limit(10)

    counts_pd = counts_df.toPandas()
    counts_pd.to_csv(out_counts, index=False)

    sample_pd = sample_df.toPandas()
    sample_pd.to_csv(out_sample, index=False)

    level_counts = {row["log_level"]: int(row["count"]) for _, row in counts_pd.iterrows()}
    ordered_levels = list(counts_pd.sort_values("count", ascending=False)["log_level"])
    pct = (lambda n: (n / total_with_levels * 100.0) if total_with_levels > 0 else 0.0)

    dist_lines = []
    for lvl in ordered_levels:
        n = level_counts.get(lvl, 0)
        dist_lines.append(f"  {lvl:<5}: {n:>10,} ({pct(n):5.2f}%)")

    summary_txt = dedent(f"""
    Total log lines processed: {total_lines:,}
    Total lines with log levels: {total_with_levels:,}
    Unique log levels found: {len(level_counts)}

    Log level distribution:
    """).strip("\n") + "\n" + "\n".join(dist_lines) + "\n"

    out_summary.write_text(summary_txt, encoding="utf-8")
    print(f"[OK] Wrote:\n  {out_counts}\n  {out_sample}\n  {out_summary}")
    spark.stop()

if __name__ == "__main__":
    main()
