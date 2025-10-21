#!/usr/bin/env python3
import argparse
import os
from pathlib import Path
from textwrap import dedent

import pandas as pd

def build_spark(master_url: str):
    from pyspark.sql import SparkSession
    return (
        SparkSession.builder
        .appName("DSAN6000-Problem2-ClusterUsage")
        .master(master_url)
        .config("spark.sql.files.ignoreCorruptFiles", "true")
        .config("spark.sql.files.ignoreMissingFiles", "true")
        .config("spark.sql.ansi.enabled", "false")  # tolerate bad timestamp strings
        # S3A support
        .config("spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.AbstractFileSystem.s3a.impl", "org.apache.hadoop.fs.s3a.S3A")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.InstanceProfileCredentialsProvider,com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
        .getOrCreate()
    )

def write_stats(stats_path: Path, timeline_df: pd.DataFrame, cluster_summary_df: pd.DataFrame):
    total_clusters = cluster_summary_df["cluster_id"].nunique()
    total_apps = timeline_df["application_id"].nunique()
    avg_per_cluster = (total_apps / total_clusters) if total_clusters else 0
    top = (
        cluster_summary_df.sort_values("num_applications", ascending=False)
        [["cluster_id", "num_applications"]].head(10)
    )
    lines = [
        f"Total unique clusters: {total_clusters}",
        f"Total applications: {total_apps}",
        f"Average applications per cluster: {avg_per_cluster:.2f}",
        "",
        "Most heavily used clusters:",
    ]
    for _, r in top.iterrows():
        lines.append(f"  Cluster {r.cluster_id}: {int(r.num_applications)} applications")
    stats_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

def make_plots(out_dir: Path, timeline_df: pd.DataFrame, cluster_summary_df: pd.DataFrame):
    import matplotlib.pyplot as plt
    try:
        import seaborn as sns
        use_sns = True
    except Exception:
        use_sns = False

    # Bar chart
    bar_png = out_dir / "problem2_bar_chart.png"
    cs = cluster_summary_df.sort_values("num_applications", ascending=False)
    plt.figure()
    if use_sns:
        import seaborn as sns
        ax = sns.barplot(data=cs, x="cluster_id", y="num_applications")
        ax = plt.gca()
    else:
        plt.bar(cs["cluster_id"].astype(str), cs["num_applications"])
    plt.xlabel("Cluster ID"); plt.ylabel("Applications"); plt.title("Applications per Cluster")
    for i, v in enumerate(cs["num_applications"].tolist()):
        plt.text(i, v, str(int(v)), ha="center", va="bottom", fontsize=8)
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout(); plt.savefig(bar_png, dpi=150); plt.close()

    # Density plot for largest cluster
    dens_png = out_dir / "problem2_density_plot.png"
    if not cs.empty:
        largest_cluster = cs.iloc[0]["cluster_id"]
        subset = timeline_df[timeline_df["cluster_id"] == largest_cluster].copy()
        dur = (pd.to_datetime(subset["end_time"]) - pd.to_datetime(subset["start_time"])).dt.total_seconds()
        dur = dur[dur.notna() & (dur >= 0)]
        plt.figure()
        if use_sns:
            import seaborn as sns
            sns.histplot(dur, bins=50, stat="density")
            try:
                sns.kdeplot(dur)
            except Exception:
                pass
        else:
            plt.hist(dur, bins=50, density=True)
        plt.xscale("log")
        plt.xlabel("Job Duration (seconds, log scale)")
        plt.ylabel("Density")
        plt.title(f"Duration Distribution — Cluster {largest_cluster} (n={len(dur)})")
        plt.tight_layout(); plt.savefig(dens_png, dpi=150); plt.close()

def run_full_spark(master_url: str, net_id: str, out_dir: Path):
    from pyspark.sql.functions import (
        regexp_extract, col, input_file_name, to_timestamp, length, trim, when
    )
    spark = build_spark(master_url)

    bucket_prefix = f"s3a://{net_id}-assignment-spark-cluster-logs/data/"

    logs_df = (
        spark.read
        .option("recursiveFileLookup", "true")
        .text(bucket_prefix)
        .withColumn("file_path", input_file_name())
    )

    # Extract pieces; parse timestamp only when non-empty
    ts_str = regexp_extract(col("value"), r"^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})", 1).alias("ts_str")

    parsed = logs_df.select(
        regexp_extract(col("file_path"), r"application_(\d+_\d+)", 0).alias("application_id"),
        regexp_extract(col("file_path"), r"application_(\d+)_\d+", 1).alias("cluster_id"),
        regexp_extract(col("file_path"), r"application_\d+_(\d+)", 1).alias("app_number"),
        ts_str,
    ).withColumn(
        "timestamp",
        when(length(trim(col("ts_str"))) > 0, to_timestamp(col("ts_str"), "yy/MM/dd HH:mm:ss"))
        .otherwise(None)
    )

    filtered = parsed.where((length(trim(col("application_id"))) > 0) & col("timestamp").isNotNull())

    # Timeline per app (min/max timestamps)
    from pyspark.sql.functions import min as smin, max as smax
    timeline_sdf = (
        filtered.groupBy("cluster_id", "application_id", "app_number")
        .agg(smin("timestamp").alias("start_time"), smax("timestamp").alias("end_time"))
    )

    # To pandas & write CSVs
    timeline_pdf = timeline_sdf.orderBy("cluster_id", "app_number").toPandas()
    timeline_csv = out_dir / "problem2_timeline.csv"
    timeline_pdf.to_csv(timeline_csv, index=False)

    cs_pdf = (
        timeline_pdf.groupby("cluster_id", as_index=False)
        .agg(
            num_applications=("application_id", "nunique"),
            cluster_first_app=("start_time", "min"),
            cluster_last_app=("end_time", "max"),
        )
        .sort_values("num_applications", ascending=False)
    )
    cs_csv = out_dir / "problem2_cluster_summary.csv"
    cs_pdf.to_csv(cs_csv, index=False)

    stats_txt = out_dir / "problem2_stats.txt"
    write_stats(stats_txt, timeline_pdf, cs_pdf)

    make_plots(out_dir, timeline_pdf, cs_pdf)

    spark.stop()
    print("[OK] Wrote:")
    print(" ", timeline_csv)
    print(" ", cs_csv)
    print(" ", stats_txt)
    print(" ", out_dir / "problem2_bar_chart.png")
    print(" ", out_dir / "problem2_density_plot.png")

def run_skip_spark(out_dir: Path):
    timeline_csv = out_dir / "problem2_timeline.csv"
    cs_csv = out_dir / "problem2_cluster_summary.csv"
    assert timeline_csv.exists() and cs_csv.exists(), "Missing CSVs in out_dir for --skip-spark"
    timeline_df = pd.read_csv(timeline_csv)
    cs_df = pd.read_csv(cs_csv)
    for c in ["start_time", "end_time"]:
        if c in timeline_df.columns:
            timeline_df[c] = pd.to_datetime(timeline_df[c], errors="coerce")
    write_stats(out_dir / "problem2_stats.txt", timeline_df, cs_df)
    make_plots(out_dir, timeline_df, cs_df)
    print("[OK] Regenerated visualizations and stats from existing CSVs.")

def main():
    parser = argparse.ArgumentParser(description="Problem 2: Cluster Usage Analysis")
    parser.add_argument("master_url", nargs="?", default=None,
                        help="spark://<MASTER_PRIVATE_IP>:7077 (omit when using --skip-spark)")
    parser.add_argument("--net-id", help="Your NetID (used for S3 bucket name, required unless --skip-spark)", default=None)
    parser.add_argument("--skip-spark", action="store_true", help="Skip Spark read; rebuild plots/stats from existing CSVs")
    parser.add_argument("--output-dir", default=str(Path.home() / "spark-cluster"),
                        help="Output directory (default: ~/spark-cluster)")
    args = parser.parse_args()

    out_dir = Path(os.path.expanduser(args.output_dir))
    out_dir.mkdir(parents=True, exist_ok=True)

    if args.skip_spark:
        run_skip_spark(out_dir)
    else:
        if not args.master_url or not args.net_id:
            raise SystemExit("Provide master_url and --net-id (or use --skip-spark).")
        run_full_spark(args.master_url, args.net_id, out_dir)

if __name__ == "__main__":
    main()