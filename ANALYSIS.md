# ANALYSIS.md

## Overview

This report presents the results of my Spark Log Analysis assignment on an AWS EC2 Spark cluster. It covers two main tasks:

- **Problem 1 – Log Level Distribution**
- **Problem 2 – Cluster Usage Analysis**

All experiments were run on a 4-node standalone Spark cluster (1 master, 3 workers), processing log data stored in S3. The goal was to analyze logging patterns and cluster usage across multiple Spark applications.

---

## Problem 1 – Log Level Distribution

### Approach

1. **Read raw log lines** from S3 using the Spark text reader with recursive lookup.  
2. **Extract fields** (timestamp, log level, message) using regex.  
3. **Aggregate** counts of log levels (`INFO`, `WARN`, `ERROR`).  
4. **Write outputs**:
   - `problem1_counts.csv`
   - `problem1_sample.csv`
   - `problem1_summary.txt`

### Results

| Log Level | Count | Percentage |
|------------|--------|------------|
| INFO | 27,389,482 | 99.92% |
| ERROR | 11,259 | 0.04% |
| WARN | 9,595 | 0.04% |

**Total lines processed:** 33,236,604  
**Lines with recognized levels:** 27,410,336  
**Unique levels detected:** 3

### Interpretation

The dataset is dominated by **INFO** messages, with very few **WARN** or **ERROR** logs. This distribution is typical for stable Spark clusters where most logs describe normal operational behavior.

---

## Problem 2 – Cluster Usage Analysis

### Objective

Analyze Spark cluster usage across all applications, identify job duration patterns, and visualize workload distribution.

### Method

1. **Extract identifiers** from file paths:
   - `cluster_id`, `application_id`, `app_number`
2. **Parse timestamps** from log lines.  
3. **Aggregate per application**:
   - `start_time = min(timestamp)`  
   - `end_time = max(timestamp)`  
4. **Summarize per cluster** to count total applications and date range.  
5. **Compute durations** and visualize results using `matplotlib` and `seaborn`.

### Key Findings

| Cluster ID | # Applications | First App | Last App |
|-------------|----------------|------------|-----------|
| 1485248649253 | 181 | 2017-01-24 17:00:28 | 2017-07-27 21:45:00 |
| 1472621869829 | 8 | 2016-09-09 07:43:47 | 2016-09-09 10:07:06 |
| 1448006111297 | 2 | 2016-04-07 10:45:21 | 2016-04-07 12:22:08 |
| Others (3 clusters) | 1 each | Various | Various |

**Totals:**
- **Unique clusters:** 6  
- **Applications processed:** 194  
- **Average per cluster:** 32.33  

### Visualization Insights

**1. Applications per Cluster**  
The bar chart shows an extreme imbalance — cluster `1485248649253` handled **181** applications (~93% of total), making it the main production environment.

**2. Duration Distribution (log scale)**  
The density plot for `1485248649253` shows a **right-skewed distribution** with most job durations between **10²–10³ seconds (minutes)** and a long tail up to **10⁴ seconds (hours)**. This reflects typical Spark workload variability, where many short ETL tasks coexist with a few long-running analytical jobs.

---

## Performance Analysis

| Metric | Observation |
|---------|--------------|
| **Problem 1 runtime** | ~3–5 minutes |
| **Problem 2 runtime** | ~10–20 minutes |
| **Cluster size** | 1 master, 3 workers (t3.large) |
| **Data volume** | ~2.8 GB |
| **Optimizations** | Recursive file lookup, lazy timestamp parsing, early aggregation, minimal driver memory use |

---

## Additional Observations

- The dominant cluster behavior suggests centralized scheduling; smaller clusters likely handled testing or ad-hoc runs.
- The long-tail job durations indicate uneven resource use — a common performance tuning target (e.g., improving parallelism or data partitioning).
- Adding `problem2_duration_stats.csv` (with p90/p99 percentiles) would allow deeper insights into latency distribution.

---

## Conclusion

- **Problem 1**: Cluster logs are stable, with INFO logs overwhelmingly prevalent.  
- **Problem 2**: Most Spark workloads run on one primary cluster with occasional short-lived clusters. Job durations are right-skewed, consistent with mixed short/long Spark tasks.  
- **Performance**: Spark-on-EC2 scaled effectively for multi-GB logs, with efficient resource utilization and fault tolerance.