# Incremental-Refresh

# Ripple-Aware Incremental Join Propagation System

Efficient, scalable Spark-based framework for ripple-aware, incremental data processing across multiple modules connected by joins.

## Overview

Workflows in modern data platforms often require joining multiple frequently-updated modules (PROJECT, CONTRACT, SUPPLIER, etc.). The traditional "full refresh" approach is resource-intensive, processing every record even for minor updates.

This project introduces a **ripple-aware incremental join system** that detects changes and propagates only affected (delta + dependent) records through complex join graphs, significantly optimizing performance and cost.

## Key Features

- **Incremental Data Processing:** Processes only data that changed since the last refresh, avoiding expensive full-table scans.
- **Ripple Propagation:** Automatically traces and processes dependent records across join chains.
- **Dynamic Join Graph:** Handles any number of modules and join relationships (n-table scalability).
- **Bidirectional Joins:** Detects and propagates changes in all directions to cover all impact permutations.
- **Smart Merge:** Aligns schemas and removes redundancies in merged join outputs.
- **Highly Configurable:** Driven by config files for modules, primary keys, and join structures.

## Approach

1. **Change Detection / Prefilter:** Identify rows that have changed using timestamp-based filtering on delta tables.
2. **Temp Modules:** Dynamically generate temp tables/views containing only primary/join keys.
3. **Bidirectional Incremental Joins:** For every join relation, execute joins in both directions (A→B and B→A) to ensure all ripple effects are captured.
4. **Merge Results:** Merge join outputs, align columns, and remove duplicate/overlapping outputs.
5. **Iterative Propagation:** Continue propagation until no new affected records are discovered.
6. **Output Generation:** Produce final result tables/views containing only changed and impacted data.

## Benefits

- **Resource & Cost Savings:** Up to 80-85% fewer rows processed (vs. full refresh).
- **Time Efficiency:** Cuts job durations by processing only relevant data.
- **Modular & Scalable:** Works for any workflow setup—can easily add more modules.
- **Enterprise Ready:** Enhanced traceability, debugging, and correctness validation.

## Code Structure

- **Incremental-refresh.scala:** Contains all Spark/Scala logic for join propagation, smart merge, and iteration.
- **Config Maps:** Define your modules, primary keys, and joins.
- **Reusable Functions:**
  - Prefilter
  - Run Incremental Join
  - Smart Merge Tables
- **Iterative Engine:** Continues join-merge-propagation until no new changes detected.


1. **Setup Spark (e.g., Databricks or local environment).**
2. **Prepare config maps and JSON file for modules & join graph.**
3. **Run the main Scala code.**
4. **Monitor outputs:** Only impacted records are refreshed/available for downstream use.

## Author

**Pooja Maheshwari**
