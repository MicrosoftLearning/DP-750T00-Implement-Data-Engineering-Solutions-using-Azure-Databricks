---
lab:
  index: 13
  title: Diagnose and Fix Spark Performance Issues
  module: Monitor, Troubleshoot, and Optimize Workloads in Azure Databricks
  module-url: https://learn.microsoft.com/training/wwl-databricks/monitor-troubleshoot-optimize-workloads-azure-databricks/
  description: In this lab, you generate synthetic workloads with intentional data skew and excessive shuffle, use the Spark UI to diagnose the performance problems, and apply targeted fixes using broadcast joins, Adaptive Query Execution, and shuffle reduction techniques.
  duration: 45 minutes
  level: 300
  islab: true
  primarytopics:
    - Azure Databricks
---

# Lab 13: Diagnose and Fix Spark Performance Issues

## Introduction

A query that used to complete in seconds now takes minutes. Cluster resources appear busy, yet most tasks finish almost immediately while a handful drag on. These are the hallmarks of **data skew** — and they are often accompanied by **excessive shuffle**, which forces Spark to move large volumes of data across the network between stages.

In this lab you deliberately reproduce both problems so you can observe exactly what they look like in the Spark UI, then apply targeted fixes and confirm the improvement.

| Part | Topic | Where |
|------|-------|--------|
| **Part 1** | Set up the environment and generate data | Notebook |
| **Part 2** | Expose data skew and inspect the Spark UI | Notebook + Lab instructions |
| **Part 3** | Fix the data skew | Notebook |
| **Part 4** | Expose excessive shuffle and inspect the Spark UI | Notebook + Lab instructions |
| **Part 5** | Reduce shuffle overhead | Notebook |

---

## 🤖 Use the Databricks Assistant throughout this lab

You are **expected and encouraged** to use the **Databricks Assistant** for every task in the notebook. 

To open the Databricks Assistant, select the ![assistant-icon](https://raw.githubusercontent.com/MicrosoftLearning/DP-750T00-Implement-Data-Engineering-Solutions-using-Azure-Databricks/refs/heads/main/Allfiles/media/databricks-assistant.svg) on the right side of any notebook cell, or use the keyboard shortcut.

Use it to:
- Understand Spark configuration options (AQE, shuffle partitions, broadcast threshold)
- Generate optimised join and aggregation code
- Interpret Spark UI metrics and stage details
- Explore alternative skew-handling techniques (salting, bucketing)

---

## Prerequisites

- An **Azure Databricks Premium** workspace is already provisioned and you have access to it.
- You have permission to create catalogs and schemas in Unity Catalog.
- Basic familiarity with PySpark DataFrames is helpful.

---

## Create a compute cluster

This lab requires a **classic compute cluster** (not Serverless). Some Spark configuration options used to disable and re-enable Adaptive Query Execution — such as `spark.databricks.optimizer.adaptive.enabled` — are only available on classic clusters.

> ⚠️ **Photon must be disabled.** Photon is Databricks' vectorized query engine. When enabled, it optimises shuffle and skew handling automatically, which masks the performance problems this lab is designed to demonstrate. You will not see skewed task distributions or high shuffle metrics in the Spark UI if Photon is active.

1. In your Databricks workspace, click **Compute** in the left sidebar.
2. Click **Create compute**.
3. Configure the cluster with the following minimal settings:

   | Setting | Value |
   |---------|-------|
   | **Cluster name** | `perf-lab` |
   | **Policy** | Unrestricted |
   | **Single node** | Enabled |
   | **Access mode** | Single user |
   | **Databricks Runtime** | Latest LTS (non-ML) — **make sure to pick a non-Photon runtime** |
   | **Node type** | Choose the smallest available (e.g. `Standard_DS3_v2` or equivalent) |
   | **Auto-terminate** | 30 minutes |

4. Expand **Advanced options**, then click the **Spark** tab. Add the following line to the **Spark config** box to explicitly disable Photon:

   ```
   spark.databricks.photon.enabled false
   ```

5. Click **Create compute** and wait for the cluster to reach the **Running** state before continuing.

---

## Import the notebook

1. In your Databricks workspace, click **Workspace** in the left sidebar.
2. Navigate to or create a folder where you want to store the lab.
3. Click the **⋮** (kebab) menu or right-click the folder, then select **Import**.
4. Choose **URL**, enter the following URL, and click **Import**:
   `https://raw.githubusercontent.com/MicrosoftLearning/DP-750T00-Implement-Data-Engineering-Solutions-using-Azure-Databricks/refs/heads/main/Allfiles/13-monitor-troubleshoot-optimize-workloads-azure-databricks.ipynb`
5. Open the imported notebook and, in the compute selector at the top, select the **`perf-lab`** cluster you created above.

---

## Part 2: Investigate Data Skew in the Spark UI

After running the cells in **Exercise 2** of the notebook, return here and follow the steps below to examine the skewed execution in the Spark UI.

### Open the Spark UI

1. In your Databricks workspace, click **Compute** in the left sidebar, then click the **`perf-lab`** cluster. On the cluster detail page, click the **Spark UI** tab. The Spark UI opens in a new browser tab.
2. You land on the **Jobs** page, which lists all Spark jobs triggered during this session.

### Identify the skewed aggregation job

3. Find the most recently completed jobs. The **groupBy aggregation** from Task 2.1 and the **sort-merge join** from Task 2.2 will appear near the top (newest first).
4. Click on the aggregation job (it will reference stages related to *aggregate* or *hashAggregate*).
5. On the job detail page, look at the **Stages** list. Click on the stage that performed the shuffle — it is typically the one with the most tasks and the longest duration.
6. Scroll down to the **Summary Metrics** table for the tasks in that stage. Focus on the **Duration** row:
   - Note the **Min**, **25th percentile**, **Median**, **75th percentile**, and **Max** values.
   - If **Max** is more than 50% higher than the **75th percentile**, you are seeing data skew — a small number of tasks are processing most of the data.
7. Scroll further down to the **Tasks** section. Sort the list by **Duration** (descending). Observe how one or two tasks have a duration far greater than the rest.

### Identify the skewed join job

8. Click **Jobs** in the Spark UI left navigation to return to the jobs list.
9. Click on the sort-merge join job from Task 2.2 and repeat the analysis. Look for the same pattern: a few tasks with much longer durations than the majority.
10. When you have finished your investigation, return to the notebook and continue with **Exercise 3**.

---

## Part 4: Investigate Excessive Shuffle in the Spark UI

After running the cells in **Exercise 4** of the notebook, return here to investigate the shuffle metrics.

### Locate the shuffle-heavy stages

1. Return to the Spark UI tab (or reopen it from the notebook toolbar).
2. Click the **Stages** tab in the left-hand navigation to see all stages across all jobs.
3. Look at the **Shuffle Read** and **Shuffle Write** columns. The stages triggered by Exercise 4 will show significant data movement in both columns — far more than the actual input data size in many cases.

### Count Exchange nodes in the DAG

4. Click on any stage with high shuffle values to open its detail page.
5. At the top of the stage detail page, expand **DAG Visualization**. Look for nodes labelled **Exchange** — each one represents a full shuffle of the data across the cluster network.
6. Count the total number of Exchange nodes visible across the stages of the Exercise 4 job. Compare this to the minimum number of shuffles that are actually needed for a groupBy → join → sort pipeline.

### Compare Shuffle Read vs Input size

7. Still in the stage detail page, scroll to the **Summary Metrics** section. Compare the **Input Size / Records** with the **Shuffle Read Size / Records**. When shuffle read is significantly larger than the original input, the pipeline is doing more network I/O than necessary.
8. When you have finished your investigation, return to the notebook and continue with **Exercise 5**.
