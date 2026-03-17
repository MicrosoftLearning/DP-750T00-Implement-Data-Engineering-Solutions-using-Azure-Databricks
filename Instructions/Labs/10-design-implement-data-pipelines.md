---
lab:
  index: 10
  title: Design and implement data pipelines with Azure Databricks
  module: Design and implement data pipelines with Azure Databricks
  module-url: https://learn.microsoft.com/training/wwl-databricks/design-implement-data-pipelines/
  description: In this lab, you build a medallion architecture pipeline (Bronze → Silver → Gold) for GlobStay hotel booking data, applying cleaning rules such as deduplication, null filtering, and date validation before producing Gold-layer aggregations for property and channel performance. You implement error handling and parameterize notebooks for job orchestration. You then configure a Lakeflow Job in the Azure Databricks UI with sequential task dependencies, retry policies, failure notifications, and an If/else condition task for data quality routing.
  duration: 45 minutes
  level: 300
  islab: true
  primarytopics:
    - Azure Databricks
---

# Lab 10: Design and Implement Data Pipelines

## Introduction

You are a data engineer at **GlobStay**, a hospitality group managing bookings across five hotel properties spanning beach resorts, mountain lodges, city-center inns, and airport transit hotels. GlobStay runs a nightly batch pipeline that ingests raw booking data from property management systems, cleans and validates it, and produces revenue analytics for the leadership team.

In this lab you design and implement that pipeline end-to-end. You will:

- Build a **medallion architecture** (Bronze → Silver → Gold) for hotel booking data
- Write **parameterized, reusable notebook tasks** that can be orchestrated by a Lakeflow Job
- Add **error handling** so individual tasks can fail gracefully and signal their status to downstream tasks
- Configure a **Lakeflow Job** with task dependencies, retry policies, and notifications
- Explore **conditional task flows** (If/else branching) for routing based on data quality results

---

## 🤖 Use the Databricks Assistant throughout this lab

You are expected and encouraged to use the **Databricks Assistant** for every exercise. Use it to get suggestions, explain errors, generate boilerplate, and explore APIs.

To open the Databricks Assistant, select the ![assistant-icon](https://raw.githubusercontent.com/MicrosoftLearning/DP-750T00-Implement-Data-Engineering-Solutions-using-Azure-Databricks/refs/heads/main/Allfiles/media/databricks-assistant.svg) on the right side of any notebook cell, or use the keyboard shortcut.

> **Example prompt:** *"Help me write a PySpark statement that deduplicates a DataFrame on the booking_id column and filters out rows where rate_per_night is less than or equal to zero."*

---

## Prerequisites

- An **Azure Databricks Premium** workspace is already provisioned and you have access to it.
- You have permission to create catalogs and schemas in Unity Catalog.
- Basic familiarity with PySpark and Spark SQL.

---

## Part 1: Notebook exercises (medallion architecture + error handling + parameters)

### Import the notebook

1. In your Databricks workspace, click **Workspace** in the left sidebar.
2. Navigate to or create a folder where you want to store the lab.
3. Click the **⋮** (kebab menu) or right-click the folder, then select **Import**.
4. Choose **URL**, enter the following URL, and click **Import**:
   `https://raw.githubusercontent.com/MicrosoftLearning/DP-750T00-Implement-Data-Engineering-Solutions-using-Azure-Databricks/refs/heads/main/Allfiles/10-design-implement-data-pipelines.ipynb`
5. Open the imported notebook and, in the compute selector at the top, choose **Serverless** compute.

### Work through the notebook exercises

The notebook contains five exercises. Run the provided setup cells and then complete each challenge cell.

> **Tip:** Challenge cells contain instructions in comments. Cells marked with ✅ are already provided — simply run them.

---

## Part 2: Orchestrate with Lakeflow Jobs (UI exercises)

These exercises use the Azure Databricks UI. You have already built the notebook logic — now you will stitch those notebooks together into a production-grade multi-task pipeline.

> **Note:** For this exercise, you will reference the notebook you imported in Part 1. In a real project you would create separate notebooks for each layer. For demonstration purposes, you'll configure the same notebook three times with different parameters.

### Exercise: Create a multi-task pipeline job

1. In the left sidebar, click **Jobs & Pipelines**.
2. Click **Create job**.
3. At the top, replace the default job name with `GlobStay Booking Pipeline`.

#### Task 1 — Ingest bronze data

4. In the task editor, enter the task name `ingest_bronze`.
5. In the **Type** dropdown, select **Notebook**.
6. In the **Source** dropdown, select **Workspace**, then choose the path to the notebook you imported.
7. In the **Compute** field, select **Serverless**.
8. Click **Save task**.

#### Task 2 — Clean silver data

9. Click **+ Add task** below Task 1 (this automatically sets a dependency on Task 1).
10. Name the task `clean_silver`, type **Notebook**, same notebook path, **Serverless** compute.
11. Under **Parameters**, add a key `layer` with value `silver`.
12. Under **Retries**, set:
    - **Retry count:** `2`
    - **Retry interval:** `60` seconds
13. Click **Save task**.

#### Task 3 — Aggregate gold data

14. Click **+ Add task** below Task 2.
15. Name the task `aggregate_gold`, type **Notebook**, same notebook path, **Serverless** compute.
16. Under **Parameters**, add a key `layer` with value `gold`.
17. Click **Save task**.

#### Verify the DAG

18. Review the DAG view. You should see three nodes arranged sequentially:
    ```
    ingest_bronze → clean_silver → aggregate_gold
    ```
19. Notice that Task 2 and Task 3 each show "Depends on" the previous task.

#### Add failure notifications

20. At the top of the job editor, click the **Notifications** tab (or scroll to the Notifications section).
21. Click **Add notification**.
22. Choose **On failure** and enter your email address.
23. Optionally enable **Mute notifications until the last retry** to avoid alert fatigue during retries.
24. Click **Save**.

#### Run the job

25. Click **Run now** (top right).
26. In the **Runs** tab, observe each task executing in sequence.
27. Click a task in the run timeline to see its output, logs, and duration.

---

### Exercise: Add a conditional task flow for data quality routing

In a real pipeline, you might want to route execution differently based on whether data quality issues were detected. This exercise adds an **If/else condition** task that branches based on a hypothetical data quality threshold.

1. Return to the **GlobStay Booking Pipeline** job editor.
2. Click **+ Add task** alongside Task 3 (deselect Task 3 first to avoid a dependency).
3. Name the task `check_quality`, type **Notebook**, same notebook path.
4. Set this task to depend on `clean_silver`.

5. Click **+ Add task**.
6. In the **Type** dropdown, select **If/else condition**.
7. Name it `quality_gate`.
8. Set this task to depend on `check_quality`.
9. In the **Condition** field, enter:
   ```
   {{tasks.check_quality.values.invalid_count}} > 5
   ```
10. For the **If true** path, add a task named `alert_data_issues` (Notebook, same path).
11. For the **If false** path, add a task named `proceed_gold` (Notebook, same path).

> **Discussion:** How does this compare to implementing error handling inside the notebook itself with `try/except`? When would you choose each approach?

---

## Summary

In this lab you:

- Built a **medallion architecture** (Bronze → Silver → Gold) for hotel booking data in Unity Catalog
- Applied **data cleaning patterns**: deduplication, null filtering, date validation, and value constraints
- Created **Gold-layer aggregations** covering property revenue and booking channel performance
- Implemented **error handling** with `try/except` and `dbutils.notebook.exit()` for job-level signaling
- Parameterized notebooks using **dbutils.widgets** and passed values between tasks using **dbutils.jobs.taskValues**
- Configured a **Lakeflow Job** with sequential task dependencies, retry policies, notifications, and an If/else condition task
