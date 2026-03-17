---
lab:
  index: 11
  title: Implement Lakeflow Jobs with Azure Databricks
  module: Implement Lakeflow Jobs
  module-url: https://learn.microsoft.com/training/wwl-databricks/implement-lakeflow-jobs/
  notebook: https://github.com/MicrosoftLearning/DP-750T00-Implement-Data-Engineering-Solutions-using-Azure-Databricks/blob/main/Allfiles/11-implement-lakeflow-jobs.ipynb
  description: In this lab, you configure and automate a CDR data pipeline for TelConnect using Lakeflow Jobs. You run a pre-built parameterized notebook that processes Call Detail Records through bronze, silver, and gold layers, then configure a Lakeflow Job in the Azure Databricks UI with task dependencies, a job parameter, scheduled and event-based triggers, failure notifications, and retry policies.
  duration: 45 minutes
  level: 300
  islab: true
  primarytopics:
    - Azure Databricks
---

# Lab 11: Implement Lakeflow Jobs

## Introduction

You are a data engineer at **TelConnect**, a regional telecommunications provider that manages millions of calls, SMS messages, and data sessions every day. Each event produces a **Call Detail Record (CDR)** — the raw signal of network activity. This data must flow reliably through a multi-layer pipeline before operations teams can use it for network performance reporting and capacity planning.

Today's task is to automate that pipeline using **Lakeflow Jobs**. You will:

- Run a pre-built parameterized **medallion architecture** notebook (Bronze → Silver → Gold) that processes CDR data
- Create a **Lakeflow Job** in the Azure Databricks UI that runs the notebook as an orchestrated task
- Pass a `processing_date` **job parameter** to the notebook using a dynamic trigger value
- Add a second downstream task with a **task dependency**
- Configure an **event-based file arrival trigger** that fires when new CDR files land in the volume
- Add a **nightly cron schedule** so the job also runs automatically each night
- Set up **failure notifications** so the network operations team is alerted immediately
- Configure **automatic retry policies** to handle transient cloud-storage errors

---

## 🤖 Use the Databricks Assistant throughout this lab

You are expected and encouraged to use the **Databricks Assistant** for every coding exercise. Use it for suggestions, error explanations, boilerplate generation, and API exploration.

To open the Databricks Assistant, select the ![assistant-icon](https://raw.githubusercontent.com/MicrosoftLearning/DP-750T00-Implement-Data-Engineering-Solutions-using-Azure-Databricks/refs/heads/main/Allfiles/media/databricks-assistant.svg) on the right side of any notebook cell, or use the keyboard shortcut.

---

## Prerequisites

- An **Azure Databricks Premium** workspace is already provisioned and you have access to it.
- You have permission to create catalogs and schemas in Unity Catalog.
- Basic familiarity with PySpark and Spark SQL.

---

## Part 1: Run the pipeline notebook

The notebook contains five exercises that build the Bronze → Silver → Gold pipeline. All cells are provided — run them in order to set up the data environment you will orchestrate in Part 2.

### Import the notebook

1. In your Databricks workspace, click **Workspace** in the left sidebar.
2. Navigate to or create a folder where you want to store the lab.
3. Click the **⋮** (kebab menu) or right-click the folder, then select **Import**.
4. Choose **URL**, enter the following URL, and click **Import**:
   `https://raw.githubusercontent.com/MicrosoftLearning/DP-750T00-Implement-Data-Engineering-Solutions-using-Azure-Databricks/refs/heads/main/Allfiles/11-implement-lakeflow-jobs.ipynb`
5. Open the imported notebook and, in the compute selector at the top, choose **Serverless** compute.

### Run the notebook

1. Run **Exercise 1** cells — this creates the `telconnect_lab` Unity Catalog with bronze, silver, and gold schemas.
2. In **Exercise 2**, run the first cell to create the `raw_uploads` volume and automatically download `telecom_cdrs.csv` from GitHub into the volume.
3. Continue running all remaining cells through **Exercise 5**.

> **Tip:** Each cell is marked ✅. Read the markdown above each code cell to understand what the step does before running it — this context will help you when configuring the Lakeflow Job in Part 2.

---

## Part 2: Orchestrate with Lakeflow Jobs (UI exercises)

These exercises are the core of this lab. Using the Azure Databricks **Jobs & Pipelines** UI, you wire the notebook you just ran into a production-grade automated job with triggers, schedules, notifications, and retry policies.

> 🤖 **Databricks Assistant reminder:** Open the Databricks Assistant in the notebook or workspace sidebar. For UI-only steps, ask it questions like *"How do I add a file arrival trigger to a Lakeflow Job?"* to get contextual guidance fast.

---

### Exercise 1: Create a Lakeflow Job with the CDR pipeline notebook

#### Create the job

1. In the left sidebar, click **Jobs & Pipelines**.
2. Click **Create job**.
3. Replace the default job name with `TelConnect CDR Pipeline`.

#### Configure the first task

4. In the task editor, enter the task name `process_cdrs`.
5. In the **Type** dropdown, select **Notebook**.
6. In the **Source** dropdown, select **Workspace**, then browse to the path of the notebook you imported (`11-implement-lakeflow-jobs`).
7. In the **Compute** selector, choose **Serverless**.
8. Click **Save task**.

#### Add a job parameter

9. In the **Job details** panel on the right, scroll to the **Parameters** section and click **Add**.
10. Set the key to `processing_date` and the value to `{{job.trigger.time.iso_date}}`.

   > This dynamic value inserts the date on which the job was triggered, matching the widget default in the notebook.

11. Click **Save** on the parameter.

#### Verify

12. In the task graph, confirm that **process_cdrs** appears as the single node.
13. Click **Run now** to test the job manually.
14. In the **Runs** tab, wait for the run to complete and confirm all notebook cells executed successfully.

---

### Exercise 2: Add a downstream notification task

In a real pipeline, you might want a second task to send a summary report after the CDR processing task completes. Here you simulate that with a short SQL task that reads from the Gold table.

1. In the **TelConnect CDR Pipeline** job editor, click **+ Add task** below `process_cdrs`.
2. Name the task `summarize_gold`.
3. In the **Type** dropdown, select **SQL**.
4. In the **SQL** field, enter:

   ```sql
   SELECT region, network_type, total_calls, drop_rate_pct
   FROM telconnect_lab.gold.network_summary
   ORDER BY drop_rate_pct DESC;
   ```

5. In the **SQL warehouse** selector, choose an available Serverless SQL warehouse.
6. Confirm that **Depends on** is already set to `process_cdrs` and the **Run if** condition is **All succeeded**.
7. Click **Save task**.

#### Verify the DAG

8. Review the DAG view. You should see:
   ```
   process_cdrs → summarize_gold
   ```

---

### Exercise 3: Configure a file arrival trigger

TelConnect drops fresh CDR files into the volume at irregular intervals throughout the day. Instead of polling on a fixed schedule, you configure the job to fire automatically each time a new file arrives.

1. In the **Job details** panel, under **Schedules & Triggers**, click **Add trigger**.
2. In the **Trigger type** dropdown, select **File arrival**.
3. In the **Storage location** field, enter:

   ```
   /Volumes/telconnect_lab/bronze/raw_uploads/
   ```

4. Under **Advanced options**, set **Wait after last change** to `60` seconds.

   > This batches rapid file arrivals into a single job run — useful when multiple CDR batches arrive in quick succession.

5. Click **Save**.

#### Pause the trigger

Since you will also add a schedule in the next exercise, pause the file arrival trigger to avoid duplicate runs during the lab:

6. Under **Schedules & Triggers**, locate the file arrival trigger and click **Pause**.

> **Note:** In production, you would keep both active — the file arrival trigger handles intraday updates while the schedule provides a reliable nightly catch-all run.

---

### Exercise 4: Add a nightly schedule

TelConnect's operations team expects the Gold summary table to be refreshed every night at 02:00 CET so dashboards are current before the morning shift begins.

1. In the **Job details** panel, click **Add trigger** again.
2. Set **Trigger type** to **Scheduled**.
3. Set **Schedule type** to **Advanced**.
4. In the **Cron expression** field, enter:

   ```
   0 0 2 * * ?
   ```

   > This expression means: at second 0, minute 0, hour 2, every day of the month, every month, any day of the week.

5. Set the **Time zone** to **Europe/Amsterdam**.
6. Click **Save**.

#### Verify the cron expression

7. After saving, review the human-readable schedule preview. It should read: *"Every day at 2:00 AM Amsterdam time."*

---

### Exercise 5: Configure failure notifications

The network operations team must be alerted immediately if the CDR pipeline fails — a missed run could delay fault detection and network SLA reporting.

#### Add job-level failure notification

1. In the **Job details** panel, scroll to **Job notifications** and click **Edit notifications**.
2. Click **Add notification**.
3. Choose **Email address** and enter your email.
4. Select the **Failure** event type.
5. Optionally also select **Duration warning** and set a threshold of `30` minutes.
6. Click **Save**.

#### Add task-level retry notification

7. Select the `process_cdrs` task in the task graph to open its configuration.
8. In the task panel, scroll to **Notifications** and click **Add**.
9. Choose **Email address** (same address), select **Failure**, and check **Mute notifications until the last retry**.

   > This prevents notification spam when the task retries. You only receive an alert if all retry attempts fail.

10. Click **Save task**.

---

### Exercise 6: Configure automatic retries

CDR files are read from cloud storage, and transient network errors or storage throttling are common causes of short-lived task failures. Configuring retries means the pipeline recovers without manual intervention.

#### Configure task-level retries on `process_cdrs`

1. Select the `process_cdrs` task in the task graph.
2. In the task configuration panel, click **+ Add** next to **Retries**.
3. Set **Retry count** to `2`.
4. Set **Retry interval** to `60` seconds.
5. Click **Save task**.

#### Configure a timeout threshold

6. In the same task configuration, click **Metric thresholds**.
7. Set the **Metric** to **Run duration**.
8. Set **Warning** to `20` minutes.
9. Set **Timeout** to `30` minutes.
10. Click **Save task**.

> If the notebook hangs — for example, a query stalls on a large CDR backlog — the timeout ensures the task fails cleanly so the retry mechanism can kick in rather than the task running indefinitely.

---

### Exercise 7: Run and monitor the complete job

1. In the **TelConnect CDR Pipeline** job editor, click **Run now**.
2. Navigate to the **Runs** tab and click the active run to open the run details.
3. Observe both tasks (`process_cdrs` and `summarize_gold`) executing in sequence.
4. Click `process_cdrs` in the run timeline to view:
   - The notebook output (each cell result)
   - The job parameter value passed in (`processing_date`)
   - The task values set by `dbutils.jobs.taskValues.set()`
5. After both tasks complete, verify that the run status is **Succeeded**.

---

## Summary

In this lab you:

- Ran a **medallion architecture** pipeline (Bronze → Silver → Gold) for TelConnect CDR data in Unity Catalog
- Created a **Lakeflow Job** with two tasks and a sequential task dependency
- Passed `processing_date` as a **dynamic job parameter** tied to the trigger time
- Configured a **file arrival trigger** that fires when new CDR files land in the volume
- Added a **nightly cron schedule** at 02:00 CET to ensure daily refresh
- Set up **failure notifications** with muted retry alerts to reduce alert noise
- Configured **task-level retries** (2 attempts, 60-second interval) and a 30-minute timeout threshold
