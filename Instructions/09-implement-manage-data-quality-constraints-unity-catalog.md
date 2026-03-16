# Lab 09: Enforcing Data Quality in an Insurance Claims Pipeline

## Introduction

In this lab, you take on the role of a data engineer at **InsureGuard**, a European insurance company. Claims data arrives daily from multiple source systems — web portals, legacy mainframes, and partner APIs. Critically, the data is dirty: missing claim identifiers, negative claim amounts, invalid category codes, malformed dates, and even unexpected new columns added by upstream teams without notice.

Bad data erodes actuarial accuracy, triggers regulatory compliance failures, and causes report breakdowns for the underwriting and fraud teams. Your mission: build a **Lakeflow Spark Declarative Pipeline** that enforces data quality rules at every layer of the claims data platform.

The notebook for this lab is written as **pipeline source code**. Each exercise adds new table definitions using the `databricks.sdk.pipelines` API. After completing the setup exercise, you will create a pipeline in the Databricks UI, connect this notebook to it, and validate and run the pipeline incrementally as you add rules.

You work through the following exercises:

| Exercise | Topic |
|---|---|
| Exercise 1 | Set up the InsureGuard data platform |
| Exercise 2 | Ingest claims with nullability monitoring (bronze) |
| Exercise 3 | Enforce type and range validation (silver validated) |
| Exercise 4 | Quarantine invalid records |
| Exercise 5 | Protect against schema drift with rescued data |

---

## 🤖 Databricks Assistant — Use it always

Throughout every exercise in this lab, you are **expected and encouraged to use the Databricks Assistant**. Every notebook cell includes a suggested prompt to help you get started. The Assistant is your pair programmer — use it to generate code, explain error messages, explore alternatives, and validate your approach.

**How to open the Databricks Assistant:** Click the **✦ Assistant** icon in the right toolbar of your notebook, or press `Alt+Shift+Space`.

---

## Prerequisites

- An **Azure Databricks Premium workspace** is already provisioned and available.
- You are familiar with basic Python and SQL concepts.
- No prior experience with Lakeflow Spark Declarative Pipelines is required.

---

## Importing the Notebook

1. Open a terminal and clone the repository:
   ```bash
   git clone https://github.com/MicrosoftLearning/DP-750T00-Implement-Data-Engineering-Solutions-using-Azure-Databricks.git DP-750
   ```

2. In the Databricks workspace, click **Workspace** in the left sidebar.

3. Navigate to or create a folder where you want to store the lab.

4. Click the **⋮** (kebab) menu or right-click the folder, then select **Import**.

5. Choose **File**, browse to `DP-750/Allfiles/09-implement-manage-data-quality-constraints-unity-catalog.ipynb`, and click **Import**.

6. Open the imported notebook and, in the compute selector at the top, choose **Serverless** compute.

---

## Non-notebook task: Create the Lakeflow Spark Declarative Pipeline

After completing **Exercise 1** (the platform setup), you need to create a pipeline in the Databricks UI and connect it to your notebook. This is required before you can run Exercises 2–5 as pipeline definitions.

1. In the Databricks workspace left sidebar, click **Jobs & Pipelines**.

2. Click **Create pipeline**.

3. Give the pipeline a meaningful name, for example: `InsureGuard Claims Quality Pipeline`.

4. Under **Pipeline mode**, select **Triggered**.

5. Under **Source code**, click **Add source code** and browse to your imported notebook (`09-implement-manage-data-quality-constraints-unity-catalog`). Click **Add**.

6. Under **Destination**, set the **Catalog** to `insureguard_lab` and the **Target schema** to `bronze`.
   > This tells the pipeline which Unity Catalog location to use. Individual tables will be created in the schemas specified inside the notebook code.

7. Under **Compute**, select **Serverless**.

8. Click **Create**.

### Connect the notebook to the pipeline

After creating the pipeline, return to your notebook:

1. At the top of the notebook, you should now see a pipeline indicator bar. If not, click the **Connect** dropdown (where **Serverless** appears) — you may see the pipeline listed there to connect.

2. Once connected, you can start, validate, or view the pipeline's dataflow graph **directly from within the notebook**.

> With the notebook connected to the pipeline, you can click **Validate** (or **Start**) from within the notebook to trigger a pipeline run without leaving your workspace. Diagnostics and data quality metrics appear inline.

---

## Non-notebook task: Monitor data quality results in the Pipeline UI

After running your pipeline (after Exercise 2 or later), you can inspect data quality metrics as follows:

1. In the Databricks workspace left sidebar, click **Jobs & Pipelines**, then select your pipeline.

2. After a successful run, the pipeline **dataflow graph** is visible. Each box represents a table defined in the notebook.

3. Click a table node (e.g., `bronze_claims_raw`) to open its details panel on the right.

4. Select the **Data quality** tab to view expectation metrics:
   - **Passed**: how many records satisfied the constraint
   - **Failed**: how many records violated the constraint
   - **Action**: whether violations were warned, dropped, or caused a failure

5. Use these metrics to understand the quality profile of the incoming claims data and confirm your expectations are working as intended.

---

## Non-notebook task: Add Delta CHECK constraints to the silver validated table

After Exercise 3 creates the `silver_claims_validated` table, you can add **Delta Lake CHECK constraints** directly from the Catalog Explorer. These constraints provide a second enforcement layer — they reject any direct writes to the table that violate the rules, even outside the pipeline.

1. Open **Catalog Explorer** from the left navigation pane.

2. Navigate to **insureguard_lab** → **silver** → **silver_claims_validated**.

3. Select the **Overview** tab and scroll to **Table constraints**.

4. Click **Add constraint** and add the following two CHECK constraints one at a time:

   - Name: `positive_claim_amount` | Expression: `claim_amount > 0`
   - Name: `valid_claim_type` | Expression: `claim_type IN ('AUTO', 'HOME', 'LIFE', 'HEALTH')`

5. After saving, try inserting a row that violates one of these constraints using a SQL notebook cell (as a test):
   ```sql
   INSERT INTO insureguard_lab.silver.silver_claims_validated
   VALUES ('TEST-001', 'POL-TEST', 'CUST-TEST', '2026-03-01', '2026-02-28', -100, 'HOME', 'OPEN', 50000, 'AGT-01');
   ```

6. Confirm the insert is rejected and note the error message.

> CHECK constraints on Delta tables complement pipeline expectations: expectations catch issues during pipeline runs, while CHECK constraints protect against direct writes from other sources.
