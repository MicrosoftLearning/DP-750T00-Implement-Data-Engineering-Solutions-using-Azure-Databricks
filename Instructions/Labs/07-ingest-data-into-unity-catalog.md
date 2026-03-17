---
lab:
  index: 07
  title: Ingest data into Unity Catalog
  module: Ingest data into Unity Catalog
  module-url: https://learn.microsoft.com/training/wwl-databricks/ingest-data-into-unity-catalog/
  description: In this lab, you practise the core data ingestion techniques available in Azure Databricks. You load CSV files from a Unity Catalog managed volume into Delta tables using PySpark DataFrames, SQL COPY INTO, and CREATE TABLE AS SELECT. You also configure Auto Loader to automatically detect and process new files from cloud storage, demonstrating exactly-once ingestion for continuously arriving data.
  duration: 45 minutes
  level: 300
  islab: true
  primarytopics:
    - Azure Databricks
---

# Lab: Ingest Data into Unity Catalog — Solaris Energy

## Scenario

You are a data engineer at **Solaris Energy**, a renewable energy company operating solar farms in Spain and Germany, and wind turbine installations in the Netherlands and Denmark. Your team is building a centralised data platform on Azure Databricks to consolidate energy production readings, turbine maintenance events, and grid telemetry from all sites.

In this lab, you will practise the core data ingestion techniques available in Azure Databricks: batch ingestion with PySpark DataFrames, SQL-based file loading with `COPY INTO`, aggregated table creation with `CTAS`, and continuous file detection with Auto Loader.

By the end of this lab, you will be able to:

- Create a Unity Catalog hierarchy (catalog, schema, volume) to house ingested data
- Load CSV data from a managed volume into a Delta table using PySpark DataFrames
- Use `COPY INTO` to incrementally load files with built-in deduplication
- Create summary tables using `CREATE TABLE AS SELECT`
- Configure Auto Loader to automatically detect and process new files from cloud storage

---

## 🤖 Databricks Assistant — use it throughout this lab!

You are expected and encouraged to use the **Databricks Assistant** for all exercises. The Assistant is available directly in the notebook cell toolbar. Use it to:

- Look up syntax for SQL and PySpark operations
- Explain error messages
- Generate boilerplate code that you then adapt
- Ask follow-up questions about how things work

To open the Databricks Assistant, select the ![assistant-icon](https://raw.githubusercontent.com/MicrosoftLearning/DP-750T00-Implement-Data-Engineering-Solutions-using-Azure-Databricks/refs/heads/main/Allfiles/media/databricks-assistant.svg) on the right side of any notebook cell, or use the keyboard shortcut.

💡 **Each exercise cell includes a suggested prompt you can copy directly into the Databricks Assistant to get started.**

---

## Prerequisites

- An Azure Databricks workspace (Premium tier) has been provisioned and you have access to it
- You have the **Data Engineer** or equivalent role in the workspace, with permission to create catalogs and volumes
- Basic familiarity with SQL and Python

---

## Non-notebook exploration: Lakeflow Connect (optional)

**Lakeflow Connect** provides a graphical, low-code approach for ingesting data from external sources such as SQL Server, Salesforce, and SharePoint directly into Unity Catalog tables — without writing custom code.

To explore Lakeflow Connect in your workspace:

1. In the Databricks workspace sidebar, click **Data Engineering** → **Data Ingestion**.
2. Browse the available connectors. Note the categories: Database connectors, SaaS connectors, and File-based ingestion.
3. Click on **SQL Server** to see how you would configure a connection, choose tables to ingest, and set a destination catalog and schema.
4. Observe the options for **SCD Type 1** (overwrite) vs **SCD Type 2** (history tracking) and **full refresh** vs **incremental** extraction.

> 💡 You do not need to complete the Lakeflow Connect setup as part of this lab — a configured SQL Server source is not provided. The exploration above is for familiarisation only.

---

## Non-notebook exploration: Lakeflow Spark Declarative Pipelines (optional)

**Lakeflow Spark Declarative Pipelines** (formerly Delta Live Tables) is the recommended way to build production-grade ingestion pipelines with automatic orchestration, schema management, and exactly-once guarantees.

To explore the pipeline editor:

1. In the sidebar, click **Data Engineering** → **Pipelines**.
2. Click **Create Pipeline** and select **ETL pipeline**.
3. Review the options for specifying a source notebook or SQL file, naming the pipeline catalog and schema, and choosing a cluster type.
4. Click **Cancel** — you do not need to create or run a pipeline.

> 💡 The **Auto CDC API** (`create_auto_cdc_flow` / `AUTO CDC INTO`) is the recommended way to process change data capture (CDC) feeds inside a Lakeflow Declarative Pipeline. It handles deduplication, out-of-order events, and SCD Type 1 or Type 2 patterns automatically. You would define it in a pipeline notebook or SQL file, then run it from the Pipelines UI.

---

## Importing the notebook

Follow these steps to import the lab notebook into your Databricks workspace:

1. In the Databricks workspace, click **Workspace** in the left sidebar.
2. Navigate to or create a folder where you want to store the lab (for example, `/Users/<your-email>/Labs`).
3. Click the **⋮** (kebab) menu or right-click the folder, then select **Import**.
4. Choose **URL**, enter the following URL, and click **Import**:
   `https://raw.githubusercontent.com/MicrosoftLearning/DP-750T00-Implement-Data-Engineering-Solutions-using-Azure-Databricks/refs/heads/main/Allfiles/07-ingest-data-into-unity-catalog.ipynb`
5. Open the imported notebook and, in the compute selector at the top, choose **Serverless** compute.

---

## Lab overview

The notebook is structured into four exercises:

| Exercise | Topic | Technique |
|---|---|---|
| 1 | Set up the catalog hierarchy | SQL DDL — `CREATE CATALOG`, `CREATE SCHEMA`, `CREATE VOLUME` |
| 2 | Batch ingestion with DataFrames | PySpark `spark.read` / `df.write` |
| 3 | SQL-based file ingestion | `COPY INTO`, `CREATE TABLE AS SELECT` |
| 4 | Auto Loader | `cloudFiles` format with `spark.readStream` |

Work through the exercises in order. Each exercise builds on the catalog and data created in the previous one.
