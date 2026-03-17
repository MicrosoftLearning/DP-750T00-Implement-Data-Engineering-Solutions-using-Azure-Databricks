---
lab:
  index: 05
  title: Govern Unity Catalog objects
  module: Govern Unity Catalog objects
  module-url: https://learn.microsoft.com/training/wwl-databricks/govern-unity-catalog-objects/
  notebook: https://github.com/MicrosoftLearning/DP-750T00-Implement-Data-Engineering-Solutions-using-Azure-Databricks/blob/main/Allfiles/05-govern-unity-catalog-objects.ipynb
  description: In this lab, you apply Unity Catalog governance controls to a connected vehicle data platform built on Azure Databricks. You use SQL to tag tables and columns for PII classification, configure Delta Lake retention policies and run VACUUM to purge deleted data, and enable predictive optimization. You then query system tables to trace data lineage programmatically and analyse the audit log to answer compliance questions about who accessed data and when.
  duration: 30 minutes
  level: 300
  islab: true
  primarytopics:
    - Azure Databricks
---

# Govern Unity Catalog Objects

## Scenario

You are a data engineer at **AutoSphere AG**, a global automotive manufacturer building a connected vehicle data platform on Azure Databricks. The platform ingests telemetry from millions of vehicles, manages customer and vehicle registration data, and tracks service records.

The data governance team has raised the following concerns:

- Vehicle telemetry data has aggressive write patterns and must have well-defined **retention policies** to control storage growth and comply with GDPR data minimisation requirements.
- The data team needs **full lineage visibility** to understand how tables are derived and trace the impact of upstream changes.
- The compliance team needs to **audit who accessed what data and when**, using queryable logs rather than manual log searches.

In this lab, you address all these concerns by applying tagging, retention policies, lineage queries, and audit log analysis in Azure Databricks Unity Catalog.

---

## Objectives

By the end of this lab, you will have:

- Applied descriptive comments and tags to tables and columns for data discovery.
- Configured Delta Lake retention settings and run a VACUUM operation.
- Viewed data lineage visually in Catalog Explorer.
- Queried lineage system tables programmatically.
- Queried the audit log system table to investigate data access patterns.

This lab should take approximately **30 minutes** to complete.

---

## 🤖 Use the Databricks Assistant throughout this lab

You are **expected and encouraged** to use the **Databricks Assistant at all times** during this lab. Every exercise cell in the notebook includes a suggested prompt you can paste directly into the Assistant panel.

To open the Databricks Assistant, select the ![assistant-icon](https://raw.githubusercontent.com/MicrosoftLearning/DP-750T00-Implement-Data-Engineering-Solutions-using-Azure-Databricks/refs/heads/main/Allfiles/media/databricks-assistant.svg) on the right side of any notebook cell, or press the keyboard shortcut shown in the toolbar.

> 💡 **Tip:** Do not just copy and paste the Assistant's output blindly. Read it, understand it, and adapt it to the task at hand. The Assistant is a tool to accelerate your thinking, not replace it.

---

## Prerequisites

Before starting this lab, ensure you have:

- Access to an **Azure Databricks Premium workspace** (already provisioned for you).
- An active **Unity Catalog metastore** attached to the workspace.
- The **CREATE CATALOG** privilege on the metastore.
- Familiarity with basic SQL (`CREATE TABLE`, `SELECT`, `ALTER TABLE`).

---

## Import the lab notebook

1. In your Azure Databricks workspace, select **Workspace** in the left sidebar.
2. Navigate to or create a folder where you want to store this lab.
3. Select the **⋮** (kebab) menu next to the folder, then select **Import**.
4. Choose **URL**, enter the following URL, and select **Import**:
   `https://raw.githubusercontent.com/MicrosoftLearning/DP-750T00-Implement-Data-Engineering-Solutions-using-Azure-Databricks/refs/heads/main/Allfiles/05-govern-unity-catalog-objects.ipynb`
5. Open the imported notebook and, in the compute selector at the top, choose **Serverless** compute.

---

## Before you open the notebook: View data lineage in Catalog Explorer

After completing Exercise 1 in the notebook (which creates your tables), come back here and follow these steps to explore lineage visually in the **Catalog Explorer**. This is a UI task and does not require any notebook code.

> ⚠️ Complete Exercise 1 in the notebook first, then return here.

### View table lineage

1. In your Azure Databricks workspace, select **Catalog** in the left sidebar to open Catalog Explorer.
2. Navigate to `automotive_catalog` > `governance_lab`.
3. Select the `vehicle_telemetry` table.
4. Select the **Lineage** tab.
5. Select **See Lineage Graph** to open the interactive lineage visualization.

Observe the upstream and downstream relationships. Notice that the graph shows:
- Which notebooks or jobs wrote to the table.
- Which downstream tables or views depend on it.

### View column-level lineage

1. Still on the **Lineage** tab, select the `service_records` table node.
2. Select a column (for example, `vehicle_id`) to explore which upstream columns it traces back to.

### View table history

1. Navigate to the `vehicle_telemetry` table in Catalog Explorer.
2. Select the **History** tab.
3. Observe the version history — each row represents one operation (write, update, VACUUM, etc.).

This history can serve as an audit trail for understanding who modified the table and when.

---

> When you have finished exploring lineage in Catalog Explorer, proceed with the remaining exercises in the notebook.
