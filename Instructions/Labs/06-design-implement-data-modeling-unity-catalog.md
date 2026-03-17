---
lab:
  index: 06
  title: Design and implement data modeling with Azure Databricks
  module: Design and implement data modeling with Azure Databricks
  module-url: https://learn.microsoft.com/training/wwl-databricks/design-implement-data-modeling-unity-catalog/
  description: In this lab, you design and implement a Delta Lake data model in Unity Catalog for a retail banking scenario, building a customer dimension with SCD Type 2 history tracking and a transaction fact table with liquid clustering. You apply Change Data Feed to build a queryable FCA compliance audit trail and use Delta Lake time travel to inspect and restore previous table versions.
  duration: 45 minutes
  level: 300
  islab: true
  primarytopics:
    - Azure Databricks
---

# Design and Implement a Data Model in Unity Catalog

## Scenario

You are a data engineer at **Northbank Financial**, a retail bank operating across the United Kingdom. The data engineering team is building a modern lakehouse platform on Azure Databricks to power customer analytics, regulatory reporting, and fraud detection.

The team needs to address the following requirements:

- **Customer dimension management:** Customer attributes such as city, account segment, and account type change over time. Regulatory reporting must reflect the **customer's profile at the time of a transaction**, not just the current state.
- **Transaction fact storage:** The platform must efficiently store and query millions of daily payment transactions. Query patterns target specific date ranges, so the physical data organisation must support fast filtering.
- **Audit trail for compliance:** Basel III and FCA regulations require that any modification to transaction records is fully traceable. The team needs a queryable log of all changes to transaction data.
- **Historical data recovery:** Accidental data changes must be recoverable without restoring from backup. The solution must support point-in-time recovery directly within the lakehouse.

In this lab, you apply data modelling concepts to design and implement these requirements using **Delta Lake**, **Unity Catalog**, **SCD Type 2**, **Change Data Feed**, and **Delta Lake time travel**.

---

## Objectives

By the end of this lab, you will have:

- Created a Unity Catalog data model with managed Delta Lake tables.
- Applied **liquid clustering** to optimise query performance on transaction tables.
- Implemented **SCD Type 2** on a customer dimension using `MERGE`.
- Queried historical customer records using point-in-time filters.
- Enabled **Change Data Feed** and queried the audit trail using `table_changes()`.
- Used **Delta Lake time travel** to inspect and restore previous table versions.

This lab should take approximately **45 minutes** to complete.

---

## 🤖 Use the Databricks Assistant throughout this lab

You are **expected and encouraged** to use the **Databricks Assistant at all times** during this lab. Every exercise cell in the notebook includes a suggested prompt you can paste directly into the Assistant panel.

To open the Databricks Assistant, click the ![assistant-icon](https://raw.githubusercontent.com/MicrosoftLearning/DP-750T00-Implement-Data-Engineering-Solutions-using-Azure-Databricks/refs/heads/main/Allfiles/media/databricks-assistant.svg) on the right side of any notebook cell, or press the keyboard shortcut shown in the toolbar.

> 💡 **Tip:** Don't just copy and paste the Assistant's output blindly. Read it, understand it, and adapt it to the task at hand. The Assistant is a tool to accelerate your thinking, not replace it.

---

## Prerequisites

Before starting this lab, ensure you have:

- Access to an **Azure Databricks Premium workspace** (already provisioned for you).
- An active **Unity Catalog metastore** attached to the workspace.
- The **CREATE CATALOG** privilege on the metastore.
- Familiarity with basic SQL (`CREATE TABLE`, `SELECT`, `MERGE`) and Python/PySpark.

---

## Import the lab notebook

1. In your Azure Databricks workspace, click **Workspace** in the left sidebar.
2. Navigate to or create a folder where you want to store this lab.
3. Click the **⋮** (kebab) menu next to the folder, then select **Import**.
4. Choose **URL**, enter the following URL, and click **Import**:
   `https://raw.githubusercontent.com/MicrosoftLearning/DP-750T00-Implement-Data-Engineering-Solutions-using-Azure-Databricks/refs/heads/main/Allfiles/06-design-implement-data-modeling-unity-catalog.ipynb`
5. Open the imported notebook and, in the compute selector at the top, choose **Serverless** compute.

---

## Before you open the notebook: Explore managed vs. external tables in Catalog Explorer

This task does not require notebook code and must be completed using the **Databricks UI**.

### Compare managed and external table storage

When you have completed **Exercise 1** in the notebook (which creates your catalog and tables), come back here and perform the following steps.

> ⚠️ Complete Exercise 1 in the notebook first, then return here.

1. In your Databricks workspace, click **Catalog** in the left sidebar to open **Catalog Explorer**.
2. Expand the **banking_lab** catalog and then the **silver** schema.
3. Click on the **dim_customer** table to open its details panel.
4. Under the **Overview** tab, locate the **Storage location** field.
   - Notice that the storage path is managed by Unity Catalog — you did not specify a `LOCATION` when creating the table.
   - This is a **managed table**: Unity Catalog controls both the metadata and the underlying data files.
5. Repeat the same inspection for the **fact_transactions** table.
6. Note the **Clustering** information listed for **fact_transactions** — this confirms that liquid clustering is active.

### Key difference to remember

| | Managed table | External table |
|---|---|---|
| **Metadata** | Unity Catalog | Unity Catalog |
| **Data files** | Unity Catalog managed location | User-specified location |
| **DROP TABLE behaviour** | Files deleted after 8 days | Files remain |
| **Automatic optimisation** | Predictive optimisation available | Not available |

For Northbank's analytics platform, managed tables are the right choice — they benefit from automatic maintenance and simpler governance.

---

Now return to the notebook and continue with **Exercise 2**.
