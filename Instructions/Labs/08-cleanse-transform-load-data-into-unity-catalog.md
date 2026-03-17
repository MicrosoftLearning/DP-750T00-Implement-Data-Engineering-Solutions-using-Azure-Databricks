---
lab:
  index: 08
  title: Cleanse, transform, and load data into Unity Catalog
  module: Cleanse, transform, and load data into Unity Catalog
  description: In this lab, you clean and reshape raw real estate data in Azure Databricks. You choose the right data types for prices and timestamps, remove duplicate listings and fill missing values using PySpark, and combine data across tables with inner and left joins. You also use SQL PIVOT and UNPIVOT to restructure market statistics for trend analysis.
  duration: 45 minutes
  level: 300
  islab: true
  primarytopics:
    - Azure Databricks
---

# Lab 08: Cleansing and Transforming Real Estate Data in Unity Catalog

## Introduction

In this lab, you take on the role of a data engineer at **Pristine Properties**, a real estate brokerage operating across major Dutch cities. The company has ingested raw property listings from multiple agent offices and satellite databases, but the data is messy: prices lack precision, listings are duplicated with updated versions, key fields contain nulls, and market statistics are delivered in a wide-column format that makes trend analysis difficult.

Your job is to clean, type-check, and reshape the data so it can reliably feed pricing dashboards and market analysis models.

You work through the following exercises:

| Exercise | Topic |
|---|---|
| Exercise 1 | Set up the Pristine Properties Platform |
| Exercise 2 | Profile the listings data |
| Exercise 3 | Choose the right data types |
| Exercise 4 | Handle duplicates and missing values |
| Exercise 5 | Join listings with agents and sales data |
| Exercise 6 | Pivot and unpivot market statistics |

---

## 🤖 Databricks Assistant — Use it always

Throughout every exercise in this lab, you are **expected and encouraged to use the Databricks Assistant**. Every notebook cell includes a suggested prompt to get you started. The Assistant is your pair programmer — use it to generate code, explain error messages, explore alternatives, and validate your approach.

To open the Databricks Assistant, select the ![assistant-icon](https://raw.githubusercontent.com/MicrosoftLearning/DP-750T00-Implement-Data-Engineering-Solutions-using-Azure-Databricks/refs/heads/main/Allfiles/media/databricks-assistant.svg) on the right side of any notebook cell, or use the keyboard shortcut.

---

## Prerequisites

- An **Azure Databricks Premium workspace** is already provisioned and available.
- You are familiar with basic SQL and Python/PySpark concepts.

---

## Importing the Notebook

1. In the Databricks workspace, click **Workspace** in the left sidebar.

2. Navigate to or create a folder where you want to store the lab.

3. Click the **⋮** (kebab) menu or right-click the folder, then select **Import**.

4. Choose **URL**, enter the following URL, and click **Import**:
   `https://raw.githubusercontent.com/MicrosoftLearning/DP-750T00-Implement-Data-Engineering-Solutions-using-Azure-Databricks/refs/heads/main/Allfiles/08-cleanse-transform-load-data-into-unity-catalog.ipynb`

5. Open the imported notebook and, in the compute selector at the top, choose **Serverless** compute.

---

## Non-notebook task: Create a data profile in Catalog Explorer

After completing Exercise 1 (the environment setup), you can create a data profile for the `realestate_lab.bronze.listings` table using the Unity Catalog UI. This is a UI-based task and does not require any code.

1. Open **Catalog Explorer** from the left navigation pane.
2. Navigate to the **realestate_lab** catalog → **bronze** schema → **listings** table.
3. Select the **Quality** tab.
4. Click **Configure** to enable data profiling.
5. Choose **Snapshot** as the profile type — this is suitable for a general-purpose table like listings.
6. Click **Save and run** to generate the first profile.
7. After the profile completes, explore the generated metrics. Look at:
   - **Null counts** — which columns have the most missing values?
   - **Distinct counts** — are there any columns where unexpected values appear?
   - **Value distributions** — what is the spread of `list_price` values?

This gives you a visual and statistical overview of the data before you begin cleaning it programmatically.
