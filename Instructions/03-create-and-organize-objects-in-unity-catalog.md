---
lab:
    title: 'Create and Organize Objects in Unity Catalog'
---

# Create and Organize Objects in Unity Catalog

## Scenario

You are a data engineer at **Lakeside University**, a mid-sized institution digitizing its academic operations. Your team has been tasked with building a modern data platform in Azure Databricks to manage student records, course catalogues, and enrollment data.

In this lab, you design and implement the full Unity Catalog namespace for the Lakeside University development environment. You create catalogs, schemas, tables with constraints, views, volumes, and reusable SQL functions — all following organizational naming conventions.

## Objectives

By the end of this lab, you will have:

- Created a catalog and medallion schemas following Unity Catalog naming conventions.
- Created managed tables with primary key and foreign key constraints.
- Built a standard view and a materialized view to serve analytical queries.
- Created a managed volume and loaded a CSV file into it.
- Written a reusable SQL scalar function for grade classification.
- Used `ALTER` statements to extend tables and apply governance tags.

This lab should take approximately **45 minutes** to complete.

---

## 🤖 Use the Databricks Assistant throughout this lab

You are expected and encouraged to use the **Databricks Assistant** at all times during this lab. Every exercise includes suggested prompts you can paste directly into the Assistant panel.

To open the Databricks Assistant, click the **✨ sparkle icon** on the right side of any notebook cell, or use the keyboard shortcut.

> 💡 **Tip:** Do not just copy and paste the Assistant's output blindly. Read it, understand it, and adapt it to the specific requirements of each task. The Assistant is a tool to accelerate your thinking, not replace it.

---

## Prerequisites

Before starting this lab, ensure you have:

- Access to an **Azure Databricks Premium workspace** (already provisioned for you).
- An active **Unity Catalog metastore** attached to the workspace.
- The **CREATE CATALOG** privilege on the metastore (granted by your instructor or workspace admin).
- Familiarity with basic SQL (`CREATE TABLE`, `SELECT`, `JOIN`).

---

## Clone the lab repository

Before starting the exercises, clone the lab repository to your local machine. This gives you access to all lab files, including the notebook and data files.

1. Open a terminal and run:

    ```bash
    git clone https://github.com/MicrosoftLearning/DP-750T00-Implement-Data-Engineering-Solutions-using-Azure-Databricks.git DP-750
    ```

    The repository will be cloned into a folder called `DP-750` in your current directory.

---

## Import the lab notebook

1. In the Databricks workspace, click **Workspace** in the left sidebar.
2. Navigate to or create the folder where you want to store your lab notebooks (for example, your home folder).
3. Click the **⋮** (kebab) menu or right-click the folder, then select **Import**.
4. Select **File**, browse to `DP-750/Allfiles/03-create-and-organize-objects-in-unity-catalog.ipynb`, and click **Import**.
5. Open the imported notebook. In the compute selector at the top of the notebook, choose **Serverless** compute.

---

## Work through the notebook

Open the imported notebook and follow the instructions in each cell to complete Exercises 1–6. When you reach the **Next steps** cell at the end of the notebook, return here and continue with the exercises below.

---

## Exercise A (non-notebook): Explore Unity Catalog in the UI

Before writing any code, take a few minutes to explore Unity Catalog through the **Catalog Explorer**.

1. In the left sidebar, click **Catalog**.
2. Browse the existing catalogs in your metastore. Note how they are organized by name.
3. Click into a catalog and then a schema to see its tables, views, and volumes.
4. Click on a table to view its **schema**, **sample data**, and **details** (owner, tags, comments).
5. Notice the **three-level namespace** at the top of the details pane: `catalog.schema.table`.

> 💡 You will create your own catalog structure in the notebook exercises that follow.

---

## Exercise B (non-notebook): Configure an AI/BI Genie Space (optional)

This optional task requires a Genie Space, which is configured entirely through the Databricks UI — not in a notebook.

After completing the notebook exercises, you can optionally create a Genie Space to experience natural language querying over your Lakeside University data.

1. In the left sidebar, click **+ New** > **Genie space**.
2. Name the space **Lakeside University Analytics**.
3. Under **Data**, add the following tables:
   - `edu_dev.silver.students`
   - `edu_dev.silver.courses`
   - `edu_dev.silver.enrollments`
   - `edu_dev.silver.vw_student_enrollments`
   - `edu_dev.gold.vw_department_enrollment_stats`
4. For the `enrollments.grade` column, update the description to: *"Numerical grade on a 0.0–10.0 scale where 8.5+ is an A, 7.0+ is a B, 5.5+ is a C, 4.0+ is a D, and below 4.0 is an F."*
5. Navigate to the **Chat** tab and ask: *"Which department has the highest average grade?"*
6. Review the SQL Genie generated and compare it to your `vw_department_enrollment_stats` materialized view.

> 🤖 **Databricks Assistant tip:** You can ask the Databricks Assistant from within a Genie space to help you write SQL instructions or define synonyms for columns.

---

## Clean up (optional)

If you want to remove the resources created during this lab, run the following in your notebook:

```sql
DROP CATALOG IF EXISTS edu_dev CASCADE;
```

> ⚠️ This will permanently delete all schemas, tables, views, volumes, and functions created under `edu_dev`. Only run this if you are sure you no longer need these objects.
