---
lab:
  title: Create and organize objects in Unity Catalog
  module: Create and organize objects in Unity Catalog
  description: In this lab, you build a complete Unity Catalog namespace for a university data platform — creating a catalog, medallion schemas, managed tables with primary and foreign key constraints, views, a volume, and a reusable SQL function. You practice DDL operations such as adding columns and applying governance tags, and explore how Unity Catalog organizes and governs structured data at every layer of the medallion architecture. By the end, you will have a fully structured, query-ready environment that reflects real-world data engineering practices on Azure Databricks.
  duration: 45 minutes
  level: 300
  islab: true
  primarytopics:
    - Azure Databricks
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

To open the Databricks Assistant, select the ![assistant-icon](https://raw.githubusercontent.com/MicrosoftLearning/DP-750T00-Implement-Data-Engineering-Solutions-using-Azure-Databricks/refs/heads/master/Allfiles/media/databricks-assistant.svg) on the right side of any notebook cell, or use the keyboard shortcut.

> 💡 **Tip:** Do not just copy and paste the Assistant's output blindly. Read it, understand it, and adapt it to the specific requirements of each task. The Assistant is a tool to accelerate your thinking, not replace it.

---

## Prerequisites

Before starting this lab, ensure you have:

- Access to an **Azure Databricks Premium workspace** (already provisioned for you).
- An active **Unity Catalog metastore** attached to the workspace.
- The **CREATE CATALOG** privilege on the metastore (granted by your instructor or workspace admin).
- Familiarity with basic SQL (`CREATE TABLE`, `SELECT`, `JOIN`).

---

## Import the lab notebook

1. In the Databricks workspace, select **Workspace** in the left sidebar.
2. Navigate to or create the folder where you want to store your lab notebooks (for example, your home folder).
3. Select the **⋮** (kebab) menu or right-click the folder, then select **Import**.
4. Select **URL**, enter the following URL, and select **Import**:
   `https://raw.githubusercontent.com/MicrosoftLearning/DP-750T00-Implement-Data-Engineering-Solutions-using-Azure-Databricks/refs/heads/master/Allfiles/03-create-and-organize-objects-in-unity-catalog.ipynb`
5. Open the imported notebook. In the compute selector at the top of the notebook, choose **Serverless** compute.

---

## Work through the notebook

Open the imported notebook and follow the instructions in each cell to complete Exercises 1–6. When you reach the **Next steps** cell at the end of the notebook, return here and continue with the exercise below.

---

## Exercise: Configure an AI/BI Genie Space (optional)

This optional task requires a Genie Space, which is configured entirely through the Databricks UI — not in a notebook.

After completing the notebook exercises, you can optionally create a Genie Space to experience natural language querying over your Lakeside University data.

1. In the left sidebar, select **+ New** > **Genie space**.
2. Under **Data**, add the following tables:
   - `edu_dev.silver.students`
   - `edu_dev.silver.courses`
   - `edu_dev.silver.enrollments`
   - `edu_dev.silver.vw_student_enrollments`
   - `edu_dev.gold.vw_department_enrollment_stats`
3. Name the space **Lakeside University Analytics**.
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
