---
lab:
  index: 01
  title: Explore Azure Databricks
  module: Explore Azure Databricks
  module-url: https://learn.microsoft.com/training/modules/explore-azure-databricks/
  notebook: https://github.com/MicrosoftLearning/DP-750T00-Implement-Data-Engineering-Solutions-using-Azure-Databricks/blob/main/Allfiles/01-explore-azure-databricks.ipynb
  description: In this lab, you explore the Azure Databricks workspace UI, upload a sample dataset to a Unity Catalog volume, and work with notebook features including Python, SQL magic commands, and Markdown. You use the Databricks Assistant throughout to generate and refine code in the context of CityMoves Transit, a fictional public transportation authority.
  duration: 30 minutes
  level: 200
  islab: true
  primarytopics:
    - Azure Databricks
---

# Lab 01: Explore Azure Databricks

In this lab, you take your first steps with Azure Databricks in the context of **CityMoves Transit**, a regional public transportation authority managing bus, tram, and train services across a metropolitan area. As a newly onboarded data engineer, your goal is to get familiar with the Azure Databricks workspace before diving into data pipelines and analytics in later labs.

By the end of this lab, you will have:

- Navigated the key areas of the Azure Databricks Workspace UI.
- Uploaded a sample dataset using the data ingestion interface.
- Explored notebook features including Python code cells, SQL magic commands, and Markdown documentation.

This lab should take approximately **45 minutes** to complete.

---

## Prerequisites

Before starting this lab, ensure you have:

- Access to an **Azure Databricks Premium workspace** (already provisioned for you).
- Familiarity with basic Azure portal navigation.
- No prior Databricks experience is required.

---

## Exercise 1: Navigate the Azure Databricks Workspace UI

Before writing any code, let's explore the Azure Databricks environment. Getting comfortable with the UI will help you work more efficiently throughout this course.

### Task 1: Explore the workspace sidebar

1. In your Azure Databricks workspace, look at the **left sidebar**. Identify the following sections:
   - **Workspace** — your personal and shared folders for notebooks and files.
   - **Recents** — recently opened objects.
   - **Catalog** — your Unity Catalog data assets (tables, volumes, schemas).
   - **Jobs & Pipelines** — automated workflows.
   - **Compute** — cluster and SQL warehouse management.
   - **Marketplace** — partner data and solutions.

2. Click **+ New** (top of the sidebar) and review the types of objects you can create (notebooks, queries, clusters, dashboards, etc.). **Do not create anything yet** — this is just for exploration.

3. Use the **Search** bar at the top to search for `routes`. Nothing will appear yet, but you will use this later to locate data assets after ingestion.

### Task 2: Explore the Databricks Assistant

The **Databricks Assistant** is an AI-powered pair programmer built directly into Azure Databricks. It can generate code, explain errors, suggest improvements, and answer questions — all without leaving the user interface. You are expected and encouraged to use it throughout this lab and all future labs.

1. From the Azure Databricks home page, click the **Databricks Assistant** icon (![assistant-icon](https://raw.githubusercontent.com/MicrosoftLearning/DP-750T00-Implement-Data-Engineering-Solutions-using-Azure-Databricks/refs/heads/main/Allfiles/media/databricks-assistant.svg)) in the top-right corner of the page to open the Assistant panel.

2. Type the following prompt and observe the response:

    ```
    What can I do with Azure Databricks as a data engineer?
    ```

3. Review the answer. Notice how the Assistant provides contextual, workspace-aware guidance.

> 💡 **From this point on, whenever you are asked to write code or SQL, use the Databricks Assistant. Describe what you want in plain language, then adapt and run the suggestion.**

### Task 3: Upload a sample transportation dataset

CityMoves Transit has provided a CSV file with route information. Your task is to upload it to the workspace using the data ingestion UI so it is available for later labs.

Before uploading, you need a Unity Catalog **volume** to store the file. Follow these steps to create one:

1. In the Databricks workspace sidebar, click **Catalog**.
2. In the Catalog Explorer, expand the **adb-dp750** catalog, then expand the **default** schema.
3. Click the **⋮** menu next to **default**, then select **Create** > **Volume**.
4. Enter `lab_data` as the volume name, leave the type as **Managed**, and click **Create**.

Now upload the data file:

5. In the Databricks workspace sidebar, click **+ New**, then select **Add or upload data**.

6. In the data upload interface, select **Upload files**.

7. Download the file from the following URL, then click **Browse** to select it:
   `https://raw.githubusercontent.com/MicrosoftLearning/DP-750T00-Implement-Data-Engineering-Solutions-using-Azure-Databricks/refs/heads/main/Allfiles/data/routes.csv`

8. When prompted for a destination, select the volume you just created: `adb-dp750` > `default` > `lab_data`.

9. After the upload completes, navigate to **Catalog** in the left sidebar and locate the uploaded file. Expand the catalog hierarchy (`adb-dp750` > `default` > `lab_data`) to verify that `routes.csv` is visible.

    > **Note**: You do not need to query or load the data in this lab. The goal is simply to get familiar with the upload workflow. You will work with this data in later labs.

---

## Import the lab notebook

Now that the data file is uploaded, import the lab notebook into your Databricks workspace.

1. In your Azure Databricks workspace, click **Workspace** in the left sidebar.

2. Navigate to or create a folder where you want to store the lab (for example, `Labs/01-explore-azure-databricks`).

3. Click the **⋮** (kebab) menu next to the folder, or right-click it, then select **Import**.

4. Choose **URL**, enter the following URL, and click **Import**:
   `https://raw.githubusercontent.com/MicrosoftLearning/DP-750T00-Implement-Data-Engineering-Solutions-using-Azure-Databricks/refs/heads/main/Allfiles/01-explore-azure-databricks.ipynb`

5. Open the imported notebook. In the compute selector at the top of the notebook, choose **Serverless** compute.

---

## Continue in the notebook

You have completed the UI-based exercises. Now open the imported notebook `01-explore-azure-databricks.ipynb` and continue with the hands-on coding exercises.

Make sure **Serverless** compute is selected before running any cells.
