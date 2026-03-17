---
lab:
  title: Select and Configure Compute in Azure Databricks
  module: Select and Configure Compute in Azure Databricks
  description: In this lab, you create and configure an all-purpose cluster in Azure Databricks, install libraries both cluster-scoped and notebook-scoped, and use the faker library to generate and analyze synthetic patient admission records with PySpark. 
  duration: 30 minutes
  level: 300
  islab: true
  primarytopics:
    - Azure Databricks
---

# Select and Configure Compute in Azure Databricks

In this lab, you work as a data engineer at **HealthBridge Analytics**, a regional healthcare organization that processes millions of patient records daily — from hospital admissions and lab results to billing and compliance data. Your team relies on Azure Databricks to run data engineering pipelines, ad hoc analysis, and pipeline validation workloads.

Before your team can process any data, you need to make sure the right compute resources are configured: a well-tuned cluster for interactive development, and properly installed libraries so your notebooks have the dependencies they need.

By the end of this lab, you will have:

- Created and configured an all-purpose cluster with appropriate performance settings, Databricks Runtime, and Photon acceleration.
- Installed a cluster-scoped library via the Databricks UI.
- Installed libraries notebook-scoped using `%pip install` and verified the installation.
- Generated and analyzed synthetic patient data using an installed library.

This lab should take approximately **30 minutes** to complete.

---

## 🤖 Use the Databricks Assistant throughout this lab

You are expected and encouraged to use the **Databricks Assistant** for every exercise. The Assistant can help you write code, explain concepts, suggest fixes, and answer questions directly within the Databricks workspace.

> **How to open the Assistant:** Select the ![assistant-icon](https://raw.githubusercontent.com/MicrosoftLearning/DP-750T00-Implement-Data-Engineering-Solutions-using-Azure-Databricks/refs/heads/master/Allfiles/media/databricks-assistant.svg) icon in the top-right toolbar of any notebook, or press `Ctrl+Shift+P` and search for "Databricks Assistant".

Every task in the notebook includes a suggested prompt you can paste directly into the Assistant. Use it — that is the point!

---

## Prerequisites

Before starting this lab, ensure you have:

- Access to an **Azure Databricks Premium workspace** (already provisioned for you).
- **Metastore admin** or **workspace admin** permissions, or a user account with cluster creation rights.
- Basic familiarity with the Azure Databricks workspace UI.

---

## Clone the lab repository and import the notebook

Before starting the exercises, clone the lab repository and import the notebook into your Databricks workspace.

1. Open a terminal and run:

    ```bash
    git clone https://github.com/MicrosoftLearning/DP-750T00-Implement-Data-Engineering-Solutions-using-Azure-Databricks.git DP-750
    ```

2. In your Azure Databricks workspace, select **Workspace** in the left sidebar.

3. Navigate to or create a folder where you want to store the lab (for example, your personal home folder).

4. Select the **⋮** (kebab) menu next to the folder or right-click the folder, then select **Import**.

5. Choose **File**, browse to `DP-750/Allfiles/02-select-and-configure-compute.ipynb`, and select **Import**.

6. Open the imported notebook. You will attach it to **Serverless** compute in a later step.

---

## Exercise 1: Create and Configure an All-Purpose Cluster

HealthBridge's data engineering team needs a shared cluster for interactive development. Your job is to create and configure this cluster with appropriate settings for a collaborative healthcare analytics workload.

### Task 1.1: Create a new all-purpose cluster

1. In the left sidebar, select **Compute**.
2. Select **Create compute**.
3. Set the **Simple form** dropdown to **OFF**.
4. In the cluster creation form, set the following:

    | Setting          | Value              |
    | ---------------- | ------------------ |
    | **Cluster name** | `healthbridge-dev` |
    | **Policy**       | Unrestricted       |
    | **Cluster mode** | Multi node         |
    | **Access mode**  | Shared             |

5. Do **not** select Create yet — continue with the settings below.

### Task 1.2: Configure performance and runtime settings

Still on the cluster creation form:

1. Under **Databricks Runtime version**, select the **latest LTS (Long Term Support)** version.

    > **Tip:** LTS versions are recommended for shared team clusters because they receive extended support and are more stable than non-LTS versions.

2. Under **Worker type**, select a **memory-optimized** instance (for example, an **E-series** VM). Healthcare analytics workloads often involve large joins across patient records and lab result tables, which benefit from higher memory-to-core ratios.

3. Under **Worker**, enable **Autoscaling** and set:
    - Minimum workers: `1`
    - Maximum workers: `3`

4. Under **Auto termination**, enable it and set the timeout to **15 minutes**.

    > **Why 15 minutes?** This prevents the cluster from staying idle between analysis sessions, which is important for cost control in healthcare organizations with strict budget governance.

5. Scroll up to confirm that **Photon Acceleration** is **enabled**.

6. Select **Create compute**.

Wait for the cluster to reach a **Running** state before proceeding. This might take several minutes.

---

## Exercise 2: Install a Cluster-Scoped Library

Your team uses the `faker` library to generate synthetic patient datasets for pipeline testing. You need to make this library available to all notebooks that run on the `healthbridge-dev` cluster.

[faker](https://faker.readthedocs.io/) is a Python library that generates realistic fake data — such as names, addresses, phone numbers, dates, and more — for use in testing, prototyping, and seeding databases. It supports many locales and data categories, making it easy to produce large volumes of plausible synthetic data without using real personal information.

### Task 2.1: Install faker as a cluster-scoped library

1. In the **Compute** page, select your `healthbridge-dev` cluster.
2. Select the **Libraries** tab.
3. Select **Install new**.
4. Set the following:

    | Setting            | Value           |
    | ------------------ | --------------- |
    | **Library source** | PyPI            |
    | **Package**        | `faker==40.8.0` |

    > **Why pin a version?** In a healthcare data engineering context, reproducibility is critical. Pinning the exact version of `faker` ensures that every team member and every pipeline run uses the same library, preventing unexpected behavior from upstream version changes.

5. Select **Install**.

6. Wait for the library status to show **Installed** on the Libraries tab.

    > **Note:** Cluster-scoped libraries are automatically reinstalled every time the cluster restarts. This keeps your environment consistent without manual intervention.

---

## Stop or delete the cluster

Now that you have explored cluster configuration and library installation, the `healthbridge-dev` cluster is no longer needed for the remaining exercises. To avoid unnecessary compute costs, stop or delete it before continuing.

1. In the left sidebar, select **Compute**.
2. Select the `healthbridge-dev` cluster.
3. Select **Terminate** to stop the cluster, or select the **⋮** menu and select **Delete** to remove it entirely.

    > **Note:** Since the remaining exercises run on **Serverless** compute, you can safely delete the cluster. Deleting it prevents it from being accidentally restarted and incurring further costs.

---

## Exercise 3: Work with Libraries in a Notebook

Now that the cluster is configured, switch to the notebook to complete the remaining code-based exercises.

### Attach the notebook to Serverless compute

1. Open the `02-select-and-configure-compute` notebook you imported earlier.
2. In the compute selector at the top of the notebook, choose **Serverless** compute.

    > **Note:** Exercises 3 and 4 in the notebook use **Serverless** compute. The cluster you created in Exercise 1 is relevant to understanding compute configuration — the notebook itself runs on Serverless to demonstrate notebook-scoped library installation, which works independently of cluster-scoped libraries.

3. Continue with the exercises inside the notebook.
