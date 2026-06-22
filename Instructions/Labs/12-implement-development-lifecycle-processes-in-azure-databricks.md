---
lab:
  index: 12
  title: Implement Development Lifecycle Processes in Azure Databricks
  module: Implement Development Lifecycle Processes in Azure Databricks
  module-url: https://learn.microsoft.com/training/wwl-databricks/implement-development-lifecycle-processes-in-azure-databricks/
  notebook: https://github.com/MicrosoftLearning/DP-750T00-Implement-Data-Engineering-Solutions-using-Azure-Databricks/blob/main/Allfiles/12-implement-development-lifecycle-processes-in-azure-databricks.ipynb
  description: In this lab, you implement a testing strategy for a data transformation pipeline using pytest, then package and deploy the pipeline as a Declarative Automation Bundle using the Databricks CLI.
  duration: 45 minutes
  level: 300
  islab: true
  primarytopics:
    - Azure Databricks
---

# Lab 12: Implement Development Lifecycle Processes in Azure Databricks

## Introduction

You are a data engineer responsible for maintaining an **order processing pipeline** used by your warehouse operations team. The pipeline reads raw order data, removes invalid records, normalizes status codes, and computes tax-inclusive totals.

As the pipeline moves toward production, your team has decided to adopt proper **software development lifecycle (SDLC) practices**. That means:

- Implementing a **testing strategy** so bugs are caught before deployment
- Packaging the pipeline as a **Declarative Automation Bundle (DAB)** so it can be deployed consistently across environments
- Using the **Databricks CLI** to validate, preview, and deploy the bundle

This lab is structured in three parts:

| Part | Topic | Where you work |
|------|-------|--------|
| **Part 1** | Implement unit tests with pytest | In a notebook running on Serverless compute in your Azure Databricks workspace |
| **Part 2** | Configure a Declarative Automation Bundle | In a **PowerShell terminal on your local machine** (the Databricks CLI runs locally) |
| **Part 3** | Deploy and verify the bundle with the Databricks CLI | In the same **local PowerShell terminal**, then verify in the Databricks workspace UI |

> **Important:** Parts 2 and 3 use the **Databricks CLI installed on your computer/lab virtual machine**, not a terminal inside the Databricks workspace. Open a local PowerShell window for those parts and keep it open throughout.

---

## 🤖 Use Genie Code throughout this lab

You are expected and **encouraged** to use the **Genie Code** for every exercise. 

To open Genie Code, select the ![assistant-icon](https://raw.githubusercontent.com/MicrosoftLearning/DP-750T00-Implement-Data-Engineering-Solutions-using-Azure-Databricks/refs/heads/main/Allfiles/media/genie-code.svg) on the right side of any notebook cell, or use the keyboard shortcut.

Use it to:
- Generate pytest fixtures and test functions
- Understand error messages and fix failing tests
- Draft YAML bundle configuration
- Look up CLI command syntax

---

## Prerequisites

- An **Azure Databricks Premium workspace** provisioned using [Lab 00: Set up your Azure Databricks environment](00-setup.md).
- You have permission to create jobs in the workspace (required for Part 3).
- Basic familiarity with Python and pytest.

---

## Part 1: Implement a Testing Strategy (Notebook)

### Import the notebook

1. In your Databricks workspace, click **Workspace** in the left sidebar.
2. Navigate to or create a folder where you want to store the lab.
3. Click the **⋮** (kebab menu) or right-click the folder, then select **Import**.
4. Choose **URL**, enter the following URL, and click **Import**:
   `https://raw.githubusercontent.com/MicrosoftLearning/DP-750T00-Implement-Data-Engineering-Solutions-using-Azure-Databricks/refs/heads/main/Allfiles/12-implement-development-lifecycle-processes-in-azure-databricks.ipynb`
5. Open the imported notebook and, in the compute selector at the top, choose **Serverless** compute.

### Work through the notebook

The notebook contains three exercises:

- **Exercise 1** — Install pytest and review the provided **transforms.py** module that you will test.
- **Exercise 2** — Write unit tests for each transformation function using pytest fixtures.
- **Exercise 3** — Write an integration test that runs the full pipeline against data created with a Spark session.

Complete all exercises before continuing to Part 2.

---

## Part 2: Configure a Declarative Automation Bundle

Declarative Automation Bundles (DABs) let you define your Databricks resources — jobs, pipelines, notebooks — as **infrastructure-as-code** in YAML configuration files. This makes deployments repeatable and auditable.

In this part, you create a bundle for the order processing job using the **Databricks CLI** on your local machine.

> **Where you work:** All commands in Parts 2 and 3 run in a **PowerShell terminal on your own computer or Lab Virtual Machine**. Open **Windows Terminal** or **PowerShell** now and keep it open for the rest of the lab. The Databricks CLI talks to your workspace over the network — you do **not** run these commands inside the Databricks web UI.

### Install and configure the Databricks CLI

1. In your local PowerShell window, install the Databricks CLI:

   ```powershell
   winget install Databricks.DatabricksCLI
   ```

   Then **close and reopen** PowerShell so the new `databricks` command is on your PATH, and verify the installation:

   ```powershell
   databricks --version
   ```

   *Expected outcome:* the command prints a version number (for example, `Databricks CLI v0.x.x`). If it reports that the command is not recognized, reopen PowerShell and try again.

2. Authenticate the CLI against your Azure Databricks workspace:

   ```powershell
   databricks auth login --host https://<your-workspace-url>
   ```

   Replace **<your-workspace-url>** with the URL of your workspace (for example, https://adb-1234567890123456.7.azuredatabricks.net). You can copy this from the address bar of your browser when the workspace is open. When prompted for a **profile name**, press Enter to accept the default. A browser window opens — sign in and approve the request.

   *Expected outcome:* PowerShell displays `Profile DEFAULT was successfully saved`, confirming the CLI is now authenticated to your workspace.

3. Create a new project directory and the two subfolders the bundle needs, then move into the project directory:

   ```powershell
   mkdir ~/order-pipeline-bundle; cd ~/order-pipeline-bundle
   mkdir notebooks, resources
   ```

   *Expected outcome:* your PowerShell prompt now ends in `order-pipeline-bundle`, and the folder contains empty `notebooks` and `resources` subfolders. Run `ls` to confirm.

### Create the bundle configuration file

The **databricks.yml** file is the heart of a Declarative Automation Bundle — it describes the resources to deploy. In this step you create that file in the root of your `order-pipeline-bundle` folder. Your task is to produce a databricks.yml file that meets the following requirements:

- Bundle name: `order-pipeline-bundle`
- A variables section with:
  - An `environment` variable (default: `development`)
  - A `cluster_policy_id` variable with a description and a **default value of an empty string (`""`)**. A cluster policy ID enforces configuration rules on job clusters, and in a production bundle you'd reference this value on those clusters. To keep this lab simple — the job uses Serverless compute — the empty-string default makes the variable **optional**, so you don't have to supply it at deploy time. If you'd rather use a real value, copy a policy ID from your workspace under **Compute** > **Policies**: open a policy and use the ID shown in its details or URL.
- A resources section defining a job named order-pipeline-job that:
  - Has a display name using the environment variable: ${var.environment}-order-pipeline
  - Has two notebook tasks:
    - validate-data — runs ./notebooks/validate.py
    - transform-data — depends on validate-data and runs ./notebooks/transform.py
- A targets section with:
  - A dev target (default, development mode, environment = development)
  - A prod target (production mode, with its own workspace host and environment = production)

You create this file with a plain-text editor rather than from the command line, so it's easier to read and edit the YAML.

1. Open the `order-pipeline-bundle` folder in a text editor:

   - **VS Code** (recommended): in your PowerShell window, run `code .` to open the current folder. If the `code` command isn't available, open VS Code manually and select **File** > **Open Folder**, then choose your `order-pipeline-bundle` folder.
   - **Notepad**: run `notepad databricks.yml`. Notepad asks whether to create the file because it doesn't exist yet — select **Yes**.

2. Create a new file named **databricks.yml** in the root of the `order-pipeline-bundle` folder (in VS Code, select **New File** and name it `databricks.yml`).

3. Copy the starter configuration below into the file. It already includes the bundle name, the `environment` variable, the first task, and the `dev` target. Complete the three sections marked with `# TODO`:

   ```yaml
   bundle:
     name: order-pipeline-bundle

   variables:
     environment:
       description: The deployment environment name
       default: development
     # TODO: Add a variable named 'cluster_policy_id'.
     # Give it a 'description' and a 'default' value of "" (an empty string),
     # which makes the variable optional so you don't have to supply it at deploy time.
     # (A real cluster policy ID comes from Compute > Policies in your workspace,
     # but you don't need one for this lab.)

   resources:
     jobs:
       order-pipeline-job:
         name: ${var.environment}-order-pipeline
         tasks:
           - task_key: validate-data
             notebook_task:
               notebook_path: ./notebooks/validate.py
           # TODO: Add a second task named 'transform-data'
           # It should depend on 'validate-data' and run ./notebooks/transform.py
           # Refer to the 'depends_on' key in the Declarative Automation Bundle schema.

   targets:
     dev:
       default: true
       mode: development
       variables:
         environment: development
     # TODO: Add a 'prod' target that:
     # - sets mode to production
     # - sets a workspace host (use a placeholder URL for now)
     # - overrides the environment variable to 'production'
   ```

   > **Tip:** YAML is indentation-sensitive — use **two spaces** per level and never use tabs. Keep each `# TODO` block aligned with the example lines around it.

4. **Save** the file (in VS Code, press **Ctrl+S**; in Notepad, select **File** > **Save**). Make sure it's saved as `databricks.yml` directly inside the order-pipeline-bundle folder, not inside notebooks or resources.

> 🤖 **Genie Code tip:** Ask *"Show me a complete Declarative Automation Bundle databricks.yml example with two targets, job tasks, and custom variables"* to get a full reference configuration you can adapt.

> **Need the solution?** A completed reference file with all `# TODO` sections filled in is available at [Allfiles/answers/12-databricks-bundle-with-answers.yml](https://github.com/MicrosoftLearning/DP-750T00-Implement-Data-Engineering-Solutions-using-Azure-Databricks/blob/main/Allfiles/answers/12-databricks-bundle-with-answers.yml). Try to complete the `# TODO` sections yourself first, then compare your work against the solution.

*Expected outcome:* a databricks.yml file exists in the root of order-pipeline-bundle, containing the cluster_policy_id variable, the second transform-data task with its depends_on, and the prod target you added. You can confirm it from PowerShell with `Get-Content databricks.yml`.

### Create placeholder notebooks (required for validation)

Bundle validation checks that every notebook referenced in `databricks.yml` actually exists on disk **and is a Databricks notebook**. A plain `.py` file isn't enough — a Databricks source-format notebook must begin with the special first line `# Databricks notebook source`, and the file must be saved as **UTF-8 without a byte-order mark (BOM)**. If the file has a BOM or uses a different encoding, the Databricks CLI can't detect the header and validation fails with *"... is not a notebook"* — even though the text looks correct in an editor.

Run these commands in your local PowerShell window (still inside the `order-pipeline-bundle` folder) to create both placeholder notebooks as BOM-free UTF-8:

```powershell
[System.IO.File]::WriteAllLines("$PWD\notebooks\validate.py",  @("# Databricks notebook source", "# validate"))
[System.IO.File]::WriteAllLines("$PWD\notebooks\transform.py", @("# Databricks notebook source", "# transform"))
```

> **Why not `Set-Content`?** `Set-Content` (and `Out-File`) can prepend a BOM or use UTF-16 depending on your PowerShell version, which is exactly what triggers the *"is not a notebook"* error. `[System.IO.File]::WriteAllLines` always writes UTF-8 **without** a BOM, so the magic header is the very first thing in the file.

*Expected outcome:* the `notebooks` folder contains `validate.py` and `transform.py`, each starting with `# Databricks notebook source`. Run `Get-Content notebooks/validate.py` to confirm the header is on line 1. When you rerun `databricks bundle validate`, the *"is not a notebook"* error is gone.

---

## Part 3: Deploy and Verify the Bundle with the Databricks CLI

With your bundle configured, you use the **Databricks CLI** to validate, preview, and deploy it to your workspace.

> **Where you work:** Run every command in this part in your **local PowerShell window**, from inside the `order-pipeline-bundle` folder. If you closed PowerShell, reopen it and run `cd ~/order-pipeline-bundle` first.

### Step 1 — Validate the bundle

Run the following command from inside the order-pipeline-bundle directory. This checks that your databricks.yml is syntactically correct and references valid resources.

```powershell
databricks bundle validate
```

*Expected outcome:* the command prints a summary showing the bundle **Name** (`order-pipeline-bundle`), the **Target** (`dev`), and the **Workspace** path, followed by `Validation OK!`. If you see errors instead, read the message (it usually names the line or key at fault), fix the YAML, and run the command again.

> 🤖 **Tip:** Copy any validation error messages and paste them into Genie Code to get an explanation and suggested fix.

### Step 2 — Preview the deployment plan

Before making any changes to your workspace, preview what the deployment would create or update:

```powershell
databricks bundle plan
```

Review the output. *Expected outcome:* the plan reports that the order-pipeline-job would be **created** (since it doesn't exist yet) — no resources are changed at this stage. To run the plan against a specific target, name it explicitly:

```powershell
databricks bundle plan -t dev
```

### Step 3 — Deploy the bundle

Deploy the bundle to the `dev` target:

```powershell
databricks bundle deploy -t dev
```

During deployment, the CLI:
- Uploads your notebook files to the workspace
- Creates the order-pipeline-job in the **Jobs & Pipelines** section of your workspace (prefixed with `[dev <username>]` because development mode is active)

*Expected outcome:* the command finishes with a message such as `Deployment complete!` and no errors.

### Step 4 — Verify the deployed resources

Confirm the deployment succeeded:

```powershell
databricks bundle summary
```

*Expected outcome:* the output lists the deployed **order-pipeline-job** along with a direct **URL** to it. Open that URL in your browser (or go to **Jobs & Pipelines** in the workspace) and confirm that:
- The job appears with a name prefixed by `[dev <your-username>]`.
- Opening the job shows both tasks — **validate-data** and **transform-data** — with **transform-data** depending on **validate-data** in the task graph.

> 🤖 **Tip:** Ask *"What does Declarative Automation Bundle development mode do to job names and schedules?"* to understand why the job is prefixed with your username.

### Step 5 — Clean up (optional)

To remove the deployed resources from your workspace, run the following from the `order-pipeline-bundle` folder:

```powershell
databricks bundle destroy -t dev
```

When prompted, confirm that you want to delete the job that was created. *Expected outcome:* the CLI reports the resources were destroyed, and the job no longer appears in **Jobs & Pipelines**.

---

## Summary

In this lab you:

- Implemented **unit tests** using pytest fixtures to verify individual transformation functions
- Configured a **Declarative Automation Bundle** with variables, job resources, and multi-environment targets
- Used the **Databricks CLI** to validate, plan, deploy, and verify a bundle deployment
