---
lab:
  index: 12
  title: Implement Development Lifecycle Processes with Azure Databricks
  module: Implement Development Lifecycle Processes in Azure Databricks
  description: In this lab, you implement a testing strategy for a data transformation pipeline using pytest, then package and deploy the pipeline as a Databricks Asset Bundle using the Databricks CLI.
  duration: 45 minutes
  level: 300
  islab: true
  primarytopics:
    - Azure Databricks
---

# Lab 12: Implement Development Lifecycle Processes

## Introduction

You are a data engineer responsible for maintaining an **order processing pipeline** used by your warehouse operations team. The pipeline reads raw order data, removes invalid records, normalizes status codes, and computes tax-inclusive totals.

As the pipeline moves toward production, your team has decided to adopt proper **software development lifecycle (SDLC) practices**. That means:

- Implementing a **testing strategy** so bugs are caught before deployment
- Packaging the pipeline as a **Databricks Asset Bundle (DAB)** so it can be deployed consistently across environments
- Using the **Databricks CLI** to validate, preview, and deploy the bundle

This lab is structured in three parts:

| Part | Topic | Where |
|------|-------|--------|
| **Part 1** | Implement unit tests with pytest | Notebook |
| **Part 2** | Configure a Databricks Asset Bundle | Workspace terminal |
| **Part 3** | Deploy and verify the bundle with the Databricks CLI | Workspace terminal |

---

## 🤖 Use the Databricks Assistant throughout this lab

You are expected and **encouraged** to use the **Databricks Assistant** for every exercise. 

To open the Databricks Assistant, select the ![assistant-icon](https://raw.githubusercontent.com/MicrosoftLearning/DP-750T00-Implement-Data-Engineering-Solutions-using-Azure-Databricks/refs/heads/main/Allfiles/media/databricks-assistant.svg) on the right side of any notebook cell, or use the keyboard shortcut.

Use it to:
- Generate pytest fixtures and test functions
- Understand error messages and fix failing tests
- Draft YAML bundle configuration
- Look up CLI command syntax

---

## Prerequisites

- An **Azure Databricks Premium** workspace is already provisioned and you have access to it.
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

- **Exercise 1** — Install pytest and review the provided `transforms.py` module that you will test.
- **Exercise 2** — Write unit tests for each transformation function using pytest fixtures.
- **Exercise 3** — Write an integration test that runs the full pipeline against data created with a Spark session.

Complete all exercises before continuing to Part 2.

---

## Part 2: Configure a Databricks Asset Bundle

Databricks Asset Bundles (DABs) let you define your Databricks resources — jobs, pipelines, notebooks — as **infrastructure-as-code** in YAML configuration files. This makes deployments repeatable and auditable.

In this part, you create a bundle for the order processing job using the **Databricks CLI** on your local machine.

### Install and configure the Databricks CLI

1. Install the Databricks CLI. On macOS, use Homebrew:

   ```bash
   brew tap databricks/tap
   brew install databricks
   ```

   On Windows (PowerShell):

   ```powershell
   winget install Databricks.DatabricksCLI
   ```

   Verify the installation:

   ```bash
   databricks --version
   ```

2. Authenticate the CLI against your Azure Databricks workspace:

   ```bash
   databricks auth login --host https://<your-workspace-url>
   ```

   Replace `<your-workspace-url>` with the URL of your workspace (for example, `https://adb-1234567890123456.7.azuredatabricks.net`). Follow the browser prompts to complete authentication.

3. Create a new project directory and navigate into it:

   ```bash
   mkdir ~/order-pipeline-bundle && cd ~/order-pipeline-bundle
   mkdir notebooks resources
   ```

### Create the bundle configuration file

Your task is to create a `databricks.yml` file with the following requirements:

- Bundle name: `order-pipeline-bundle`
- A `variables` section with:
  - An `environment` variable (default: `development`)
  - A `cluster_policy_id` variable with a description and no default value
- A `resources` section defining a job named `order-pipeline-job` that:
  - Has a display name using the `environment` variable: `${var.environment}-order-pipeline`
  - Has two notebook tasks:
    - `validate-data` — runs `./notebooks/validate.py`
    - `transform-data` — depends on `validate-data` and runs `./notebooks/transform.py`
- A `targets` section with:
  - A `dev` target (default, development mode, `environment` = `development`)
  - A `prod` target (production mode, with its own workspace host and `environment` = `production`)

Use the heredoc below as a **starting point** and fill in the sections marked with `# TODO`:

```bash
cat > databricks.yml << 'EOF'
bundle:
  name: order-pipeline-bundle

variables:
  environment:
    description: The deployment environment name
    default: development
  # TODO: Add a variable named 'cluster_policy_id'
  # It should have a description and no default value.

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
        # Refer to the 'depends_on' key in the Databricks Asset Bundle schema.

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
EOF
```

> 🤖 **Databricks Assistant tip:** Ask *"Show me a complete Databricks Asset Bundle databricks.yml example with two targets, job tasks, and custom variables"* to get a full reference configuration you can adapt.

### Create placeholder notebooks (required for validation)

Bundle validation checks that referenced notebooks exist. Create two placeholder notebook files:

```bash
echo "# validate" > notebooks/validate.py
echo "# transform" > notebooks/transform.py
```

---

## Part 3: Deploy and Verify the Bundle with the Databricks CLI

With your bundle configured, you use the **Databricks CLI** to validate, preview, and deploy it to your workspace.

### Step 1 — Validate the bundle

Run the following command from inside the `order-pipeline-bundle` directory. This checks that your `databricks.yml` is syntactically correct and references valid resources.

```bash
databricks bundle validate
```

If validation succeeds, you will see a summary showing the bundle name, target environment, and workspace path. If there are errors, review the output and fix the YAML before continuing.

> 🤖 **Tip:** Copy any validation error messages and paste them into the Databricks Assistant to get an explanation and suggested fix.

### Step 2 — Preview the deployment plan

Before making any changes to your workspace, preview what the deployment would create or update:

```bash
databricks bundle plan
```

Review the output. You should see that the `order-pipeline-job` would be **created** (since it doesn't exist yet). For a non-default target, specify it explicitly:

```bash
databricks bundle plan -t dev
```

### Step 3 — Deploy the bundle

Deploy the bundle to the `dev` target:

```bash
databricks bundle deploy -t dev
```

During deployment, the CLI:
- Uploads your notebook files to the workspace
- Creates the `order-pipeline-job` in the **Jobs & Pipelines** section of your workspace (prefixed with `[dev <username>]` because development mode is active)

### Step 4 — Verify the deployed resources

Confirm the deployment succeeded:

```bash
databricks bundle summary
```

The output includes direct URLs to the created job. Open the URL in your browser to verify that the job appears in your workspace with both tasks (`validate-data` and `transform-data`) correctly configured.

> 🤖 **Tip:** Ask *"What does Databricks Asset Bundle development mode do to job names and schedules?"* to understand why the job is prefixed with your username.

### Step 5 — Clean up (optional)

To remove the deployed resources from your workspace:

```bash
databricks bundle destroy -t dev
```

Confirm when prompted to delete the job that was created.

---

## Summary

In this lab you:

- Implemented **unit tests** using pytest fixtures to verify individual transformation functions
- Configured a **Databricks Asset Bundle** with variables, job resources, and multi-environment targets
- Used the **Databricks CLI** to validate, plan, deploy, and verify a bundle deployment
