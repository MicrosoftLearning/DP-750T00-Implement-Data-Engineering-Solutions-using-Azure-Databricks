---
lab:
    title: 'Secure Unity Catalog Objects'
---

# Secure Unity Catalog Objects

## Scenario

You are a data engineer at **NorthMart Retail**, a national supermarket chain operating across four regions: North, South, East, and West. Your team manages a centralized data platform in Azure Databricks, containing customer data, loyalty programme records, and regional sales transactions.

The security team has raised several concerns:

- Regional analysts should only see data for their own region — not other regions' customer records.
- Customer email addresses are personally identifiable information (PII) and must be masked for most users.
- A third-party loyalty platform requires an API key to integrate — that key must never be stored in notebooks or code.

In this lab, you address all three concerns by implementing **access control**, **row filtering**, **column masking**, and **Azure Key Vault-backed secrets** in Azure Databricks Unity Catalog.

## Objectives

By the end of this lab, you will have:

- Granted and verified schema-level permissions to a Databricks group using SQL.
- Applied a row filter function to restrict customer records by region.
- Applied a column mask to protect PII email data.
- Created an Azure Key Vault and stored a secret.
- Created a Key Vault-backed secret scope in Azure Databricks.
- Retrieved a secret securely inside a notebook.

This lab should take approximately **35–40 minutes** to complete.

---

## 🤖 Use the Databricks Assistant throughout this lab

You are **expected and encouraged** to use the **Databricks Assistant** at all times during this lab. Every exercise cell in the notebook includes a suggested prompt you can paste directly into the Assistant panel.

To open the Databricks Assistant, click the **✨ sparkle icon** on the right side of any notebook cell, or press the keyboard shortcut shown in the toolbar.

> 💡 **Tip:** Do not just copy and paste the Assistant's output blindly. Read it, understand it, and adapt it to the task requirements. The Assistant is a tool to accelerate your thinking, not replace it.

---

## Prerequisites

Before starting this lab, ensure you have:

- Access to an **Azure Databricks Premium workspace** (already provisioned for you).
- An active **Unity Catalog metastore** attached to the workspace.
- The **CREATE CATALOG** privilege on the metastore.
- An **Azure subscription** where you can create a Key Vault.
- Familiarity with basic SQL (`CREATE TABLE`, `SELECT`, `GRANT`).

---

## Clone the lab repository

Before starting the exercises, clone the lab repository to your local machine.

1. Open a terminal and run:

    ```bash
    git clone https://github.com/MicrosoftLearning/DP-750T00-Implement-Data-Engineering-Solutions-using-Azure-Databricks.git DP-750
    ```

---

## Import the lab notebook

1. In your Azure Databricks workspace, click **Workspace** in the left sidebar.
2. Navigate to or create a folder where you want to store this lab.
3. Click the **⋮** (kebab) menu next to the folder, then select **Import**.
4. Choose **File**, browse to `DP-750/Allfiles/04-secure-unity-catalog-objects.ipynb`, and click **Import**.
5. Open the imported notebook and, in the compute selector at the top, choose **Serverless** compute.

---

## Before you open the notebook: Create a Databricks group

You will grant permissions to a Databricks group in Exercise 1. Create the group now so it is ready when you reach that exercise.

1. In the Databricks workspace, click **your username** (top right) → **Admin Settings**.
2. Navigate to **Groups** → **Add group**.
3. Name the group `retail-analysts` and click **Save**.
4. Once the group is created, add your own user account as a member.

> ℹ️ This group represents the regional analyst team at NorthMart Retail. You will grant it access to the lab schema in Exercise 1.

---

## Before Exercise 4: Create an Azure Key Vault

Exercise 4 requires an Azure Key Vault with a pre-created secret. Complete these steps in the Azure Portal before reaching Exercise 4 in the notebook, or prepare them in parallel.

### Step 1: Create the Key Vault

1. Open the [Azure Portal](https://portal.azure.com) and click **Create a resource**.
2. Search for **Key Vault** and click **Create**.
3. Configure the Key Vault:
   - **Resource group**: use your lab resource group.
   - **Key vault name**: `kv-northmart-<your-initials>` (must be globally unique).
   - **Region**: the same region as your Databricks workspace.
   - **Pricing tier**: Standard.
4. On the **Access configuration** tab, set the **Permission model** to **Vault access policy**.
5. Click **Review + create**, then **Create**.

### Step 2: Add an access policy for your user

1. Once the Key Vault is deployed, open it in the portal.
2. Click **Access policies** → **Create**.
3. Under **Secret permissions**, select **Get** and **List**.
4. Under **Principal**, search for and select your own Azure user account.
5. Click **Create** to save the policy.

### Step 3: Add a secret

1. In the Key Vault, click **Secrets** → **Generate/Import**.
2. Set the following:
   - **Name**: `loyalty-api-key`
   - **Value**: `NORTHMART-LOYALTY-2026-SECURE`
3. Click **Create**.

### Step 4: Note down the Key Vault details

Before leaving the Key Vault, navigate to **Properties** and copy:
- **Vault URI** (DNS Name), for example: `https://kv-northmart-abc.vault.azure.net/`
- **Resource ID**, for example: `/subscriptions/xxxxxxxx/resourceGroups/rg-lab/providers/Microsoft.KeyVault/vaults/kv-northmart-abc`

You will need both values when creating the Databricks secret scope in Exercise 4.

### Step 5: Create a Databricks secret scope

1. In your browser, navigate to:

    ```
    https://<your-databricks-workspace-url>#secrets/createScope
    ```

    > ⚠️ The `S` in `createScope` must be uppercase. Replace `<your-databricks-workspace-url>` with your actual workspace URL (omit any trailing `/`).

2. Configure the scope:
   - **Scope Name**: `retail-kv-scope`
   - **Manage Principal**: `All workspace users`
   - **DNS Name**: paste the Vault URI from Step 4.
   - **Resource ID**: paste the Resource ID from Step 4.
3. Click **Create**.

> ✅ **Expected result:** You should see a confirmation message that the scope was created. The scope is now linked to your Azure Key Vault, and any secrets you add there are accessible from Databricks notebooks.

---

You are now ready to open the notebook and complete the exercises.
