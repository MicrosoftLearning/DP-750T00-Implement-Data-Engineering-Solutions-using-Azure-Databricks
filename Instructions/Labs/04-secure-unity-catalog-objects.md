---
lab:
  index: 04
  title: Secure Unity Catalog objects
  module: Secure Unity Catalog objects
  module-url: https://learn.microsoft.com/training/wwl-databricks/secure-unity-catalog-objects/
  notebook: https://github.com/MicrosoftLearning/DP-750T00-Implement-Data-Engineering-Solutions-using-Azure-Databricks/blob/main/Allfiles/04-secure-unity-catalog-objects.ipynb
  description: In this lab, you secure Unity Catalog objects in Azure Databricks by granting fine-grained access control to a Databricks group, applying row filters to restrict customer data by region, and masking PII email addresses using column mask functions. You also create an Azure Key Vault-backed secret scope and retrieve secrets securely inside a notebook, so sensitive credentials are never exposed in code.
  duration: 45 minutes
  level: 300
  islab: true
  primarytopics:
    - Azure Databricks
---

# Lab 04: Secure Unity Catalog Objects

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

To open the Databricks Assistant, select the ![assistant-icon](https://raw.githubusercontent.com/MicrosoftLearning/DP-750T00-Implement-Data-Engineering-Solutions-using-Azure-Databricks/refs/heads/main/Allfiles/media/databricks-assistant.svg) on the right side of any notebook cell, or press the keyboard shortcut shown in the toolbar.

> 💡 **Tip:** Do not just copy and paste the Assistant's output blindly. Read it, understand it, and adapt it to the task requirements. The Assistant is a tool to accelerate your thinking, not replace it.

---

## Prerequisites

Before starting this lab, ensure you have:

- Access to an **Azure Databricks Premium workspace** (already provisioned for you).
- An active **Unity Catalog metastore** attached to the workspace.
- The **CREATE CATALOG** privilege on the metastore.
- An **Azure subscription** where you can create a Key Vault.
- Familiarity with basic SQL (CREATE TABLE, SELECT, GRANT).

---

## Import the lab notebook

1. In your Azure Databricks workspace, select **Workspace** in the left sidebar.
2. Navigate to or create a folder where you want to store this lab.
3. Select the **⋮** (kebab) menu next to the folder, then select **Import**.
4. Choose **URL**, enter the following URL, and select **Import**:
   `https://raw.githubusercontent.com/MicrosoftLearning/DP-750T00-Implement-Data-Engineering-Solutions-using-Azure-Databricks/refs/heads/main/Allfiles/04-secure-unity-catalog-objects.ipynb`
5. Open the imported notebook and, in the compute selector at the top, choose **Serverless** compute.

---

## Before you open the notebook: Create a Databricks group

You will grant permissions to a Databricks group in Exercise 1. Create the group now so it is ready when you reach that exercise.

1. In the Databricks workspace, select **your username** (top right) → **Settings**.
2. Navigate to **Identity and access** → **Groups** → select **Manage** → **Add group**.
3. Name the group `retail-analysts` and select **Save**.
4. Once the group is created, add your own user account as a member.

> ℹ️ This group represents the regional analyst team at NorthMart Retail. You will grant it access to the lab schema in Exercise 1.

---

## Before Exercise 4: Create an Azure Key Vault

Exercise 4 requires an Azure Key Vault with a pre-created secret. Complete these steps in the Azure Portal before reaching Exercise 4 in the notebook, or prepare them in parallel.

### Step 1: Create the Key Vault

1. Open the [Azure Portal](https://portal.azure.com) and select **Create a resource**.
2. Search for **Key Vault** and select **Create**.
3. Configure the Key Vault:
   - **Resource group**: use your lab resource group.
   - **Key vault name**: `kv-northmart-<your-initials>` (must be globally unique).
   - **Region**: the same region as your Databricks workspace.
   - **Pricing tier**: Standard.
4. On the **Access configuration** tab, set the **Permission model** to **Vault access policy**.
5. Select **Review + create**, then **Create**.

### Step 2: Add an access policy for your user

1. Once the Key Vault is deployed, open it in the portal.
2. Select **Access policies** → **Create**.
3. Under **Secret permissions**, select **Get** and **List**.
4. Under **Principal**, search for and select your own Azure user account.
5. Select **Create** to save the policy.

### Step 3: Add a secret

1. In the Key Vault, select **Secrets** → **Generate/Import**.
2. Set the following:
   - **Name**: `loyalty-api-key`
   - **Value**: `NORTHMART-LOYALTY-2026-SECURE`
3. Select **Create**.

### Step 4: Note down the Key Vault details

Before leaving the Key Vault, navigate to **Properties** and copy:
- **Vault URI** (DNS Name), for example: *https://kv-northmart-abc.vault.azure.net/*
- **Resource ID**, for example: */subscriptions/xxxxxxxx/resourceGroups/rg-lab/providers/Microsoft.KeyVault/vaults/kv-northmart-abc*

You will need both values when creating the Databricks secret scope in Exercise 4.

### Step 5: Create a Databricks secret scope

1. In your browser, navigate to:

    ```
    https://<your-databricks-workspace-url>#secrets/createScope
    ```

    > ⚠️ The **S** in **createScope** must be uppercase. Replace `<your-databricks-workspace-url>` with your actual workspace URL (omit any trailing '/').

2. Configure the scope:
   - **Scope Name**: `retail-kv-scope`
   - **Manage Principal**: `All workspace users`
   - **DNS Name**: paste the Vault URI from Step 4.
   - **Resource ID**: paste the Resource ID from Step 4.
3. Select **Create**.

> ✅ **Expected result:** You should see a confirmation message that the scope was created. The scope is now linked to your Azure Key Vault, and any secrets you add there are accessible from Databricks notebooks.

---

You are now ready to open the notebook and complete the exercises.
