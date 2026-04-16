---
lab:
  index: 00
  title: Clean up your Azure Databricks environment
  module: Clean up your Azure Databricks environment
  description: Clean up your resources in Azure using Azure Cloud Shell.
  duration: 5 minutes
  level: 100
  islab: false
---

# Clean up your Azure Databricks environment

After completing all the labs in this course, you should delete the resources you created to avoid unnecessary Azure charges. This cleanup lab removes the resource group and all resources within it.

This cleanup should take approximately **5 minutes** to complete.

---

## Delete the resource group and all resources

Deleting the resource group removes the Azure Databricks workspace and any other resources that were created inside it during the labs.

### Task 1: Open Azure Cloud Shell

1. Sign in to the Azure portal at `https://portal.azure.com` using the credentials provided to you.

2. Select the **Cloud Shell** button (**>_**) in the toolbar at the top of the portal. If prompted, select **Bash** as the shell type.

3. Wait for the Cloud Shell prompt to appear.

### Task 2: Delete the resource group

1. In Cloud Shell, run the following command to download and execute the cleanup script:

    ```bash
    curl -sL https://raw.githubusercontent.com/MicrosoftLearning/DP-750T00-Implement-Data-Engineering-Solutions-using-Azure-Databricks/refs/heads/main/Instructions/Labs/99-cleanup.md | bash
    ```

2. The script deletes the resource group asynchronously. You can close Cloud Shell after running the command.

> [!NOTE]
> This permanently deletes the **rg-dp750** resource group along with the **adb-dp750** Azure Databricks workspace and any other resources you may have added to the group during the labs. This action cannot be undone.

