---
name: lab-testing
description: Test the lab exercise using playwright-cli and report on any issues encountered, along with suggestions for improving the clarity of the instructions and notebooks.
---

# Task: Open Azure Databricks

First, make sure to open azure databricks in headed mode using the playwright-cli:

```sh
playwright-cli open https://adb-7405611805451218.18.azuredatabricks.net --headed
```

any output from playwright-cli should be stored in this folder: `Testing/.playwright-cli`

# Task: Wait for the user to sign in

Wait for the user to sign in, and ask the user to say "go" before proceeding to the next task

# Task: Test the lab excercise

- Follow the instructions from this path, using the playwright-cli.

`Instructions/Labs/03-create-and-organize-objects-in-unity-catalog.md`

IMPORTANT: when you need to import the notebook, make sure to replace 

   `https://raw.githubusercontent.com/MicrosoftLearning/DP-750T00-Implement-Data-Engineering-Solutions-using-Azure-Databricks/refs/heads/main/Allfiles/03-create-and-organize-objects-in-unity-catalog.ipynb`

with 

  `https://raw.githubusercontent.com/MicrosoftLearning/DP-750T00-Implement-Data-Engineering-Solutions-using-Azure-Databricks/refs/heads/main/Allfiles/answers/03-create-and-organize-objects-in-unity-catalog-with-answers.ipynb`

This will allow you to test the lab exercise with the answers included in the notebook, which should make it easier to identify any issues with the instructions or the notebook itself.

Report on any issues you encounter - don't fix them yet. Also, come up with suggestions to improve the clarity from the instructions and/or notebooks.