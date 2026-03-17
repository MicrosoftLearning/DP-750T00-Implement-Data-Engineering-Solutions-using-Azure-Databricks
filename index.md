---
title: Online Hosted Instructions
permalink: index.html
layout: home
---

This page lists exercises associated with DP-750 (*Implement data engineering solutions using Azure Databricks*) Microsoft skilling content on [Microsoft Learn](https://learn.microsoft.com/en-us/training/courses/dp-750t00)

> **Note**: If you encounter any bugs with the content, please [create a new issue in the GitHub repo](https://github.com/MicrosoftLearning/DP-750T00-Implement-Data-Engineering-Solutions-using-Azure-Databricks/issues/new).

{% assign labs = site.pages | where_exp:"page", "page.url contains '/Instructions/Labs'" %}
{% for activity in labs  %}
## Lab {{ activity.lab.index}}: [{{ activity.lab.title }}]({{ site.github.url }}{{ activity.url }})  
  
{{ activity.lab.description }}

- Duration: {{ activity.lab.duration }}
- To learn more, visit [this]({{ activity.lab.module-url }}) learn module.
- Supporting notebook can be viewed [here]({{ activity.lab.notebook }}).

{% endfor %}
