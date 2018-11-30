# Self-Paced Spark Tutorial

This self-paced tutorial will walk you through the basics of [Apache Spark](https://spark.apache.org). Since we will be using PySpark (the Python API for Apache Spark) for Spark programming, students are expected to have a programming background and experience with Python. You may take [this](http://ai.berkeley.edu/tutorial.html#PythonBasics) Python mini-course if you want to refresh your Python knowledge.

## Getting Started

Throughout this tutorial, we will be using the Spark environment provided by [Databricks](https://databricks.com/). To get started, you need to [sign up](https://databricks.com/signup#signup/community) for an account and log into the [DataBricks Community Edition](https://community.cloud.databricks.com/).

## Introduction to Notebook

**Notebook** is a convenient interface to a document that may contain runnable code, visualization, narrative text, etc. Though displayed as a webpage, a notebook is, in essence, a script and can be imported and exported. As for how to import a notebook, you could refer to [this](https://docs.databricks.com/user-guide/notebooks/notebook-manage.html#import-a-notebook) webpage. Also there is [one](https://docs.databricks.com/user-guide/notebooks/notebook-manage.html#export-a-notebook) for exporting. 

Later assignments, including this tutorial, will primarily be in the form of notebooks.

## Launching Tutorial

First of all, you need to import `spark_tutorial.ipynb` in the repo as a notebook into your databricks workspace.

As a notebook requires an execution environment, you have to create one so as to actually run the code. Follow the instructions [here](https://docs.databricks.com/user-guide/clusters/create.html) to create a new cluster. During the creation, it is sufficient to just type in the cluster name and leave all the other configurations as default. After that, attach the notebook as mentioned [here](https://docs.databricks.com/user-guide/notebooks/notebook-manage.html#attach), and the tutorial is ready to run. Besides, you could start from [here](https://docs.databricks.com/user-guide/notebooks/notebook-use.html#run-notebooks) if unfamiliar with the usage of notebooks.

As a good practice, clear the resources after usage by [detaching](https://docs.databricks.com/user-guide/notebooks/notebook-manage.html#detach-a-notebook-from-a-cluster) the notebook and [deleting](https://docs.azuredatabricks.net/user-guide/notebooks/notebook-manage.html#attach-a-notebook-to-a-cluster) the cluster. The content in the notebook that you have saved will persist, but you will need to re-create another cluster to run it again.





