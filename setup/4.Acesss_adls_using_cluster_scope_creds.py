# Databricks notebook source
# MAGIC %md
# MAGIC 1. set the spark config the cluster fs.azure.account.key.mydbstorage.dfs.core.windows.net as a kev value pair

# COMMAND ----------

dbutils.fs.ls("abfss://raw@mydbstorage.dfs.core.windows.net")

# COMMAND ----------


