# Databricks notebook source
# documentation can be found in https://learn.microsoft.com/en-us/azure/databricks/connect/storage/azure-storage
# generate new token in vault because this experies in 9 hours
spark.conf.set("fs.azure.account.auth.type.mydbstorage.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.mydbstorage.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.mydbstorage.dfs.core.windows.net", dbutils.secrets.get('formula1-scope', 'sastoken'))

# COMMAND ----------

dbutils.fs.ls("abfss://raw@mydbstorage.dfs.core.windows.net")

# COMMAND ----------


