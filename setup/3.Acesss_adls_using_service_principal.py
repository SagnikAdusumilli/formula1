# Databricks notebook source
# MAGIC %md
# MAGIC 1. Register the service principal (also known as AD application)
# MAGIC 2. Create secret for the application 
# MAGIC 3. Configure databricks to access storage account via service principal
# MAGIC 4. Assign ROle Stroage Blob Contributor to the Data Lake

# COMMAND ----------

client_id = dbutils.secrets.get(scope='formula1-scope', key="serviceappclientid")
tenant_id = dbutils.secrets.get(scope='formula1-scope', key="servicetenantid")
client_secret = dbutils.secrets.get(scope='formula1-scope', key="servicesecret")

# COMMAND ----------

# documentation can be found in https://learn.microsoft.com/en-us/azure/databricks/connect/storage/azure-storage

spark.conf.set("fs.azure.account.auth.type.mydbstorage.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.mydbstorage.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.mydbstorage.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.mydbstorage.dfs.core.windows.net",  client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.mydbstorage.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

dbutils.fs.ls("abfss://raw@mydbstorage.dfs.core.windows.net")

# COMMAND ----------


