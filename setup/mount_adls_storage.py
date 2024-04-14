# Databricks notebook source
# dbutils.secrets.list('formula1-scope')

# COMMAND ----------

storage_account_name = "mydbstorage"
client_id = dbutils.secrets.get(scope='formula1-scope', key="serviceappclientid")
tenant_id = dbutils.secrets.get(scope='formula1-scope', key="servicetenantid")
client_secret = dbutils.secrets.get(scope='formula1-scope', key="servicesecret")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth", 
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": f"{client_id}",
          "fs.azure.account.oauth2.client.secret": f"{client_secret}",
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}


# COMMAND ----------

# function to mount container
def mount_adls(container_name):
    if  any(m.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for m in dbutils.fs.mounts()):
        return 
    dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage_account_name}/{container_name}",
    extra_configs = configs)

# COMMAND ----------

# mount containers
mount_adls("raw")
mount_adls("processed")
mount_adls("presentation")

# COMMAND ----------

# set folder paths
raw_folder_path = '/mnt/mydbstorage/raw'
processed_folder_path = '/mnt/mydbstorage/processed'
presentation_folder_path = '/mnt/mydbstorage/presentation'

dbutils.notebook.exit(f"folders created:\n raw_folder_path: {raw_folder_path}\n processed_folder_path: {processed_folder_path}\n presentation_folder_path: {presentation_folder_path}")

# COMMAND ----------


