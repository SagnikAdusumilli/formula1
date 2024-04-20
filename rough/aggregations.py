# Databricks notebook source
# MAGIC %run
# MAGIC ../setup/mount_adls_storage

# COMMAND ----------

race_results = spark.read.parquet(f"{presentation_folder_path}/race_results")


# COMMAND ----------

demo_df = race_results.filter('race_year = 2020')

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

demo_df.select(count('*')).show()

# COMMAND ----------

demo_df.select(count_distinct("race_name")).show()

# COMMAND ----------

demo_df.select(sum("points")).show()

# COMMAND ----------

demo_df.filter("driver_name = 'Lewis Hamilton' ").select(sum("points"), countDistinct("race_name")).show()

# COMMAND ----------

demo_df.createOrReplaceTempView('demo_view')

# COMMAND ----------


