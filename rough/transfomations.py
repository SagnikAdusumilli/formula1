# Databricks notebook source
# MAGIC %run
# MAGIC ../setup/mount_adls_storage

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races")

# COMMAND ----------

display(races_df)

# COMMAND ----------

#pypark sql-like api for python
# races_df.filter("race_year = 2021").show()
# pure python
races_df.filter(races_df["race_year"] == 2021).show()

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### JOINS

# COMMAND ----------

circuits_df = spark.read.parquet(f'{processed_folder_path}/circuits')\
    .withColumnRenamed('name', 'circuit_name')

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

races_2021 = races_df.filter('race_year = 2021').withColumnRenamed('name', 'race_name')
display(races_2021)

# COMMAND ----------

race_circuits_df = circuits_df.join(races_2021, circuits_df.circuit_id == races_2021.circuit_id)\
    .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_2021.race_name, races_2021.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------


