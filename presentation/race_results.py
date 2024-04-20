# Databricks notebook source
# MAGIC %run
# MAGIC ../setup/mount_adls_storage

# COMMAND ----------

races = spark.read.parquet(f'{processed_folder_path}/races').withColumnRenamed('name', 'race_name')\
    .withColumnRenamed('date', 'race_date')
circuts = spark.read.parquet(f'{processed_folder_path}/circuits').withColumnRenamed('location', 'circuit_location')
drivers = spark.read.parquet(f'{processed_folder_path}/drivers').withColumnRenamed('name', 'driver_name')\
    .withColumnRenamed('number', 'driver_number')\
    .withColumnRenamed('nationality', 'driver_nationality')
constructors = spark.read.parquet(f'{processed_folder_path}/constructors').withColumnRenamed('name', 'team')
results = spark.read.parquet(f'{processed_folder_path}/results').withColumnRenamed('time', 'race_time')

# COMMAND ----------

# MAGIC %md
# MAGIC Join the dataframes

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

race_circuits_df = races.join(circuts, races.circuit_id == circuts.circuit_id)\
    .select(races.race_id, races.race_year, races.race_name, races.race_date, circuts.circuit_location)

# COMMAND ----------

race_results_df = results.join(race_circuits_df, results.race_id == race_circuits_df.race_id)\
    .join(drivers, results.driver_id == drivers.driver_id)\
    .join(constructors, results.constructor_id == constructors.constructor_id)
        

# COMMAND ----------

race_results_df = race_results_df.select("race_year", "race_name", "race_date", "circuit_location", "driver_name", "driver_nationality", "team", "grid", "fastest_lap", "race_time", "points").withColumn("created_date", current_timestamp())

# COMMAND ----------

race_results_df.createOrReplaceTempView ('race_results')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from race_results

# COMMAND ----------

race_results_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------


