# Databricks notebook source
# MAGIC %run
# MAGIC ../setup/mount_adls_storage

# COMMAND ----------

# get data for 2021
races = spark.read.parquet(f'{processed_folder_path}/races').withColumnRenamed('name', 'race_name')\
    .withColumnRenamed('date', 'race_date')
circuts = spark.read.parquet(f'{processed_folder_path}/circuits').withColumnRenamed('location', 'circuit_location')
drivers = spark.read.parquet(f'{processed_folder_path}/drivers').withColumnRenamed('name', 'driver_name')\
    .withColumnRenamed('number', 'driver_number')\
    .withColumnRenamed('nationality', 'driver_nationality')
team = spark.read.parquet(f'{processed_folder_path}/constructors').withColumnRenamed('name', 'constructors_name')
results = spark.read.parquet(f'{processed_folder_path}/results')

# COMMAND ----------

# join tables by schema
circuits_races = circuts.join(races, (circuts.circuit_id == races.circuit_id)).select('race_year', 'race_name', 'race_date', 'circuit_location', 'race_id')

driver_results = drivers.join(results, (drivers.driver_id == results.driver_id)).select('driver_name', 'driver_number', 'driver_nationality', 'race_id')

team_results = team.join(results, (team.constructor_id == results.constructor_id)).select('constructors_name', 'grid', 'fastest_lap', 'time', 'points', 'race_id')


# COMMAND ----------

display(circuits_races)

# COMMAND ----------

display(driver_results)

# COMMAND ----------

display(team_results)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

df1 = driver_results.join(circuits_races, (circuits_races.race_id == driver_results.race_id)).drop(circuits_races.race_id)
race_results = df1.join(team_results, (df1.race_id == team_results.race_id)).drop(df1.race_id).drop(team_results.race_id).withColumn("created_date", current_timestamp()).drop('race_id')
display(race_results)

# COMMAND ----------

race_results.createOrReplaceTempView ('race_results')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from race_results where race_year = 2020

# COMMAND ----------

race_results.write.mode("overwrite").parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------


