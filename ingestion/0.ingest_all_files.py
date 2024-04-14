# Databricks notebook source
dbutils.notebook.run("1.ingest_circuits_file", 0) # arguments can be passed in as a dictionary eg: {"p_data_src": "Ergast API"}

# COMMAND ----------

dbutils.notebook.run("2.ingest_race_file", 0)

# COMMAND ----------

dbutils.notebook.run("3.ingest_constructors_file", 0)

# COMMAND ----------

dbutils.notebook.run("4.ingest_drivers_file", 0)

# COMMAND ----------

dbutils.notebook.run("5.ingest_results_file", 0)

# COMMAND ----------

dbutils.notebook.run("6.ingest_pitstops_file", 0)

# COMMAND ----------

dbutils.notebook.run("7.ingest_laptimes_file", 0)

# COMMAND ----------

dbutils.notebook.run("8.ingest_qualifying_file", 0)
