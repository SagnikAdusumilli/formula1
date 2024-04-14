# Databricks notebook source
# using DDL formatted string to define the schema
constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

df_raw = spark.read.schema(constructors_schema).json('/mnt/mydbstorage/raw/constructors.json')

# COMMAND ----------

display(df_raw)

# COMMAND ----------

constructor_dropped_df = df_raw.drop('url')

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

constructor_final_df = constructor_dropped_df.withColumnRenamed("constructorId", "constructor_id")\
.withColumnRenamed("constructorRef", "constructor_ref")\
.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

display(constructor_final_df)

# COMMAND ----------

constructor_final_df.write.parquet("/mnt/mydbstorage/processed/constructors", mode="overwrite")

# COMMAND ----------


