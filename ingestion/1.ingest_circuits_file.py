# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest circuits.csv file

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Read csv files using spark dataframe reader

# COMMAND ----------

# circuits_df = spark.read.csv("/mnt/mydbstorage/raw/circuits.csv", header=True, inferSchema=True)
# Note the above option inferSchema creates another spark job to read through the data and infer schema. This is not efficient, and in production env, you don't want to do that due to the large volume of data and also you want to process to fail in case of bad records 

# COMMAND ----------

# The production apprach is to identify the schema and use spark to check if data conforms to the identified schema 
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

# The 3rd parameter specifies if the field is nullable
circuits_schema = StructType(fields=[
    StructField("circuitId", IntegerType(), False),
    StructField("circuitRef", StringType(), True),
    StructField("name", StringType(), True),
    StructField("location", StringType(), True),
    StructField("country", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lng", DoubleType(), True),
    StructField("alt", IntegerType(), True),
    StructField("url", StringType(), True),  
])

# COMMAND ----------

circuits_df = spark.read.schema(circuits_schema).csv(f"{raw_folder_path}/circuits.csv", header=True)
# note put the .schema before .csv

# COMMAND ----------

# MAGIC %md
# MAGIC ### Selecting and renaming the columns

# COMMAND ----------

circuits_selected_df = circuits_df.select("circuitId", "circuitRef", "name", "location", "country", "lat", "lng", "alt")
# another method: circits_df.select(circuits_df.circuitId,...)
# 3rd method is put circuits_df[colname]
# last method 
# from pyspark.sql.functions import col
# circuits_df.select(col("circuitId")...)
# the other 3 methods allow you to apply column functions while doing select 

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id") \
.withColumnRenamed("circuitRef", "circuit_ref") \
.withColumnRenamed("lat", "latitude") \
.withColumnRenamed("lng", "longitude") \
.withColumnRenamed("alt", "altitude")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Adding Ingestion Date column

# COMMAND ----------

circuits_final_df = add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Write the data

# COMMAND ----------

circuits_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/circuits")

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------


