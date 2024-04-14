# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

name_schema = StructType(fields=[StructField("forename", StringType(), True), StructField("surname", StringType(), True)])
drivers_schema = StructType(fields=[
    StructField("driverId", IntegerType(), False),
    StructField("driverRef", StringType(), True),
    StructField("number", IntegerType(), True),
    StructField("code", StringType(), True),
    StructField("name", name_schema),
    StructField('dob', DateType(), True),
    StructField("nationality", StringType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

df_raw = spark.read.json('/mnt/mydbstorage/raw/drivers.json', schema=drivers_schema)

# COMMAND ----------

display(df_raw)

# COMMAND ----------

from pyspark.sql.functions import col, concat, current_timestamp, lit

# COMMAND ----------

drives_with_columns_df = df_raw.withColumnRenamed("driverId", "driver_id")\
                               .withColumnRenamed("driverRef", "driver_ref")\
                               .withColumn("ingestion_date", current_timestamp())\
                               .withColumn("name", concat(col('name.forename'), lit(' '), col('name.surname')))

# COMMAND ----------

display(drives_with_columns_df)

# COMMAND ----------

final_df = drives_with_columns_df.drop(col("url"))

# COMMAND ----------

final_df.write.parquet("/mnt/mydbstorage/processed/drivers", mode="overwrite")

# COMMAND ----------


