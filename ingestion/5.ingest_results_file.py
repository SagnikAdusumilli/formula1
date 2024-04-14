# Databricks notebook source
from pyspark.sql.types import *

# COMMAND ----------

results_schema = StructType(fields=[StructField("resultId", IntegerType(), False),
    StructField("raceId", IntegerType(), True),
    StructField("driverId", IntegerType(), True),
    StructField("constructorId", IntegerType(), True),
    StructField("number", IntegerType(), True),
    StructField("grid", IntegerType(), True),
    StructField("position", StringType(), True),
    StructField("positionOrder", IntegerType(), True),
    StructField("points", IntegerType(), True),
    StructField("laps", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("milliseconds", IntegerType(), True),
    StructField("fastestLap", IntegerType(), True),
    StructField("rank", IntegerType(), True),
    StructField("fastestLapTime", StringType(), True),
    StructField("fastestLapSpeed", FloatType(), True),
    StructField("statusId", StringType(), True)
])

# COMMAND ----------

results_df = spark.read.json("/mnt/mydbstorage/raw/results.json", schema=results_schema)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

results_with_cols_df = results_df.withColumnRenamed("resultId", "result_id") \
                                 .withColumnRenamed("raceId", "race_id") \
                                 .withColumnRenamed("driverId", "driver_id") \
                                 .withColumnRenamed("constructorId", "constructor_id") \
                                 .withColumnRenamed("positionText", "position_text") \
                                 .withColumnRenamed("positionOrder", "position_order") \
                                 .withColumnRenamed("fastestLap", "fastest_lap") \
                                 .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
                                 .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
                                 .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

results_final_df = results_with_cols_df.drop(col("statusId"))

# COMMAND ----------

results_final_df.write.parquet("/mnt/mydbstorage/processed/results", mode="overwrite", partitionBy="race_id")

# COMMAND ----------


