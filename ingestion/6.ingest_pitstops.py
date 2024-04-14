# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

pit_stops_schema = StructType(fields=[
    StructField('raceId', IntegerType(), False),
    StructField('driverId', IntegerType(), True),
    StructField('stop', StringType(), True),
    StructField('lap', IntegerType(), True),
    StructField('time', StringType(), True),
    StructField('duration', StringType(), True),
    StructField('milliseconds', IntegerType(), True)
])

# COMMAND ----------

pit_stops_df = spark.read.option("multiLine", True).json( "/mnt/mydbstorage/raw/pit_stops.json", schema=pit_stops_schema)

# COMMAND ----------

display(pit_stops_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = pit_stops_df.withColumnRenamed('driverId', 'driver_id')\
                       .withColumnRenamed('raceId', 'race_id')\
                       .withColumn('ingestion_date', current_timestamp())

# COMMAND ----------

final_df.write.mode("overwrite").parquet('/mnt/mydbstorage/processed/pit_stops')

# COMMAND ----------


