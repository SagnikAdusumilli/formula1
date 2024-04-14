# Databricks notebook source
from pyspark.sql.types import *

# COMMAND ----------

qualify_schema = StructType(fields=[
    StructField('qualifyId', IntegerType(), False),
    StructField('raceId', IntegerType(), True),
    StructField('driverId', IntegerType(), True),
    StructField('constructorId', IntegerType(), True),
    StructField('number', IntegerType(), True),
    StructField('position', IntegerType(), True),
    StructField('q1', StringType(), True),
    StructField('q2', StringType(), True),
    StructField('q3', StringType(), True)
])

# COMMAND ----------

qualify_df = spark.read \
.option('multiLine', True)\
.json('/mnt/mydbstorage/raw/qualifying/*.json', schema=qualify_schema)
# could also use wild cards to specify only cerntain files 

# COMMAND ----------

display(qualify_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = qualify_df.withColumnRenamed("qualifyId", "qualify_id") \
                      .withColumnRenamed("driverId", "driver_id") \
                      .withColumnRenamed("raceId", "race_id") \
                      .withColumnRenamed("constructorId", "constructor_id") \
                      .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

final_df.write.parquet('/mnt/mydbstorage/processed/qualify', mode='overwrite')

# COMMAND ----------

display(spark.read.parquet('/mnt/mydbstorage/processed/qualify'))

# COMMAND ----------


