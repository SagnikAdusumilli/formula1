# Databricks notebook source
from pyspark.sql.types import *

# COMMAND ----------

laptimes_schema = StructType(fields=[
    StructField('raceId', IntegerType(), False),
    StructField('driverId', IntegerType(), True),
    StructField('lap', IntegerType(), True),
    StructField('position', IntegerType(), True),
    StructField('time', StringType(), True),
    StructField('milliseconds', IntegerType(), True)
])

# COMMAND ----------

laptimes_df = spark.read.schema(laptimes_schema)\
                        .csv('/mnt/mydbstorage/raw/lap_times/lap_times_split*.csv')
# could also use wild cards to specify only cerntain files 

# COMMAND ----------

display(laptimes_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = laptimes_df.withColumnRenamed("driverId", "driver_id") \
                      .withColumnRenamed("raceId", "race_id") \
                      .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

final_df.write.parquet('/mnt/mydbstorage/processed/lap_times', mode='overwrite')

# COMMAND ----------


