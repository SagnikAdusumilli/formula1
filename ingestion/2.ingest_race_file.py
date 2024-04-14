# Databricks notebook source
# MAGIC %md
# MAGIC ### Create schema and read file

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
# cannot import DayTimeInterValType. That is only supported by pyspark 3.3 and above

# COMMAND ----------

race_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False),
    StructField("year", IntegerType(), True),
    StructField("round", IntegerType(), True),
    StructField("circuitId", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("date", DateType(), True),
    StructField("time", StringType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

df_raw = spark.read.schema(race_schema).csv("/mnt/mydbstorage/raw/races.csv", header=True)

# COMMAND ----------

display(df_raw)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filtering and transformation

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp, lit, concat, current_timestamp

# COMMAND ----------

selected_df = df_raw.select(col("raceId").alias("race_id"), col("year").alias("race_year"), col("round"),
                             col("circuitId").alias("circuit_id"), col("name"), col("date"), col("time"))

# COMMAND ----------

selected_df = selected_df.withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(" "), col('time')), 'yyyy-MM-dd HH:mm:ss'))\
.select("*")

# COMMAND ----------

final_df = selected_df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

display(final_df)

# COMMAND ----------

final_df.write.parquet("/mnt/mydbstorage/processed/races", partitionBy='race_year', mode='overwrite')

# COMMAND ----------

# MAGIC %md 
# MAGIC Verify

# COMMAND ----------

df = spark.read.parquet("/mnt/mydbstorage/processed/races")

# COMMAND ----------

display(df)

# COMMAND ----------


