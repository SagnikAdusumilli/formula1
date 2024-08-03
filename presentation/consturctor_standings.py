# Databricks notebook source
# MAGIC %run
# MAGIC ../setup/mount_adls_storage

# COMMAND ----------

# Team, wins and points

# COMMAND ----------

constructors_standing_df = spark.read.parquet(f"{presentation_folder_path}/driver_standings").select("team", "wins", "total_points", "race_year")

# COMMAND ----------

from pyspark.sql.functions import *
constructors_standing_df = constructors_standing_df.groupBy("team", "race_year").agg(sum("wins").alias("wins"), sum("total_points").alias("points"))

# COMMAND ----------

from pyspark.sql.window import Window
constructors_spec = Window.partitionBy("race_year").orderBy(desc("points"), desc("wins"))

constructors_standing_df = constructors_standing_df.withColumn("rank", rank().over(constructors_spec))

# COMMAND ----------

display(constructors_standing_df.filter("race_year = 2020"))

# COMMAND ----------

constructors_standing_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/constructor_standings")

# COMMAND ----------


