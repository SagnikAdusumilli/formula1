# Databricks notebook source
# MAGIC %run
# MAGIC ../setup/mount_adls_storage

# COMMAND ----------

race_results = spark.read.parquet(f"{presentation_folder_path}/race_results")


# COMMAND ----------

demo_df = race_results.filter('race_year = 2020')

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

demo_df.select(count('*')).show()

# COMMAND ----------

demo_df.select(count_distinct("race_name")).show()

# COMMAND ----------

demo_df.select(sum("points")).show()

# COMMAND ----------

demo_df.filter("driver_name = 'Lewis Hamilton' ").select(sum("points"), countDistinct("race_name")).show()

# COMMAND ----------

demo_df.createOrReplaceTempView('demo_view')

# COMMAND ----------

demo_df.groupBy("driver_name").sum('points').show()

# COMMAND ----------

demo_df.groupBy("driver_name").agg(sum("points"), countDistinct("race_name")).show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select driver_name, sum(points), count(distinct(race_name)) from demo_view
# MAGIC group by driver_name

# COMMAND ----------

# MAGIC %md 
# MAGIC ### window functions
# MAGIC
# MAGIC window specification is parition the data. Window function is the function applied on paritions \
# MAGIC in this case we're order by points for each year 

# COMMAND ----------

demo_df = race_results.filter("race_year in (2019,2020)")

# COMMAND ----------

grouped_df = demo_df.groupBy("race_year", "driver_name").agg(sum("points").alias('total_points'), countDistinct('race_name').alias("number_of_races"))

# COMMAND ----------

display(grouped_df)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import *
# parition data by race year
# order by desc
# apply rank function
driverRankSpec = Window.partitionBy("race_year").orderBy(desc("total_points"))
grouped_df.withColumn("rank", rank().over(driverRankSpec)).show(100)
