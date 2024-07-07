# Databricks notebook source
import pyspark

# COMMAND ----------

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import count
import os

# COMMAND ----------

spark = SparkSession.builder.appName("PythonSession").getOrCreate()

# COMMAND ----------


currentPath  = os.getcwd()
curr = os.path.join(currentPath,"data.csv")
mm_df = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/mmani648@gmail.com/mnm_dataset-1.csv")

# COMMAND ----------

count_mnm_df = mm_df.select("State","Color","Count").groupBy("State","Color").agg(count("Count").alias("Total")).orderBy("Total",ascending=False)
