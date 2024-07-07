# Databricks notebook source
from pyspark.sql import SparkSession

# COMMAND ----------

data = [[1, "Jules", "Damji", "https://tinyurl.1", "1/4/2016", 4535, ["twitter","LinkedIn"]],
 [2, "Brooke","Wenig", "https://tinyurl.2", "5/5/2018", 8908, ["twitter","LinkedIn"]],
 [3, "Denny", "Lee", "https://tinyurl.3", "6/7/2019", 7659, ["web","twitter", "FB", "LinkedIn"]],
 [4, "Tathagata", "Das", "https://tinyurl.4", "5/12/2018", 10568,["twitter", "FB"]],
 [5, "Matei","Zaharia", "https://tinyurl.5", "5/14/2014", 40578, ["web","twitter", "FB", "LinkedIn"]],
 [6, "Reynold", "Xin", "https://tinyurl.6", "3/2/2015", 25568,["twitter", "LinkedIn"]]
 ]


# COMMAND ----------

schema ="`id` INT, `firstname` STRING, `lastname` STRING,`Url` STRING ,`Published` STRING,`HITS` INT, `campaigns` ARRAY<STRING>"

# COMMAND ----------

spark = SparkSession.builder.appName("mytest").getOrCreate()

# COMMAND ----------

df =spark.createDataFrame(data,schema)

# COMMAND ----------

df.show()

# COMMAND ----------

df.columns

# COMMAND ----------

from pyspark.sql.functions import *
with_big_hits =df.withColumn("BigHitters",expr("Hits>10000"))
with_big_hits.show()

# COMMAND ----------

df.withColumn("AuthorId",(concat(expr("firstname"),expr("lastname"),expr("id")))).select(col("AuthorId")).show()

# COMMAND ----------

df.withColumn("authorid",expr("concat(firstname,lastname,id)")).show()

# COMMAND ----------

df.orderBy(col("id").desc()).show()

# COMMAND ----------

# rows in spark are object/////////////////

# COMMAND ----------

from pyspark.sql import Row

# COMMAND ----------

# we can create data from Row
rows =[Row("mewo","cat"),Row("Bahooo","Dog")]

# COMMAND ----------

df_animal = spark.createDataFrame(rows,["sound","Animal"])
df_animal.show()

# COMMAND ----------


