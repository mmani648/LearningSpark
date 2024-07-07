# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

FileSchema  = StructType([StructField('CallNumber', IntegerType(), True),
                     StructField('UnitID', StringType(), True),
                     StructField('IncidentNumber', IntegerType(), True),
                     StructField('CallType', StringType(), True),                  
                     StructField('CallDate', StringType(), True),      
                     StructField('WatchDate', StringType(), True),
                     StructField('CallFinalDisposition', StringType(), True),
                     StructField('AvailableDtTm', StringType(), True),
                     StructField('Address', StringType(), True),       
                     StructField('City', StringType(), True),       
                     StructField('Zipcode', IntegerType(), True),       
                     StructField('Battalion', StringType(), True),                 
                     StructField('StationArea', StringType(), True),       
                     StructField('Box', StringType(), True),       
                     StructField('OriginalPriority', StringType(), True),       
                     StructField('Priority', StringType(), True),       
                     StructField('FinalPriority', IntegerType(), True),       
                     StructField('ALSUnit', BooleanType(), True),       
                     StructField('CallTypeGroup', StringType(), True),
                     StructField('NumAlarms', IntegerType(), True),
                     StructField('UnitType', StringType(), True),
                     StructField('UnitSequenceInCallDispatch', IntegerType(), True),
                     StructField('FirePreventionDistrict', StringType(), True),
                     StructField('SupervisorDistrict', StringType(), True),
                     StructField('Neighborhood', StringType(), True),
                     StructField('Location', StringType(), True),
                     StructField('RowID', StringType(), True),
                     StructField('Delay', FloatType(), True)])


# COMMAND ----------

spark = SparkSession.builder.appName('fire').getOrCreate()

# COMMAND ----------

df = spark.read.csv("dbfs:/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv",header=True,schema=FileSchema)

# COMMAND ----------

df.select("IncidentNumber","CallType","AvailableDtTm","Address").where(col('CallType')=="Medical Incident").show(10)

# COMMAND ----------

# types of CallType
df.select("CallType").where(col("CallType").isNotNull()).agg(countDistinct("CallType")).alias("UniqueCallTypes").show()

# COMMAND ----------

Calltypes =df.select("CallType").where(col("CallType").isNotNull()).distinct().collect()

# COMMAND ----------

Calltypes =list(map(lambda x:x[0],Calltypes))
Calltypes

# COMMAND ----------

# Renamed Coulmns
df2  = df.withColumnRenamed("Delay","ResponseDelayedMins").select("ResponseDelayedMins").where(col("ResponseDelayedMins")>5)
df2.show()

# COMMAND ----------

df.select("CallDate").show() # format mm/dd/yyyy
df.select("WatchDate").show()# format mm/dd/yyyy
df.select("AvailableDtTm").show() #formate mm/dd/yyy hh:mm:ss

# COMMAND ----------

# conveting to timestamp object
new_df = df.withColumn("IncidentDate",to_timestamp(col("CallDate"),"MM/dd/yyyy")).drop("CallDate").withColumn("OnWatchDate",to_timestamp(col("WatchDate"),"MM/dd/yyyy")).drop("WatchDate").withColumn("AvailableTime",to_timestamp(col("AvailableDtTm"),"MM/dd/yyyy hh:mm:ss a")).drop("AvailableDtTm")

# COMMAND ----------

# veiw data after converting
new_df.select("IncidentDate","OnWatchDate","AvailableTime").show()

# COMMAND ----------

# using date time filters from sql.functions
new_df.select(year("IncidentDate")).distinct().orderBy(year("IncidentDate"))
# got the most calltype source
new_df.select("CallType").where((col("CallType").isNotNull())).groupBy("CallType").count().orderBy("count",ascending=False).show()


# COMMAND ----------

# what were all the different types of fire calls in 2018
new_df.select("CallType",year("IncidentDate").alias("Year")).where(col("Calltype").isNotNull()).where(year("IncidentDate")==2018).groupBy("CallType","Year").count().orderBy("count",ascending=False).show()

# COMMAND ----------

#what month within year of 2018 saw the hihest number of fire calls
new_df.select("IncidentNumber",month("IncidentDate").alias("Month"),year("IncidentDate").alias("year")).where(year("IncidentDate")==2018).groupBy("Month").count().orderBy("count",ascending=False).show()

# COMMAND ----------

# which neighbouthood generated most of the calls in 2018
new_df.select("Neighborhood",year("IncidentDate").alias("Year")).where(year("IncidentDate")==2018).groupBy("Neighborhood","Year").count().orderBy("count",ascending=False).show()

# COMMAND ----------

# which neighbour hood called alot
new_df.select("Neighborhood","Delay",year("IncidentDate").alias("Year")).where(col("Delay")>40).where(year("IncidentDate")==2018).orderBy("Delay",ascending=False).show()

# COMMAND ----------

#most calls in week
new_df.select("IncidentNumber",weekofyear("IncidentDate").alias("Week")).where(year("IncidentDate")==2018).groupBy("Week").count().orderBy("count",ascending=False).show()

# COMMAND ----------

# trying to check correaltion between zipcode and number of incident happed
corr_df = new_df.select("Zipcode").groupBy("Zipcode").count().orderBy("count",ascending=False)
corrrealtion = corr_df.corr("Zipcode","count")
print(corrrealtion)
