# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

df = spark.read.format("csv")\
            .option("inferSchema",True)\
            .option("header",True)\
            .load("/Volumes/workspace/default/rebuild_volume1/BigMart Sales.csv")\
            #.cache()

# COMMAND ----------

df2 = df.filter(col("Outlet_Location_Type") == 'Tier 1')

# COMMAND ----------

df3 = df.filter(col("Outlet_Location_Type") == 'Tier 2')

# COMMAND ----------

df3.display()

# COMMAND ----------

df.unpersist()

# COMMAND ----------

from pyspark.storagelevel import StorageLevel

# COMMAND ----------

df.persist(StorageLevel.MEMORY_ONLY)

# COMMAND ----------


