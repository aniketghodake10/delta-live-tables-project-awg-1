# Databricks notebook source
# MAGIC %md
# MAGIC **Turning OFF the AQE**

# COMMAND ----------

spark.conf.set("spark.sql.adaptive.enabled", "true")

# COMMAND ----------

spark.conf.get("spark.sql.adaptive.enabled")

# COMMAND ----------

from pyspark.sql.functions import * 

# COMMAND ----------

df = spark.read.format("csv")\
            .option("inferSchema",True)\
            .option("header",True)\
            .load("/FileStore/rawdata/BigMart_Sales.csv")

# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------

display(df)

# COMMAND ----------

df_new = df.groupBy("Item_fat_Content").count()

df_new.display()

# COMMAND ----------

`
