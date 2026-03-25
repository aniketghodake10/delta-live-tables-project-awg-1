# Databricks notebook source
# MAGIC %md
# MAGIC **Turning OFF AQE and DPP and AutoBroadcast**

# COMMAND ----------

spark.conf.set("spark.sql.adaptive.enabled","true")
spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")


# COMMAND ----------

df = spark.read.format("csv")\
        .option("header",True)\
        .option("inferSchema",True)\
        .load("/FileStore/rawdata/BigMart_Sales.csv")

df = df.limit(8)

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Preparing the Partitioned Data**

# COMMAND ----------

df.write.format("parquet")\
        .mode("append")\
        .partitionBy("Item_identifier")\
        .option("path","/FileStore/rawdata/dpp_partionednew")\
        .save()

# COMMAND ----------

# MAGIC %md
# MAGIC **Non Partitioned Data**

# COMMAND ----------

df.write.format("parquet")\
        .mode("append")\
        .option("path","/FileStore/rawdata/dpp_nonpartioned")\
        .save()

# COMMAND ----------

# MAGIC %md
# MAGIC **Dataframes**

# COMMAND ----------

df1 = spark.read.format("parquet")\
          .load("/FileStore/rawdata/dpp_partionednew")

# COMMAND ----------

df2 = spark.read.format("parquet")\
          .load("/FileStore/rawdata/dpp_nonpartioned")

# COMMAND ----------

# MAGIC %md
# MAGIC **JOINS**

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df_join = df1.join(df2.filter(col("Item_Identifier")=="FDA15"),df1['Item_Identifier']==df2['Item_Identifier'],"inner")

# COMMAND ----------

df_join.display()

# COMMAND ----------

df_join.explain()

# COMMAND ----------


