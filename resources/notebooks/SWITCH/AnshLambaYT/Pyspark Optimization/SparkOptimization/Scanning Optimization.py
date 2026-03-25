# Databricks notebook source
# MAGIC %md
# MAGIC **TURN OFF AQE**

# COMMAND ----------

spark

# COMMAND ----------

# MAGIC %sql
# MAGIC --spark.conf.set("spark.sql.adaptive.enabled", "false")
# MAGIC SET spark.sql.adaptive.enabled = true;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Checking AQE Status
# MAGIC --spark.conf.get("spark.sql.adaptive.enabled")
# MAGIC SET spark.sql.adaptive.enabled;

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC **Data Reading**

# COMMAND ----------

df = spark.read.format("csv")\
        .option("inferSchema",True)\
        .option("header",True)\
        .load("/Volumes/workspace/default/rebuild_volume1/BigMart Sales.csv")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Get No. of Partitions**

# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Changing DEFAULT Partition Size to 128KB**

# COMMAND ----------

# Changing the default partition size to 128KB 

spark.conf.set("spark.sql.files.maxPartitionBytes", 131072)

# COMMAND ----------

#df.rdd.getNumPartitions()
print(df.inputFiles())

# COMMAND ----------

# MAGIC %md
# MAGIC **Changing the default partition size to 128MB**

# COMMAND ----------

spark.conf.set("spark.sql.files.maxPartitionBytes", 134217728)

# COMMAND ----------

# MAGIC %md
# MAGIC **Repartitioning**

# COMMAND ----------

df = df.repartition(10)

# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------

# MAGIC %md
# MAGIC **Get Partition Info**

# COMMAND ----------

# Function to get the partition id

df = df.withColumn("partition_id",spark_partition_id())

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Data Writing**

# COMMAND ----------

df.write.format("parquet")\
    .mode("append")\
    .option("path","/Volumes/workspace/default/rebuild_volume1/parquetWrite")\
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC **New Data Reading**

# COMMAND ----------

df_new = spark.read.format("parquet")\
              .load("/Volumes/workspace/default/rebuild_volume1/parquetWrite")

df_new = df_new.filter(col("Outlet_Location_Type") == 'Tier 1')

# COMMAND ----------

df_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **SCANNING OPTIMIZATION**

# COMMAND ----------

df.write.format("parquet")\
      .mode("append")\
      .partitionBy("Outlet_Location_Type")\
      .option("path","/Volumes/workspace/default/rebuild_volume1/parquetWriteOpt")\
      .save()

# COMMAND ----------

df_new = spark.read.format("parquet")\
              .load("/Volumes/workspace/default/rebuild_volume1/parquetWriteOpt")

df_new = df_new.filter(col("Outlet_Location_Type") == 'Tier 1')

df_new.display()

# COMMAND ----------


