# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC **CREATING A DATAFRAME**

# COMMAND ----------

data = [("A", 100), ("A", 200), ("A", 300), ("B", 400), ("C", 500)]
df = spark.createDataFrame(data, ["user_id", "purchase"])

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **ADDING SALT COLUMN**

# COMMAND ----------

df = df.withColumn("salt_column",floor(rand()*3)) # Salt of 3 [0,1,2]

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Creating Concat Column on original groupBy col and salt_column to create a new groupBy col**

# COMMAND ----------

df = df.withColumn("user_id_salt",concat(col("user_id"),lit("-"),col("salt_column")))

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Applying Group By on this new col**

# COMMAND ----------

df = df.groupBy("user_id_salt").agg(sum("purchase"))

df.display()

# COMMAND ----------


