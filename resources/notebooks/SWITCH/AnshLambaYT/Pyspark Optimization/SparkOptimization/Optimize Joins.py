# Databricks notebook source
from pyspark.sql.functions import *
#spark.conf.set("spark.sql.adaptive.enabled", "false")

# COMMAND ----------

#spark.conf.set("spark.sql.adaptive.enabled", "false")

# COMMAND ----------

# Big DataFrame
df_transactions = spark.createDataFrame([
    (1, "US", 100),
    (2, "IN", 200),
    (3, "UK", 150),
    (4, "US", 80),
], ["id", "country_code", "amount"])

# Small DataFrame
df_countries = spark.createDataFrame([
    ("US", "United States"),
    ("IN", "India"),
    ("UK", "United Kingdom"),
], ["country_code", "country_name"])

# COMMAND ----------

df_transactions.display()

# COMMAND ----------

df_countries.display()

# COMMAND ----------

df_join_opt = df_transactions.join(broadcast(df_countries),df_transactions['country_code']==df_countries['country_code'],"inner")

# COMMAND ----------

df_join_opt.display()

# COMMAND ----------


