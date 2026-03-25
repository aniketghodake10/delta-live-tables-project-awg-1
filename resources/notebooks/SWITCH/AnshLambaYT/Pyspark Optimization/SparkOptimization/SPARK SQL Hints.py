# Databricks notebook source
from pyspark.sql.functions import *
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

df_transactions.createOrReplaceTempView("transactions")
df_countries.createOrReplaceTempView("countries")

# COMMAND ----------

df_sql_opt = spark.sql(
    '''SELECT /* broadcast(c) */ 
        * 
FROM transactions t 
JOIN countries c 
ON t.country_code = c.country_code''')

# COMMAND ----------

df_sql_opt.display()

# COMMAND ----------


