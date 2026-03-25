# Databricks notebook source
# MAGIC %md
# MAGIC **Create the Dataframes**

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

transactions_data = [("A", 100), ("A", 200), ("A", 300), ("B", 150), ("C", 250)]
transactions_df = spark.createDataFrame(transactions_data, ["user_id", "amount"])

users_data = [("A", "India"), ("B", "USA"), ("C", "UK")]
users_df = spark.createDataFrame(users_data, ["user_id", "country"])


# COMMAND ----------

# MAGIC %md
# MAGIC **Salt the transaction_df**

# COMMAND ----------

from pyspark.sql.functions import *

salt_count = 3

# Add a random salt
transactions_salted = transactions_df.withColumn("salt", floor(rand() * salt_count))

# Create salted user_id
transactions_salted = transactions_salted.withColumn("salted_user_id", concat_ws("_", "user_id", "salt"))


# COMMAND ----------

# Displaying transactions_df
transactions_salted.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Adding Salt to users_df (all the values of salt) and then Exploding it**

# COMMAND ----------

from pyspark.sql.functions import explode, array, lit

# Create array of salt values [0,1,2] for each row
users_df = users_df.withColumn("salt", array([lit(i) for i in range(salt_count)]))

# Exploding the values of the array 
users_expanded = users_df.withColumn("salt", explode(col('salt')))

# Create salted user_id
users_expanded = users_expanded.withColumn("salted_user_id", concat_ws("_", "user_id", "salt"))


# COMMAND ----------

# Displaying users_expanded

users_expanded.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Applying JOIN**

# COMMAND ----------

joined_df = transactions_salted.join(users_expanded, on="salted_user_id", how="inner")

# COMMAND ----------

joined_df.display()

# COMMAND ----------


