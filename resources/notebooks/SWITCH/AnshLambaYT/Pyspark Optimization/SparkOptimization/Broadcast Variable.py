# Databricks notebook source
# Dataframe
df = spark.createDataFrame([
    ("1001",),
    ("1002",),
    ("1004",)
], ["product_id"])

# Lookup dictionary (small)
product_dict = {
    "1001": "iPhone",
    "1002": "Samsung",
    "1003": "Pixel"
}

# COMMAND ----------

df.display()

# COMMAND ----------

# Broadcasting the dictionary variable

broad_vr = spark.sparkContext.broadcast(product_dict)

# COMMAND ----------

# Broadcast Variable Value
broad_vr.value

# COMMAND ----------

# Broadcast Variable Key (1001) Value
broad_vr.value.get('1001')

# COMMAND ----------

# Our Function

def mymap(x):

    return broad_vr.value.get(x)

# Converting it into UDF
mymap_udf = udf(mymap)

# COMMAND ----------



# COMMAND ----------

# Using udf function and passing "product_id as input to the function"

df_with_names = df.withColumn("product_name", mymap_udf("product_id"))

# COMMAND ----------

df_with_names.display()

# COMMAND ----------


