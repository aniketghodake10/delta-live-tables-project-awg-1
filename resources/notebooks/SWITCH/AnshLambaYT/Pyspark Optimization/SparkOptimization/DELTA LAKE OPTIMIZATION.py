# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE SCHEMA ansh_schema

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE ansh_schema.anshtbl
# MAGIC (
# MAGIC   id INT,
# MAGIC   salary INT
# MAGIC )
# MAGIC USING DELTA 
# MAGIC LOCATION '/FileStore/rawdata/deltatbl'

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO ansh_schema.anshtbl
# MAGIC VALUES 
# MAGIC (5,100),
# MAGIC (6,100)

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE delta.`/FileStore/rawdata/deltatbl` ZORDER BY (id)

# COMMAND ----------


