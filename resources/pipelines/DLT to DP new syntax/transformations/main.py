from pyspark import pipelines as dp
from pyspark.sql.functions import *

# streaming table
@dp.table(name="read_json_tbl2")
def read_json_tbl2():
  return spark.readStream.format("cloudFiles")\
        .option("cloudFiles.format", "json")\
        .load("/Volumes/delta_live_tables_catalog/source/dlt_volume/dlt16MAR/raw/")

# streaming table
@dp.table(name="read_json_tbl3")
def read_json_tbl2():
  return spark.readStream.format("cloudFiles")\
        .option("cloudFiles.format", "json")\
        .load("/Volumes/delta_live_tables_catalog/source/dlt_volume/dlt16MAR/raw/")

# temporary streaming view
@dp.temporary_view(name="silver_streaming_temp_view2")
def silver_streaming_temp_view2():
  df = spark.readStream.table("read_json_tbl2")
  #df = df.dropDuplicates()
  return df

# temporary batch view
@dp.temporary_view(name="silver_batch_temp_view2")
def silver_batch_temp_view2():
  df = spark.read.table("read_json_tbl2")
  #df = df.dropDuplicates()
  return df

# normal table/materialized view (both are same)
@dlt.materialized_view(name="silver_normal_tbl_or_materialized_view2")
def silver_normal_tbl_or_materialized_view2():
  df = spark.read.table("read_json_tbl2")
  #df = df.dropDuplicates()
  return df

# empty streaming table
@dp.table(name="empty_streaming_tbl_using_pass")
def empty_streaming_tbl_using_pass():
  empty_df = spark.createDataFrame([], schema="id INT")
  return empty_df

# empty streaming table
dp.create_streaming_table(
    name="silver_scd1_tbl2",
    comment="Target for streaming data" #,
    #schema="id INT, name STRING, ts TIMESTAMP"  # Optional: Explicit schema
)

# empty streaming table
dp.create_streaming_table(
    name="silver_scd2_tbl3",
    comment="Target for streaming data" #,
    #schema="id INT, name STRING, ts TIMESTAMP"  # Optional: Explicit schema
)

# scd type1 auto_cdc_flow
dp.create_auto_cdc_flow(
  target = "silver_scd1_tbl2",
  source = "read_json_tbl2",
  keys = ["customer_id"],
  sequence_by = "order_id",
  ignore_null_updates = False,
  apply_as_deletes = None,
  apply_as_truncates = None,
  except_column_list = None,
  stored_as_scd_type = 1
)

# scd type2 auto_cdc_flow
dp.create_auto_cdc_flow(
  target = "silver_scd2_tbl3",
  source = "read_json_tbl2",
  keys = ["customer_id"],
  sequence_by = "order_id",
  ignore_null_updates = False,
  apply_as_deletes = None,
  apply_as_truncates = None,
  except_column_list = None,
  stored_as_scd_type = 2
)

# # scd type2 without sequence - NOT POSSIBLE by auto_cdc_flow
# dp.create_auto_cdc_flow(
#   target = "silver_scd2_tbl4",
#   source = "read_json_tbl2",
#   keys = ["customer_id"],
#   sequence_by = None,
#   ignore_null_updates = False,
#   apply_as_deletes = None,
#   apply_as_truncates = None,
#   except_column_list = None,
#   stored_as_scd_type = 2
# )

# normal table
@dp.table(name="gold_streaming_read_from_normal_tbl2")
def gold_streaming_read_from_normal_tbl2():
  df = spark.readStream.schema("order_id int, customer_id int, amount double, status string").table("silver_normal_tbl_or_materialized_view2")
  df = df.groupBy("customer_id").agg(sum("amount").alias("total_amount"))
  #df = df.dropDuplicates()
  return df

# streaming table
@dp.table(name="gold_streaming_tbl2")
def gold_streaming_tbl():
  df = spark.readStream.table("silver_streaming_temp_view2")
  df = df.groupBy("customer_id").agg(sum("amount").alias("total_amount"))
  #df = df.dropDuplicates()
  return df

# normal table
@dp.materialized_view(name="gold_batch_tbl2")
def gold_batch_tbl2():
  df = spark.read.table("silver_batch_temp_view2")
  df = df.groupBy("customer_id").agg(sum("amount").alias("total_amount"))
  #df = df.dropDuplicates()
  return df

# empty streaming table
dp.create_streaming_table(
    name="append_flow_stremaing_tbl2",
    comment="Target for streaming data" #,
    #schema="id INT, name STRING, ts TIMESTAMP"  # Optional: Explicit schema
)

# Append flow to existing streaming table
@dp.append_flow(target="append_flow_stremaing_tbl2")
def ingest_data_from_read_json_tbl2():
    return spark.readStream.table("read_json_tbl2")
  
# Append flow to existing streaming table
@dp.append_flow(target="append_flow_stremaing_tbl2")
def ingest_data_from_read_json_tbl3():
    return spark.readStream.table("read_json_tbl3")