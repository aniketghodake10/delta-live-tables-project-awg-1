import dlt

@dlt.table(name="read_json_tbl")
def read_json_tbl():
  return spark.readStream.format("cloudFiles")\
        .option("cloudFiles.format", "json")\
        .load("/Volumes/delta_live_tables_catalog/source/dlt_volume/dlt9SEP/raw/")

@dlt.view(name="silver_view")
def silver_view():
  df = spark.readStream.table("read_json_tbl")
  #df = df.dropDuplicates()
  return df

@dlt.table(name="silver_normal_tbl")
def silver_normal_tbl():
  df = spark.read.table("read_json_tbl")
  #df = df.dropDuplicates()
  return df

@dlt.materialized_view(name="silver_materialized_view")
def silver_materialized_view():
  df = spark.read.table("read_json_tbl")
  #df = df.dropDuplicates()
  return df

dlt.create_streaming_table(name="silver_scd1_tbl")

dlt.create_auto_cdc_flow(
  target = "silver_scd1_tbl",
  source = "read_json_tbl",
  keys = ["customer_id"],
  sequence_by = "order_id",
  ignore_null_updates = False,
  apply_as_deletes = None,
  apply_as_truncates = None,
  except_column_list = None,
  stored_as_scd_type = 1
)

@dlt.table(name="gold_streaming_tbl")
def gold_streaming_tbl():
  df = spark.read.table("silver_normal_tbl")
  #df = df.dropDuplicates()
  return df