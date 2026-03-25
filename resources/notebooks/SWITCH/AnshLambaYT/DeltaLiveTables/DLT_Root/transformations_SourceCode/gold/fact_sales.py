import dlt



dlt.create_streaming_table(name="fact_sales")

dlt.create_auto_cdc_flow(
  target = "fact_sales",
  source = "sales_stg_trns",
  keys = ["sales_id"],
  sequence_by = "sale_timestamp",
  ignore_null_updates = False,
  apply_as_deletes = None,
  apply_as_truncates = None,
  except_column_list = None,
  stored_as_scd_type = 1
)