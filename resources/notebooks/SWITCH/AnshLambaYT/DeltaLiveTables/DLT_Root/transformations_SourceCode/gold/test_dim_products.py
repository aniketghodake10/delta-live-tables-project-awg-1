import dlt



dlt.create_streaming_table(name="test_dim_products")

dlt.create_auto_cdc_flow(
  target = "test_dim_products",
  source = "products_enr",
  keys = ["product_id"],
  sequence_by = "last_updated",
  ignore_null_updates = False,
  apply_as_deletes = None,
  apply_as_truncates = None,
  except_column_list = None,
  stored_as_scd_type = 2
)