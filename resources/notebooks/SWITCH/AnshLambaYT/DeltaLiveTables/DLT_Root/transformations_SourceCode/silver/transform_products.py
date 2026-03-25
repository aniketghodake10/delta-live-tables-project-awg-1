import dlt
from pyspark.sql.functions import col

@dlt.view(name='products_stg_trns')
def sales_stg_trns():
    df = spark.readStream.table('products_stg')
    df = df.withColumn('price', col('price').cast('int'))
    return df

dlt.create_streaming_table(name="products_enr")

dlt.create_auto_cdc_flow(
  target = "products_enr",
  source = "products_stg_trns",
  keys = ["product_id"],
  sequence_by = "last_updated",
  ignore_null_updates = False,
  apply_as_deletes = None,
  apply_as_truncates = None,
  except_column_list = None,
  stored_as_scd_type = 1
)