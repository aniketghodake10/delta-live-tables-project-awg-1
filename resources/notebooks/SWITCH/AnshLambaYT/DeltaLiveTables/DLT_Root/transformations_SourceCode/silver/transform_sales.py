import dlt
from pyspark.sql.functions import col

@dlt.view(name='sales_stg_trns')
def sales_stg_trns():
    df = spark.readStream.table('sales_stg')
    df = df.withColumn('total_amount', col('quantity') * col('amount'))
    return df

dlt.create_streaming_table(name="sales_enr")

dlt.create_auto_cdc_flow(
  target = "sales_enr",
  source = "sales_stg_trns",
  keys = ["sales_id"],
  sequence_by = "sale_timestamp",
  ignore_null_updates = False,
  apply_as_deletes = None,
  apply_as_truncates = None,
  except_column_list = None,
  stored_as_scd_type = 1
)