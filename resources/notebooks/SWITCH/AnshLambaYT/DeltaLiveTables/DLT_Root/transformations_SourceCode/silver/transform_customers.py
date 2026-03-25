import dlt
from pyspark.sql.functions import col, upper

@dlt.view(name='customers_stg_trns')
def sales_stg_trns():
    df = spark.readStream.table('customers_stg')
    df = df.withColumn('customer_name', upper(col('customer_name')))
    return df

dlt.create_streaming_table(name="customers_enr")

dlt.create_auto_cdc_flow(
  target = "customers_enr",
  source = "customers_stg_trns",
  keys = ["customer_id"],
  sequence_by = "last_updated",
  ignore_null_updates = False,
  apply_as_deletes = None,
  apply_as_truncates = None,
  except_column_list = None,
  stored_as_scd_type = 1
)