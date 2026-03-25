import dlt

customers_rules = {'rule1':'customer_id is not null',
                   'rule2':'customer_name is not null'}

@dlt.table(name="customers_stg")
@dlt.expect_all_or_drop(customers_rules)
def customers_stg():
    df = spark.readStream.table("delta_live_tables_catalog.source.customers")
    return df