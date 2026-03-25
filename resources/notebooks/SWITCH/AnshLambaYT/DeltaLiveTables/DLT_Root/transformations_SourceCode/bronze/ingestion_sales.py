import dlt

# sales_rules = {'rule1':'sales_id is not null'}

# # create empty streaming table
# dlt.create_streaming_table(name="sales_stg",
#                            expect_all_or_drop=sales_rules)

# # east sales flow
# @dlt.append_flow(target='sales_stg')
# def east_sales():
#     df = spark.readStream.table('delta_live_tables_catalog.source.sales_east')
#     return df

# # west sales flow
# @dlt.append_flow(target='sales_stg')
# def west_sales():
#     df = spark.readStream.table('delta_live_tables_catalog.source.sales_west')
#     return df





sales_rules = {"rule1": "sales_id IS NOT NULL"}

@dlt.table(
    name="sales_stg"
)
@dlt.expect_all_or_drop(sales_rules)
def sales_stg():
    east = spark.readStream.table("delta_live_tables_catalog.source.sales_east")
    west = spark.readStream.table("delta_live_tables_catalog.source.sales_west")
    return east.unionByName(west)
