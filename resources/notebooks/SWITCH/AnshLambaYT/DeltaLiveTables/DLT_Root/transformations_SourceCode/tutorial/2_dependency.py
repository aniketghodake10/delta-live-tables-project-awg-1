# import dlt
# import pyspark.sql.functions as F

# '''creating an End-to-end basic pipeline'''

# #Staging Area
# @dlt.table(name="staging_orders")
# def staging_orders():
#     return spark.readStream.table("delta_live_tables_catalog.source.orders")

# #Transformed Area
# @dlt.view(name="transformed_orders")
# def transformed_orders():
#     df = spark.readStream.table("staging_orders")
#     df = df.withColumn('order_status', F.lower(F.col('order_status')))
#     return df

# #Aggregated Area
# @dlt.table(name="aggregated_orders")
# def aggregated_orders():
#     df = spark.readStream.table("transformed_orders")
#     df = df.groupBy('order_status').count()
#     return df

