# import dlt

# #create streaming table
# @dlt.table(name="first_stream_table")
# def first_stream_table():
#   return spark.readStream.table("delta_live_tables_catalog.source.orders")

# #create materialzed view
# @dlt.table(name="first_mat_view")
# def first_mat_view():
#   return spark.read.table("delta_live_tables_catalog.source.orders")

# #create batch view
# @dlt.view(name="first_batch_view")
# def first_batch_view():
#   return spark.read.table("delta_live_tables_catalog.source.orders")

# #create streaming view
# @dlt.view(name="first_stream_view")
# def first_bafirst_stream_viewtch_view():
#   return spark.readStream.table("delta_live_tables_catalog.source.orders")