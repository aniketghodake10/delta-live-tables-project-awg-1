import dlt


@dlt.materialized_view(name="business_sales")
def business_sales():
    df_fact = spark.read.table("fact_sales")
    df_dimCust = spark.read.table("dim_customers")
    df_dimProd = spark.read.table("dim_products")
    df_join = df_fact.join(df_dimCust, df_fact.customer_id == df_dimCust.customer_id).join(df_dimProd, df_fact.product_id == df_dimProd.product_id)
    df_agg = df_join.groupBy("region", "category").agg({"total_amount": "sum"}).withColumnRenamed("sum(total_amount)", "total_sales")
    return df_agg