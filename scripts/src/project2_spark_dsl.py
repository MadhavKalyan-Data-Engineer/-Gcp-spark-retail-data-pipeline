"""
Project 2: Customer Data Enrichment Pipeline (PySpark)

Ingest multi-region customer data from Google Cloud Storage

Merge datasets across geographies using union operations

Apply business rule–based filtering on customer grade

Derive standardized regional identifiers using conditional transformations
"""


customer_lo_df=spark.read.format("json").load("gs://myretail-prod-uscentral1-stg-orders/json/source/customer_lo.json")
customer_lo_df.write.format("parquet").mode("overwrite").save("gs://myretail-prod-uscentral1-stg-orders/json/target/customer_lo/")
customer_lo_prq_df=spark.read.format("parquet").load("gs://myretail-prod-uscentral1-stg-orders/json/target/customer_lo/")

customer_ny_df=spark.read.format("json").load("gs://myretail-prod-uscentral1-stg-orders/json/source/customer_ny.json")
customer_ny_df.write.format("parquet").mode("overwrite").save("gs://myretail-prod-uscentral1-stg-orders/json/target/customer_ny")
customer_ny_prq_df=spark.read.format("parquet").load("gs://myretail-prod-uscentral1-stg-orders/json/target/customer_ny/")

salesman_lo_df=spark.read.format("json").load("gs://myretail-prod-uscentral1-stg-orders/json/source/salesman_lo.json")
salesman_lo_df.write.format("parquet").mode("overwrite").save("gs://myretail-prod-uscentral1-stg-orders/json/target/salesman_lo/")
salesman_lo_prq_df=spark.read.format("parquet").load("gs://myretail-prod-uscentral1-stg-orders/json/target/salesman_lo/")

salesman_ny_df=spark.read.format("json").load("gs://myretail-prod-uscentral1-stg-orders/json/source/salesman_ny.json")
salesman_ny_df.write.format("parquet").mode("overwrite").save("gs://myretail-prod-uscentral1-stg-orders/json/target/salesman_ny/")
salesman_ny_prq_df=spark.read.format("parquet").load("gs://myretail-prod-uscentral1-stg-orders/json/target/salesman_ny/")

orders_df=spark.read.format("json").load("gs://myretail-prod-uscentral1-stg-orders/json/source/orders.json")
orders_df.write.format("parquet").mode("overwrite").save("gs://myretail-prod-uscentral1-stg-orders/json/target/orders/")
orders_prq_df=spark.read.format("parquet").load("gs://myretail-prod-uscentral1-stg-orders/json/target/orders/")

customer_df=customer_lo_df.union(customer_ny_df)
salesman_df=salesman_lo_df.union(salesman_ny_df)
from pyspark.sql.functions import col,lit,sum,coalesce
customer_orders_df=customer_df.join(orders_prq_df,on="customer_id",how="inner").withColumn("city1",col("city"))
customer_orders_salesman_df=customer_orders_df.join(salesman_df,on="salesman_id",how="inner")
select_df=customer_orders_salesman_df.select("name","city1","purch_amt")
group_sum_df=select_df.groupBy("name","city1").agg(sum("purch_amt").alias("Tot_Purchase_Amnt"))
final_df=group_sum_df.withColumn("Tot_Purchase_Amnt",coalesce('Tot_Purchase_Amnt',lit(0)))
final_df.show()

final_df.write.format("parquet").mode("append").save("gs://myretail-prod-uscentral1-stg-orders/json/target/Retail_Summary1/")
