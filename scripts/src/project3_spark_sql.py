"""
Spark SQL Transformation Layer
- Registers Parquet datasets as temporary SQL views
- Executes union, join, and group-by aggregations via Spark SQL
- Produces aggregated retail metrics
- Writes query results to GCS in columnar Parquet format
"""


 customer_lo:
 ------------
customer_lo_df=spark.read.format("json").load("gs://myretail-prod-uscentral1-stg-orders/json/source/customer_lo.json")
customer_lo_df.write.format("parquet").mode("overwrite").save("gs://myretail-prod-uscentral1-stg-orders/json/target/customer_lo/")
customer_lo_prq_df=spark.read.format("parquet").load("gs://myretail-prod-uscentral1-stg-orders/json/target/customer_lo/")
customer_lo_prq_df.createOrReplaceTempView("customer_lo")

customer_ny:
-----------
customer_ny_df=spark.read.format("json").load("gs://myretail-prod-uscentral1-stg-orders/json/source/customer_ny.json")
customer_ny_df.write.format("parquet").mode("overwrite").save("gs://myretail-prod-uscentral1-stg-orders/json/target/customer_ny/")
customer_ny_prq_df=spark.read.format("parquet").load("gs://myretail-prod-uscentral1-stg-orders/json/target/customer_ny/")
customer_ny_prq_df.createOrReplaceTempView("customer_ny")

salesman_lo:
------------
salesman_lo_df=spark.read.format("json").load("gs://myretail-prod-uscentral1-stg-orders/json/source/salesman_lo.json")
salesman_lo_df.write.format("parquet").mode("overwrite").save("gs://myretail-prod-uscentral1-stg-orders/json/target/salesman_lo/")
salesman_lo_prq_df=spark.read.format("parquet").load("gs://myretail-prod-uscentral1-stg-orders/json/target/salesman_lo/")
salesman_lo_prq_df.createOrReplaceTempView("salesman_lo")

salesman_ny:
------------
salesman_ny_df=spark.read.format("json").load("gs://myretail-prod-uscentral1-stg-orders/json/source/salesman_ny.json")
salesman_ny_df.write.format("parquet").mode("overwrite").save("gs://myretail-prod-uscentral1-stg-orders/json/target/salesman_ny/")
salesman_ny_prq_df=spark.read.format("parquet").load("gs://myretail-prod-uscentral1-stg-orders/json/target/salesman_ny/")
salesman_ny_prq_df.createOrReplaceTempView("salesman_ny")

orders:
-------
orders_df=spark.read.format("json").load("gs://myretail-prod-uscentral1-stg-orders/json/source/orders.json")
orders_df.write.format("parquet").mode("overwrite").save("gs://myretail-prod-uscentral1-stg-orders/json/target/orders/")
orders_prq_df=spark.read.format("parquet").load("gs://myretail-prod-uscentral1-stg-orders/json/target/orders/")
orders_prq_df.createOrReplaceTempView("orders")

final_df=spark.sql("select salesman.name,customer.city,coalesce(sum(orders.purch_amt),0) as Tot_Purchase_Amnt from \
(select * from customer_lo union select * from customer_ny)customer \
join  (select * from orders)orders \
on customer.customer_id = orders.customer_id  \
join  (select * from salesman_lo union select * from salesman_ny)salesman \
on salesman.salesman_id = orders.salesman_id \
group by salesman.name,customer.city") \
final_df.write.format("parquet").mode("overwrite").save("gs://myretail-prod-uscentral1-stg-orders/json/target/Retail_Summary/")



