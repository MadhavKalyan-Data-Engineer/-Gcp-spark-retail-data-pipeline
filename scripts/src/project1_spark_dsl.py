"""
Project 1: PySpark DataFrame API
- Read customer data from GCS
- Union London & New York customers
- Filter grade = 200
- Add derived country column
"""

create bucket myretail-prod-uscentral1-stg-orders
copy linux files to the bucket 
gsutil linux file path gs://myretail-prod-uscentral1-stg-orders/customer_lo.json
customer_lo_df=spark.read.format("json").load("gs://myretail-prod-uscentral1-stg-orders/customer_lo.json")
customer_filter_df=customer_lo_df.filter(customer_lo_df.grade==200)
customer_filter_df.show()
Customer_select_df=customer_filter_df.select("City","Cust_name","Customer_id")
Customer_select_df.show()
customer_select_df.write.format("json").mode("overwrite").save("/myretail-prod-uscentral1-stg-orders/customer_lo.json")

