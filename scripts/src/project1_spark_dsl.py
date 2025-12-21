"""
Project 1: PySpark DataFrame API
- Read customer data from GCS
- Union London & New York customers
- Filter grade = 200
- Add derived country column
"""

create bucket kalyanspark
copy linux files to the bucket 
gsutil linux file path gs://kalyanspark/filename.json
customer_lo_df=spark.read.format("json").load("gs://kalyanspark/filename.json")
customer_filter_df=customer_lo_df.filter(customer_lo_df.grade==200)
customer_filter_df.show()
Customer_select_df=customer_filter_df.select("City","Cust_name","Customer_id")
Customer_select_df.show()
customer_select_df.write.format("json").mode("overwrite").save("/kalyanhdfs/customer_lo.json")


