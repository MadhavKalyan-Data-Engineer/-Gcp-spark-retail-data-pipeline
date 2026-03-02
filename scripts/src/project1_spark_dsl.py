"""
Project 1: PySpark DataFrame API
- Read policyholder claims data from GCS
- Union Mumbai & Delhi policyholders
- Filter risk_score = 200
- Add derived country column
"""

# create bucket myinsurance-prod-uscentral1-stg-claims
# copy linux files to the bucket 
# gsutil cp /local/path/claims_mumbai.json gs://myinsurance-prod-uscentral1-stg-claims/claims_mumbai.json
# gsutil cp /local/path/claims_delhi.json gs://myinsurance-prod-uscentral1-stg-claims/claims_delhi.json

from pyspark.sql.functions import lit

claims_mumbai_df = spark.read.format("json").load("gs://myinsurance-prod-uscentral1-stg-claims/claims_mumbai.json")
claims_delhi_df = spark.read.format("json").load("gs://myinsurance-prod-uscentral1-stg-claims/claims_delhi.json")

claims_union_df = claims_mumbai_df.union(claims_delhi_df)
claims_union_df.show()

claims_filter_df = claims_union_df.filter(claims_union_df.risk_score == 200)
claims_filter_df.show()

claims_select_df = claims_filter_df.select("city", "policyholder_name", "policy_id", "claim_amount")
claims_select_df.show()

claims_final_df = claims_select_df.withColumn("country", lit("India"))
claims_final_df.show()

claims_final_df.write.format("json").mode("overwrite").save("gs://myinsurance-prod-uscentral1-stg-claims/output/high_risk_claims.json")
