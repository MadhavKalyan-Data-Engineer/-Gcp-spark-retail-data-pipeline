"""
Project 2: Policyholder Data Enrichment Pipeline (PySpark)
Ingest multi-region policyholder data from Google Cloud Storage
Merge datasets across geographies using union operations
Apply business rule–based filtering on policy risk score
Derive standardized regional identifiers using conditional transformations
"""

# Read policyholder JSON from GCS and convert to Parquet
policyholder_mumbai_df = spark.read.format("json").load("gs://myinsurance-prod-uscentral1-stg-claims/json/source/policyholder_mumbai.json")
policyholder_mumbai_df.write.format("parquet").mode("overwrite").save("gs://myinsurance-prod-uscentral1-stg-claims/json/target/policyholder_mumbai/")
policyholder_mumbai_prq_df = spark.read.format("parquet").load("gs://myinsurance-prod-uscentral1-stg-claims/json/target/policyholder_mumbai/")

policyholder_delhi_df = spark.read.format("json").load("gs://myinsurance-prod-uscentral1-stg-claims/json/source/policyholder_delhi.json")
policyholder_delhi_df.write.format("parquet").mode("overwrite").save("gs://myinsurance-prod-uscentral1-stg-claims/json/target/policyholder_delhi/")
policyholder_delhi_prq_df = spark.read.format("parquet").load("gs://myinsurance-prod-uscentral1-stg-claims/json/target/policyholder_delhi/")

# Read agent JSON from GCS and convert to Parquet
agent_mumbai_df = spark.read.format("json").load("gs://myinsurance-prod-uscentral1-stg-claims/json/source/agent_mumbai.json")
agent_mumbai_df.write.format("parquet").mode("overwrite").save("gs://myinsurance-prod-uscentral1-stg-claims/json/target/agent_mumbai/")
agent_mumbai_prq_df = spark.read.format("parquet").load("gs://myinsurance-prod-uscentral1-stg-claims/json/target/agent_mumbai/")

agent_delhi_df = spark.read.format("json").load("gs://myinsurance-prod-uscentral1-stg-claims/json/source/agent_delhi.json")
agent_delhi_df.write.format("parquet").mode("overwrite").save("gs://myinsurance-prod-uscentral1-stg-claims/json/target/agent_delhi/")
agent_delhi_prq_df = spark.read.format("parquet").load("gs://myinsurance-prod-uscentral1-stg-claims/json/target/agent_delhi/")

# Read claims JSON from GCS and convert to Parquet
claims_df = spark.read.format("json").load("gs://myinsurance-prod-uscentral1-stg-claims/json/source/claims.json")
claims_df.write.format("parquet").mode("overwrite").save("gs://myinsurance-prod-uscentral1-stg-claims/json/target/claims/")
claims_prq_df = spark.read.format("parquet").load("gs://myinsurance-prod-uscentral1-stg-claims/json/target/claims/")

# Union Mumbai & Delhi policyholders and agents
policyholder_df = policyholder_mumbai_df.union(policyholder_delhi_df)
agent_df = agent_mumbai_df.union(agent_delhi_df)

from pyspark.sql.functions import col, lit, sum, coalesce

# Join policyholders with claims, then with agents
policyholder_claims_df = policyholder_df.join(claims_prq_df, on="policy_id", how="inner").withColumn("city1", col("city"))
policyholder_claims_agent_df = policyholder_claims_df.join(agent_df, on="agent_id", how="inner")

# Select relevant columns
select_df = policyholder_claims_agent_df.select("policyholder_name", "city1", "claim_amount")

# Aggregate total claim amount per policyholder per city
group_sum_df = select_df.groupBy("policyholder_name", "city1").agg(sum("claim_amount").alias("Tot_Claim_Amnt"))

# Handle nulls in total claim amount
final_df = group_sum_df.withColumn("Tot_Claim_Amnt", coalesce("Tot_Claim_Amnt", lit(0)))
final_df.show()

# Write final output to GCS
final_df.write.format("parquet").mode("append").save("gs://myinsurance-prod-uscentral1-stg-claims/json/target/Insurance_Claims_Summary/")
