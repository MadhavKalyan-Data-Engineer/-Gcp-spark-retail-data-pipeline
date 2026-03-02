# GCP Spark Insurance Data Pipeline

## 📌 Project Overview
This project demonstrates an end-to-end Data Engineering pipeline built using PySpark on Google Cloud Platform (GCP). The pipeline processes insurance policyholder, agent, and claims data stored in Google Cloud Storage (GCS), applies transformations, and generates aggregated analytics outputs.

## 🛠 Tech Stack
- PySpark
- Spark DSL
- Google Cloud Storage (GCS)
- JSON & Parquet
- Linux / gsutil

## 🏗 Architecture
- Source data stored in GCS (JSON)
- Spark reads data from GCS
- Transformations using DataFrame API & Spark SQL
- Optimized storage using Parquet
- Final aggregated output written back to GCS

## 🔄 Data Pipeline Flow
1. Upload JSON files to GCS
2. Read data using PySpark
3. Convert JSON to Parquet
4. Perform unions, joins, filters
5. Aggregate insurance claim amounts
6. Store final summary in GCS

## 📊 Use Case
Insurance analytics to calculate total claim amount by:
- Agent
- City

## 📂 Project Modules
- Project 1: Spark DataFrame API transformations
- Project 2: End-to-end ETL with joins & aggregation
- Project 3: Spark SQL-based implementation

## 🚀 Output
Final insurance claims summary stored in Parquet format in GCS for downstream analytics.

## 👤 Author
Madhav Kalyan  
Data Engineer | PySpark | GCP | SQL
