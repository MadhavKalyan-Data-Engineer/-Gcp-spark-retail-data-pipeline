# Created GCS bucket
gsutil mb gs://kalanspark

# Upload files
gsutil cp /local/path/*.json gs://kalanspark/json/source/
