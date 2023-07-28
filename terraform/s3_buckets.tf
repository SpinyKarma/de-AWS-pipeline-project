######################
####  S3 BUCKETS  ####
######################

resource "aws_s3_bucket" "raw_csv_data_bucket" {
    bucket_prefix = "terrific-totes-ingestion-bucket"
}

resource "aws_s3_bucket" "code_bucket" {
    bucket_prefix = "lambda-code-bucket"
}

resource "aws_s3_bucket" "processed-parquet-data" {
    bucket_prefix = "terrific-totes-processed-bucket"
}