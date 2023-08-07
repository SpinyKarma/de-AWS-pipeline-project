######################
####  S3 BUCKETS  ####
######################


resource "aws_s3_bucket" "raw_csv_data_bucket" {
  bucket_prefix = var.s3_raw_data_bucket_prefix
  tags = {
    Project = "Northcoders-AWS-ETL-pipeline"
  }
}

resource "aws_s3_bucket" "code_bucket" {
  bucket_prefix = var.s3_code_bucket_prefix
  tags = {
    Project = "Northcoders-AWS-ETL-pipeline"
  }
}

resource "aws_s3_bucket" "processed-parquet-data" {
  bucket_prefix = var.s3_processed_data_bucket_prefix
  tags = {
    Project = "Northcoders-AWS-ETL-pipeline"
  }
}
