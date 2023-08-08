######################
####  S3 BUCKETS  ####
######################


resource "aws_s3_bucket" "raw_csv_data_bucket" {
  bucket_prefix = "terrific-totes-ingestion-bucket"
  tags = {
    Repo       = "https://github.com/SpinyKarma/de-AWS-pipeline-project"
    Managed_by = "Terraform"
    Project    = "Northcoders-AWS-ETL-pipeline"
  }
}

resource "aws_s3_bucket" "code_bucket" {
  bucket_prefix = "lambda-code-bucket"
  tags = {
    Repo       = "https://github.com/SpinyKarma/de-AWS-pipeline-project"
    Managed_by = "Terraform"
    Project    = "Northcoders-AWS-ETL-pipeline"
  }
}

resource "aws_s3_bucket" "processed-parquet-data" {
  bucket_prefix = "terrific-totes-processed-bucket"
  tags = {
    Repo       = "https://github.com/SpinyKarma/de-AWS-pipeline-project"
    Managed_by = "Terraform"
    Project    = "Northcoders-AWS-ETL-pipeline"
  }
}
