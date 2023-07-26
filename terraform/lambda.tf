locals {
  lambda_path = "../src/lambda_ingestion"
}

data "archive_file" "lambda_zip" {
    type = "zip"
    source_dir = local.lambda_path
    output_path = "${path.module}/lambda_ingestion.zip"
}

resource "aws_s3_object" "lambda_code" {
    bucket = aws_s3_bucket.code_bucket.bucket
    key = "lambda_ingestion.zip"
    acl = "private"

    source = data.archive_file.lambda_zip.output_path
}