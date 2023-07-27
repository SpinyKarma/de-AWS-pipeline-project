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

resource "aws_lambda_function" "ingestion_lambda" {  
  filename = "../src/lambda_ingestion/lambda.py"
  function_name = "ingestion_lambda_handler"
  role = aws_iam_role.ingestion_lambda_role.arn
  
  handler = "ingestion_lambda_handler"
  runtime = "python3.10"
}