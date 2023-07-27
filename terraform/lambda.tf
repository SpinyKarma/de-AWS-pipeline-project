locals {
  lambda_path = "../src/lambda_ingestion"
}

data "archive_file" "lambda_zip" {
  type        = "zip"
  source_dir  = "${path.module}/../src/lambda_ingestion"
  output_path = "${path.module}/../lambda_ingestion.zip"
}

resource "aws_s3_object" "lambda_code" {
  bucket = aws_s3_bucket.code_bucket.bucket
  key    = "lambda_ingestion.zip"
  acl    = "private"

  source = data.archive_file.lambda_zip.output_path
}

resource "aws_lambda_function" "ingestion_lambda" {
  s3_bucket     = aws_s3_bucket.code_bucket.bucket
  s3_key        = "lambda_ingestion.zip"
  function_name = "ingestion_lambda_handler"
  role          = aws_iam_role.ingestion_lambda_role.arn
  handler       = "ingestion_lambda.ingestion_lambda_handler"
  #handler may need to be "lambda_ingestion.lambda.ingestion_lambda_handler"
  runtime = "python3.10"
}
