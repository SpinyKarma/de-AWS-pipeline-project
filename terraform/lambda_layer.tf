data "archive_file" "lambda_layer_zip" {
  type        = "zip"
  source_dir  = "${path.module}/../lambda_layer"
  output_path = "${path.module}/../lambda_layer.zip"
}

resource "aws_s3_object" "lambda_layer_code" {
  bucket      = aws_s3_bucket.code_bucket.bucket
  key         = "lambda_layer.zip"
  acl         = "private"
  source      = data.archive_file.lambda_layer_zip.output_path
  source_hash = data.archive_file.lambda_layer_zip.output_base64sha256
}

resource "aws_lambda_layer_version" "lambda_requirements_layer" {
  layer_name          = "lambda_requirements_layer"
  s3_bucket           = aws_s3_bucket.code_bucket.bucket
  s3_key              = "lambda_layer.zip"
  compatible_runtimes = ["python3.10"]
  source_code_hash    = data.archive_file.lambda_layer_zip.output_base64sha256
}
