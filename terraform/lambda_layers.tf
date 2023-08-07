##########################
####  LAMBDA LAYER 1  ####
##########################


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
  tags = {
    Repo       = "https://github.com/SpinyKarma/de-AWS-pipeline-project"
    Managed_by = "Terraform"
    Project    = "Northcoders-AWS-ETL-pipeline"
  }
}

resource "aws_lambda_layer_version" "lambda_requirements_layer" {
  layer_name          = "lambda_requirements_layer"
  s3_bucket           = aws_s3_object.lambda_layer_code.bucket
  s3_key              = aws_s3_object.lambda_layer_code.key
  compatible_runtimes = ["python3.10"]
  source_code_hash    = data.archive_file.lambda_layer_zip.output_base64sha256
}


##########################
####  LAMBDA LAYER 2  ####
##########################


data "archive_file" "lambda_layer_zip_2" {
  type        = "zip"
  source_dir  = "${path.module}/../lambda_layer_2"
  output_path = "${path.module}/../lambda_layer_2.zip"
}

resource "aws_s3_object" "lambda_layer_code_2" {
  bucket      = aws_s3_bucket.code_bucket.bucket
  key         = "lambda_layer_2.zip"
  acl         = "private"
  source      = data.archive_file.lambda_layer_zip_2.output_path
  source_hash = data.archive_file.lambda_layer_zip_2.output_base64sha256
  tags = {
    Repo       = "https://github.com/SpinyKarma/de-AWS-pipeline-project"
    Managed_by = "Terraform"
    Project    = "Northcoders-AWS-ETL-pipeline"
  }
}

resource "aws_lambda_layer_version" "lambda_requirements_layer_2" {
  layer_name          = "lambda_requirements_layer_2"
  s3_bucket           = aws_s3_object.lambda_layer_code_2.bucket
  s3_key              = aws_s3_object.lambda_layer_code_2.key
  compatible_runtimes = ["python3.10"]
  source_code_hash    = data.archive_file.lambda_layer_zip_2.output_base64sha256
}
