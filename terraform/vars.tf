variable "lambda_name" {
  type    = string
  default = "s3-file-reader"
}

data "aws_caller_identity" "current" {}

data "aws_region" "current" {}

