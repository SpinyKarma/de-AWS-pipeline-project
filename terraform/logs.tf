resource "aws_cloudwatch_log_group" "ingestion_log_group" {
  name_prefix = "ingestion-lambda-"
}