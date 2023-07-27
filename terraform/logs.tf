resource "aws_cloudwatch_log_group" "ingestion_log_group" {
  name = "/aws/lambda/${aws_lambda_function.ingestion_lambda.function_name}"
}
