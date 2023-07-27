resource "aws_cloudwatch_event_rule" "ingestion_lambda_rule" {
    name="ingestion_lambda_rule"
    schedule_expression = "rate(3 minutes)"
}
resource "aws_cloudwatch_event_target" "ingestion_lambda_target" {
  rule=aws_cloudwatch_event_rule.ingestion_lambda_rule.name
  target_id = "SendToLambda"
  arn= aws_lambda_function.ingestion_lambda.arn
}

resource "aws_lambda_permission" "ingestion_lambda_event" {
  statement_id = "AllowExecutionFromEventBridge"
  action = "lambda:InvokeFunction"
  function_name = "ingestion_lambda_handler"
  principal = "events.amazonaws.com"
  source_arn = aws_cloudwatch_event_rule.ingestion_lambda_rule.arn
}

