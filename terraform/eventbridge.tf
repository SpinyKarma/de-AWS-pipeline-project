resource "aws_cloudwatch_event_rule" "ingestion_lambda_rule" {
    name="ingestion_lambda_rule"
    schedule_expression = "rate(3 minutes)"
}
# resource "aws_cloudwatch_event_target" "ingestion_lambda_target" {
#   rule=aws_cloudwatch_event_rule.ingestion_lambda_rule.name
#   target_id = "SendToLambda"
#   arn=""
# }

# resource "aws_lambda_permission" "ingestion_lambda_event" {
#   statement_id = "AllowExecutionFromEventBridge"
#   action = "lambda:InvokeFunction"
#   function_name =          # cheange when we have  Lambda file
#   principal = "events.amazonaws.com"
#   source_arn = aws_cloudwatch_event_rule.ingestion_lambda_rule.arn
# }

