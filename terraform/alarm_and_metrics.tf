resource "aws_cloudwatch_log_metric_filter" "table_ingestion_error_metric" {
  name           = "table_ingestion_error_metric"
  pattern        = "TableIngestionError"
  log_group_name = "/aws/lambda/${aws_lambda_function.ingestion_lambda.function_name}"

  metric_transformation {
    name      = "table_ingestion_error_metric"
    namespace = "table_ingestion_error_metric"
    value     = "1"
  }
}

resource "aws_cloudwatch_metric_alarm" "table_ingestion_error_alarm" {
  alarm_name                = "table_ingestion_error_alarm"
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  evaluation_periods        = 1
  metric_name               = "table_ingestion_error_metric"
  namespace                 = "table_ingestion_error_metric"
  period                    = 60
  statistic                 = "Sum"
  threshold                 = "1"

  alarm_actions = [aws_sns_topic.notification_topic.arn]
}

resource "aws_cloudwatch_log_metric_filter" "invalid_credentials_error_metric" {
  name           = "invalid_credentials_error_metric"
  pattern        = "InvalidCredentialsError"
  log_group_name = "/aws/lambda/${aws_lambda_function.ingestion_lambda.function_name}"

  metric_transformation {
    name      = "invalid_credentials_error_metric"
    namespace = "invalid_credentials_error_metric"
    value     = "1"
  }
}

resource "aws_cloudwatch_metric_alarm" "invalid_credentials_error_alarm" {
  alarm_name                = "invalid_credentials_error_alarm"
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  evaluation_periods        = 1
  metric_name               = "invalid_credentials_error_metric"
  namespace                 = "invalid_credentials_error_metric"
  period                    = 60
  statistic                 = "Sum"
  threshold                 = "1"

  alarm_actions = [aws_sns_topic.notification_topic.arn]
}

resource "aws_cloudwatch_log_metric_filter" "ingestion_end" {
  name           = "ingestion_end"
  pattern        = "END"
  log_group_name = "/aws/lambda/${aws_lambda_function.ingestion_lambda.function_name}"

  metric_transformation {
    name      = "ingestion_end"
    namespace = "ingestion_end"
    value     = "1"
  }
}

resource "aws_cloudwatch_log_metric_filter" "exception_error_metric" {
  name           = "exception_error"
  pattern        = "Exception"
  log_group_name = "/aws/lambda/${aws_lambda_function.ingestion_lambda.function_name}"

  metric_transformation {
    name      = "exception_Error_metric"
    namespace = "exception_Error_metric"
    value     = "1"
  }
}

resource "aws_cloudwatch_metric_alarm" "exception_error_alarm" {
  alarm_name                = "exception_Error_alarm"
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  evaluation_periods        = 1
  metric_name               = "exception_Error_metric"
  namespace                 = "exception_Error_metric"
  period                    = 60
  statistic                 = "Sum"
  threshold                 = "1"

  alarm_actions = [aws_sns_topic.notification_topic.arn]
}