##############################################
####  INGESTION LAMBDA POLICIES AND ROLES ####
##############################################

resource "aws_iam_role" "ingestion_lambda_role" {
  name_prefix        = "role-ingestion-"
  assume_role_policy = <<EOF
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "sts:AssumeRole"
                ],
                "Principal": {
                    "Service": [
                        "lambda.amazonaws.com"
                    ]
                }
            }
        ]
    }
    EOF
}

resource "aws_iam_role_policy_attachment" "ingestion_s3_write_policy_attachment" {
  role       = aws_iam_role.ingestion_lambda_role.name
  policy_arn = aws_iam_policy.s3_policy.arn
}

resource "aws_iam_role_policy_attachment" "ingestion_secret_read_policy_attachment" {
  role       = aws_iam_role.ingestion_lambda_role.name
  policy_arn = aws_iam_policy.secret_read_policy.arn
}

resource "aws_iam_role_policy_attachment" "ingestion_cw_write_policy_attachment" {
  role       = aws_iam_role.ingestion_lambda_role.name
  policy_arn = aws_iam_policy.cw_write_policy.arn
}


######################
####  LOG GROUP   ####
######################

resource "aws_cloudwatch_log_group" "ingestion_log_group" {
  name = "/aws/lambda/${aws_lambda_function.ingestion_lambda.function_name}"
}


############################
####  INGESTION LAMBDA  ####
############################

data "archive_file" "ingestion_lambda_zip" {
  type        = "zip"
  source_dir  = "${path.module}/../src/lambda_ingestion"
  output_path = "${path.module}/../lambda_ingestion.zip"
}

resource "aws_s3_object" "ingestion_lambda_code" {
  bucket      = aws_s3_bucket.code_bucket.bucket
  key         = "lambda_ingestion.zip"
  acl         = "private"
  source      = data.archive_file.ingestion_lambda_zip.output_path
  source_hash = data.archive_file.ingestion_lambda_zip.output_base64sha256
}

resource "aws_lambda_function" "ingestion_lambda" {
  s3_bucket        = aws_s3_bucket.code_bucket.bucket
  s3_key           = "lambda_ingestion.zip"
  function_name    = "ingestion_lambda_handler"
  role             = aws_iam_role.ingestion_lambda_role.arn
  handler          = "ingestion_lambda.ingestion_lambda_handler"
  runtime          = "python3.10"
  timeout          = "60"
  source_code_hash = data.archive_file.ingestion_lambda_zip.output_base64sha256
  layers           = [aws_lambda_layer_version.lambda_requirements_layer.arn]
}


########################
####  EVENT BRIDGE  ####
########################

resource "aws_cloudwatch_event_rule" "ingestion_lambda_rule" {
  name                = "ingestion_lambda_rule"
  schedule_expression = "rate(3 minutes)"
}

resource "aws_cloudwatch_event_target" "ingestion_lambda_target" {
  rule      = aws_cloudwatch_event_rule.ingestion_lambda_rule.name
  target_id = "SendToLambda"
  arn       = aws_lambda_function.ingestion_lambda.arn
}

resource "aws_lambda_permission" "ingestion_lambda_event" {
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = "ingestion_lambda_handler"
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.ingestion_lambda_rule.arn
}


#############################
####  ALARM AND METRICS  ####
#############################

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
  alarm_name          = "table_ingestion_error_alarm"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "table_ingestion_error_metric"
  namespace           = "table_ingestion_error_metric"
  period              = 60
  statistic           = "Sum"
  threshold           = "1"

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
  alarm_name          = "invalid_credentials_error_alarm"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "invalid_credentials_error_metric"
  namespace           = "invalid_credentials_error_metric"
  period              = 60
  statistic           = "Sum"
  threshold           = "1"

  alarm_actions = [aws_sns_topic.notification_topic.arn]
}

resource "aws_cloudwatch_log_metric_filter" "no_timestamp_error_metric" {
  name           = "no_timestamp_error"
  pattern        = "NonTimestampedCSVError"
  log_group_name = "/aws/lambda/${aws_lambda_function.ingestion_lambda.function_name}"

  metric_transformation {
    name      = "no_timestamp_Error_metric"
    namespace = "no_timestamp_Error_metric"
    value     = "1"
  }
}

resource "aws_cloudwatch_metric_alarm" "no_timestamp_error_alarm" {
  alarm_name          = "no_timestamp_Error_alarm"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "no_timestamp_Error_metric"
  namespace           = "no_timestamp_Error_metric"
  period              = 60
  statistic           = "Sum"
  threshold           = "1"

  alarm_actions = [aws_sns_topic.notification_topic.arn]
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
  alarm_name          = "exception_Error_alarm"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "exception_Error_metric"
  namespace           = "exception_Error_metric"
  period              = 60
  statistic           = "Sum"
  threshold           = "1"

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


