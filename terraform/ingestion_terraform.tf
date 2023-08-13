########################################################
####  INGESTION LAMBDA ROLE AND POLICY ATTACHMENTS  ####
########################################################


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
  tags = {
    Repo       = "https://github.com/SpinyKarma/de-AWS-pipeline-project"
    Managed_by = "Terraform"
    Project    = "Northcoders-AWS-ETL-pipeline"
    Lambda     = "Ingestion"
  }
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
  policy_arn = aws_iam_policy.cw_ingestion_write_policy.arn
}

resource "aws_iam_role_policy_attachment" "ingestion_lambda_execute_policy_attachment" {
  role       = aws_iam_role.ingestion_lambda_role.name
  policy_arn = aws_iam_policy.lambda_execute_policy.arn
}

resource "aws_iam_role_policy_attachment" "ingestion_sns_publish_policy_attachment" {
  role       = aws_iam_role.ingestion_lambda_role.name
  policy_arn = aws_iam_policy.sns_publish_policy.arn
}



###############################
####  INGESTION LOG GROUP  ####
###############################


resource "aws_cloudwatch_log_group" "ingestion_log_group" {
  name = "/aws/lambda/${aws_lambda_function.ingestion_lambda.function_name}"
  tags = {
    Repo       = "https://github.com/SpinyKarma/de-AWS-pipeline-project"
    Managed_by = "Terraform"
    Project    = "Northcoders-AWS-ETL-pipeline"
    Lambda     = "Ingestion"
  }
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
  tags = {
    Repo       = "https://github.com/SpinyKarma/de-AWS-pipeline-project"
    Managed_by = "Terraform"
    Project    = "Northcoders-AWS-ETL-pipeline"
    Lambda     = "Ingestion"
  }
}

resource "aws_lambda_function" "ingestion_lambda" {
  s3_bucket        = aws_s3_object.ingestion_lambda_code.bucket
  s3_key           = aws_s3_object.ingestion_lambda_code.key
  function_name    = "ingestion_lambda_handler"
  role             = aws_iam_role.ingestion_lambda_role.arn
  handler          = "ingestion_lambda.ingestion_lambda_handler"
  runtime          = "python3.10"
  timeout          = "60"
  source_code_hash = data.archive_file.ingestion_lambda_zip.output_base64sha256
  layers           = [aws_lambda_layer_version.lambda_requirements_layer.arn]
  tags = {
    Repo       = "https://github.com/SpinyKarma/de-AWS-pipeline-project"
    Managed_by = "Terraform"
    Project    = "Northcoders-AWS-ETL-pipeline"
    Lambda     = "Ingestion"
  }
}


################################
####  EVENT BRIDGE TRIGGER  ####
################################


resource "aws_cloudwatch_event_rule" "ingestion_lambda_rule" {
  name                = "ingestion_lambda_rule"
  schedule_expression = "rate(3 minutes)"
  is_enabled          = true
  tags = {
    Repo       = "https://github.com/SpinyKarma/de-AWS-pipeline-project"
    Managed_by = "Terraform"
    Project    = "Northcoders-AWS-ETL-pipeline"
    Lambda     = "Ingestion"
  }
}

resource "aws_cloudwatch_event_target" "ingestion_lambda_target" {
  rule      = aws_cloudwatch_event_rule.ingestion_lambda_rule.name
  target_id = "SendToLambda"
  arn       = aws_lambda_function.ingestion_lambda.arn
}

resource "aws_lambda_permission" "ingestion_lambda_event" {
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.ingestion_lambda.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.ingestion_lambda_rule.arn
}


##############################
####  ALARMS AND METRICS  ####
##############################


resource "aws_cloudwatch_log_metric_filter" "table_ingestion_error_metric" {
  name           = "table_ingestion_error_metric"
  pattern        = "TableIngestionError"
  log_group_name = aws_cloudwatch_log_group.ingestion_log_group.name

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
  metric_name         = aws_cloudwatch_log_metric_filter.table_ingestion_error_metric.metric_transformation[0].name
  namespace           = aws_cloudwatch_log_metric_filter.table_ingestion_error_metric.metric_transformation[0].namespace
  period              = 60
  statistic           = "Sum"
  threshold           = "1"
  alarm_actions       = [aws_sns_topic.notification_topic.arn]
  tags = {
    Repo       = "https://github.com/SpinyKarma/de-AWS-pipeline-project"
    Managed_by = "Terraform"
    Project    = "Northcoders-AWS-ETL-pipeline"
    Lambda     = "Ingestion"
  }
}

resource "aws_cloudwatch_log_metric_filter" "invalid_credentials_error_metric" {
  name           = "invalid_credentials_error_metric"
  pattern        = "InvalidCredentialsError"
  log_group_name = aws_cloudwatch_log_group.ingestion_log_group.name

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
  metric_name         = aws_cloudwatch_log_metric_filter.invalid_credentials_error_metric.metric_transformation[0].name
  namespace           = aws_cloudwatch_log_metric_filter.invalid_credentials_error_metric.metric_transformation[0].namespace
  period              = 60
  statistic           = "Sum"
  threshold           = "1"
  alarm_actions       = [aws_sns_topic.notification_topic.arn]
  tags = {
    Repo       = "https://github.com/SpinyKarma/de-AWS-pipeline-project"
    Managed_by = "Terraform"
    Project    = "Northcoders-AWS-ETL-pipeline"
    Lambda     = "Ingestion"
  }
}

resource "aws_cloudwatch_log_metric_filter" "no_timestamp_error_metric" {
  name           = "no_timestamp_error_metric"
  pattern        = "NonTimestampedCSVError"
  log_group_name = aws_cloudwatch_log_group.ingestion_log_group.name

  metric_transformation {
    name      = "no_timestamp_Error_metric"
    namespace = "no_timestamp_Error_metric"
    value     = "1"
  }
}

resource "aws_cloudwatch_metric_alarm" "no_timestamp_error_alarm" {
  alarm_name          = "no_timestamp_error_alarm"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = aws_cloudwatch_log_metric_filter.no_timestamp_error_metric.metric_transformation[0].name
  namespace           = aws_cloudwatch_log_metric_filter.no_timestamp_error_metric.metric_transformation[0].namespace
  period              = 60
  statistic           = "Sum"
  threshold           = "1"
  alarm_actions       = [aws_sns_topic.notification_topic.arn]
  tags = {
    Repo       = "https://github.com/SpinyKarma/de-AWS-pipeline-project"
    Managed_by = "Terraform"
    Project    = "Northcoders-AWS-ETL-pipeline"
    Lambda     = "Ingestion"
  }
}


resource "aws_cloudwatch_log_metric_filter" "exception_error_metric" {
  name           = "exception_error_metric"
  pattern        = "Exception"
  log_group_name = aws_cloudwatch_log_group.ingestion_log_group.name

  metric_transformation {
    name      = "exception_Error_metric"
    namespace = "exception_Error_metric"
    value     = "1"
  }
}

resource "aws_cloudwatch_metric_alarm" "exception_error_alarm" {
  alarm_name          = "exception_error_alarm"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = aws_cloudwatch_log_metric_filter.exception_error_metric.metric_transformation[0].name
  namespace           = aws_cloudwatch_log_metric_filter.exception_error_metric.metric_transformation[0].namespace
  period              = 60
  statistic           = "Sum"
  threshold           = "1"
  alarm_actions       = [aws_sns_topic.notification_topic.arn]
  tags = {
    Repo       = "https://github.com/SpinyKarma/de-AWS-pipeline-project"
    Managed_by = "Terraform"
    Project    = "Northcoders-AWS-ETL-pipeline"
    Lambda     = "Ingestion"
  }
}


