###################################################
####  LOAD LAMBDA POLICIES AND ROLES ####
###################################################

resource "aws_iam_role" "load_lambda_role" {
  name_prefix        = "role-load-"
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

resource "aws_iam_role_policy_attachment" "load_s3_write_policy_attachment" {
  role       = aws_iam_role.load_lambda_role.name
  policy_arn = aws_iam_policy.s3_policy.arn
}

resource "aws_iam_role_policy_attachment" "load_cw_write_policy_attachment" {
  role       = aws_iam_role.load_lambda_role.name
  policy_arn = aws_iam_policy.cw_load_write_policy.arn
}

resource "aws_iam_role_policy_attachment" "load_lambda_execute_policy_attachment" {
  role       = aws_iam_role.load_lambda_role.name
  policy_arn = aws_iam_policy.lambda_execute_policy.arn
}

resource "aws_iam_role_policy_attachment" "load_sns_publish_policy_attachment" {
  role       = aws_iam_role.load_lambda_role.name
  policy_arn = aws_iam_policy.sns_publish_policy.arn
}


######################
####  LOG GROUP   ####
######################

resource "aws_cloudwatch_log_group" "load_log_group" {
  name = "/aws/lambda/${aws_lambda_function.load_lambda.function_name}"
}


#################################
####  LOAD LAMBDA  ####
#################################


data "archive_file" "load_lambda_zip" {
  type        = "zip"
  source_dir  = "${path.module}/../src/lambda_loading"
  output_path = "${path.module}/../lambda_load.zip"
}

resource "aws_s3_object" "load_lambda_code" {
  bucket      = aws_s3_bucket.code_bucket.bucket
  key         = "lambda_load.zip"
  acl         = "private"
  source      = data.archive_file.load_lambda_zip.output_path
  source_hash = data.archive_file.load_lambda_zip.output_base64sha256
}

resource "aws_lambda_function" "load_lambda" {
  s3_bucket        = aws_s3_object.load_lambda_code.bucket
  s3_key           = aws_s3_object.load_lambda_code.key
  function_name    = "load_lambda_handler"
  role             = aws_iam_role.load_lambda_role.arn
  handler          = "load_lambda.load_lambda_handler"
  runtime          = "python3.10"
  timeout          = "60"
  source_code_hash = data.archive_file.load_lambda_zip.output_base64sha256
  layers           = [aws_lambda_layer_version.lambda_requirements_layer_2.arn]
}

######################
###  load TRIGGER  ###
######################

resource "aws_lambda_function_event_invoke_config" "load_trigger" {
  function_name = aws_lambda_function.transformation_stage_2_lambda.function_name
  destination_config {
    on_success {
      destination = aws_lambda_function.load_lambda.arn
    }
    on_failure {
      destination = aws_sns_topic.notification_topic.arn
    }
  }
}

resource "aws_lambda_permission" "load_lambda_event" {
  statement_id  = "AllowExecutionFromIngestionLambda"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.load_lambda.function_name
  principal     = "lambda.amazonaws.com"
  source_arn    = aws_lambda_function.transformation_stage_2_lambda.arn
}

#############################
####  ALARM AND METRICS  ####
#############################

resource "aws_cloudwatch_log_metric_filter" "load_missing_bucket_error_metric" {
  name           = "metric_name"
  pattern        = "Missing Bucket Error"
  log_group_name = aws_cloudwatch_log_group.load_log_group.name

  metric_transformation {
    name      = "missing_bucket_metric"
    namespace = "missing_bucket_metric"
    value     = "1"
  }
}

resource "aws_cloudwatch_metric_alarm" "load_missing_bucket_error_alarm" {
  alarm_name          = "missing_bucket_error_alarm"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "missing_bucket_metric"
  namespace           = "missing_bucket_metric"
  period              = 60
  statistic           = "Sum"
  threshold           = "1"
  alarm_actions       = [aws_sns_topic.notification_topic.arn]
}

