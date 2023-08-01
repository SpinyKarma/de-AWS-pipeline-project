###################################################
####  TRANSFORMATION LAMBDA POLICIES AND ROLES ####
###################################################

resource "aws_iam_role" "transformation_lambda_role" {
  name_prefix        = "role-transformation-"
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

################################
###  TRANSFORMATION TRIGGER  ###
################################

# resource "aws_cloudwatch_log_group" "subscription_log_group" {
#   name = "transform_trigger"
# }


# resource "aws_lambda_permission" "allow_cloudwatch_trigger_transformation" {
#  statement_id = "AllowExecutionFromCloudWatch"
#  action = "lambda:InvokeFunction"
#  function_name = "transformation_lambda_handler"
#  principal   = "logs.${var.region}.amazonaws.com"
#  source_arn = data.aws_cloudwatch_log_group.ingestion_log_group.arn
# }

# resource "aws_cloudwatch_log_subscription_filter" "transformation_trigger" {
#  depends_on      = ["aws_lambda_permission.allow_cloudwatch_trigger_transformation"]
#  name            = "transformation_trigger"
#  log_group_name  = "transform_trigger"
#  filter_pattern  = "END"
#  destination_arn = aws_lambda_function.ingestion_lambda.arn
#  distribution    = "ByLogStream"
# }


resource "aws_iam_role_policy_attachment" "trans_s3_write_policy_attachment" {
  role       = aws_iam_role.transformation_lambda_role.name
  policy_arn = aws_iam_policy.s3_policy.arn
}

resource "aws_iam_role_policy_attachment" "transformation_secret_read_policy_attachment" {
  role       = aws_iam_role.transformation_lambda_role.name
  policy_arn = aws_iam_policy.secret_read_policy.arn
}

resource "aws_iam_role_policy_attachment" "transformation_cw_write_policy_attachment" {
  role       = aws_iam_role.transformation_lambda_role.name
  policy_arn = aws_iam_policy.cw_write_policy.arn
}


######################
####  LOG GROUP   ####
######################

resource "aws_cloudwatch_log_group" "transformation_log_group" {
  name = "/aws/lambda/${aws_lambda_function.transformation_lambda.function_name}"
}


#################################
####  TRANSFORMATION LAMBDA  ####
#################################


data "archive_file" "transformation_lambda_zip" {
  type        = "zip"
  source_dir  = "${path.module}/../src/lambda_transformation"
  output_path = "${path.module}/../lambda_transformation.zip"
}

resource "aws_s3_object" "transformation_lambda_code" {
  bucket      = aws_s3_bucket.code_bucket.bucket
  key         = "lambda_transformation.zip"
  acl         = "private"
  source      = data.archive_file.transformation_lambda_zip.output_path
  source_hash = data.archive_file.transformation_lambda_zip.output_base64sha256
}

resource "aws_lambda_function" "transformation_lambda" {
  s3_bucket        = aws_s3_bucket.code_bucket.bucket
  s3_key           = "lambda_transformation.zip"
  function_name    = "transformation_lambda_handler"
  role             = aws_iam_role.transformation_lambda_role.arn
  handler          = "transformation_lambda.transformation_lambda_handler"
  runtime          = "python3.10"
  timeout          = "60"
  source_code_hash = data.archive_file.transformation_lambda_zip.output_base64sha256
  layers           = [aws_lambda_layer_version.lambda_requirements_layer.arn]
}

################################
###  TRANSFORMATION TRIGGER  ###
################################

resource "aws_lambda_function_event_invoke_config" "transform_trigger" {
  function_name     = aws_lambda_function.ingestion_lambda.function_name
  destination_config{
    on_success{
      destination = aws_lambda_function.transformation_lambda.arn
    }
    on_failure{
      destination = aws_sns_topic.notification_topic.arn
    }
  }
}
resource "aws_lambda_permission" "transformation_lambda_event" {
  statement_id  = "AllowExecutionFromIngestionLambda"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.transformation_lambda.function_name
  principal     = "lambda.amazonaws.com"
  source_arn    = aws_lambda_function.ingestion_lambda.arn
}





########################
####  EVENT BRIDGE  ####
# ########################

# resource "aws_cloudwatch_event_rule" "transformation_lambda_rule" {
#   name                = "transformation_lambda_rule"
#   schedule_expression = "rate(3 minutes)"
# }

# resource "aws_cloudwatch_event_target" "transformation_lambda_target" {
#   rule      = aws_cloudwatch_event_rule.transformation_lambda_rule.name
#   target_id = "SendToLambda"
#   arn       = aws_lambda_function.transformation_lambda.arn
# }

# resource "aws_lambda_permission" "transformation_lambda_event" {
#   statement_id  = "AllowExecutionFromEventBridge"
#   action        = "lambda:InvokeFunction"
#   function_name = "transformation_lambda_handler"
#   principal     = "events.amazonaws.com"
#   source_arn    = aws_cloudwatch_event_rule.transformation_lambda_rule.arn
# }


#############################
####  ALARM AND METRICS  ####
#############################

# resource "aws_cloudwatch_log_metric_filter" "table_transformation_error_metric" {
#   name           = "table_transformation_error_metric"
#   pattern        = "TableTransformationError"
#   log_group_name = "/aws/lambda/${aws_lambda_function.transformation_lambda.function_name}"

#   metric_transformation {
#     name      = "table_transformation_error_metric"
#     namespace = "table_transformation_error_metric"
#     value     = "1"
#   }
# }

# resource "aws_cloudwatch_metric_alarm" "table_transformation_error_alarm" {
#   alarm_name                = "table_transformation_error_alarm"
#   comparison_operator       = "GreaterThanOrEqualToThreshold"
#   evaluation_periods        = 1
#   metric_name               = "table_transformation_error_metric"
#   namespace                 = "table_transformation_error_metric"
#   period                    = 60
#   statistic                 = "Sum"
#   threshold                 = "1"

#   alarm_actions = [aws_sns_topic.notification_topic.arn]
# }

# resource "aws_cloudwatch_log_metric_filter" "invalid_credentials_error_metric" {
#   name           = "invalid_credentials_error_metric"
#   pattern        = "InvalidCredentialsError"
#   log_group_name = "/aws/lambda/${aws_lambda_function.transformation_lambda.function_name}"

#   metric_transformation {
#     name      = "invalid_credentials_error_metric"
#     namespace = "invalid_credentials_error_metric"
#     value     = "1"
#   }
# }

# resource "aws_cloudwatch_metric_alarm" "invalid_credentials_error_alarm" {
#   alarm_name                = "invalid_credentials_error_alarm"
#   comparison_operator       = "GreaterThanOrEqualToThreshold"
#   evaluation_periods        = 1
#   metric_name               = "invalid_credentials_error_metric"
#   namespace                 = "invalid_credentials_error_metric"
#   period                    = 60
#   statistic                 = "Sum"
#   threshold                 = "1"

#   alarm_actions = [aws_sns_topic.notification_topic.arn]
# }

# resource "aws_cloudwatch_log_metric_filter" "transformation_end" {
#   name           = "transformation_end"
#   pattern        = "END"
#   log_group_name = "/aws/lambda/${aws_lambda_function.transformation_lambda.function_name}"

#   metric_transformation {
#     name      = "transformation_end"
#     namespace = "transformation_end"
#     value     = "1"
#   }
# }

# resource "aws_cloudwatch_log_metric_filter" "exception_error_metric" {
#   name           = "exception_error"
#   pattern        = "Exception"
#   log_group_name = "/aws/lambda/${aws_lambda_function.transformation_lambda.function_name}"

#   metric_transformation {
#     name      = "exception_Error_metric"
#     namespace = "exception_Error_metric"
#     value     = "1"
#   }
# }

# resource "aws_cloudwatch_metric_alarm" "exception_error_alarm" {
#   alarm_name                = "exception_Error_alarm"
#   comparison_operator       = "GreaterThanOrEqualToThreshold"
#   evaluation_periods        = 1
#   metric_name               = "exception_Error_metric"
#   namespace                 = "exception_Error_metric"
#   period                    = 60
#   statistic                 = "Sum"
#   threshold                 = "1"

#   alarm_actions = [aws_sns_topic.notification_topic.arn]
# }
