#############################################################
####  TRANSFORMATION LAMBDA ROLE AND POLICY ATTACHMENTS  ####
#############################################################


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
  tags = {
    Project = "Northcoders-AWS-ETL-pipeline"
    Lambda  = "Transformation"
  }
}

resource "aws_iam_role_policy_attachment" "transformation_s3_write_policy_attachment" {
  role       = aws_iam_role.transformation_lambda_role.name
  policy_arn = aws_iam_policy.s3_policy.arn
}

resource "aws_iam_role_policy_attachment" "transformation_cw_write_policy_attachment" {
  role       = aws_iam_role.transformation_lambda_role.name
  policy_arn = aws_iam_policy.cw_transformation_write_policy.arn
}

resource "aws_iam_role_policy_attachment" "transformation_lambda_execute_policy_attachment" {
  role       = aws_iam_role.transformation_lambda_role.name
  policy_arn = aws_iam_policy.lambda_execute_policy.arn
}

resource "aws_iam_role_policy_attachment" "transformation_sns_publish_policy_attachment" {
  role       = aws_iam_role.transformation_lambda_role.name
  policy_arn = aws_iam_policy.sns_publish_policy.arn
}


####################################
####  TRANSFORMATION LOG GROUP  ####
####################################


resource "aws_cloudwatch_log_group" "transformation_log_group" {
  name = "/aws/lambda/${aws_lambda_function.transformation_lambda.function_name}"
  tags = {
    Project = "Northcoders-AWS-ETL-pipeline"
    Lambda  = "Transformation"
  }
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
  tags = {
    Project = "Northcoders-AWS-ETL-pipeline"
    Lambda  = "Transformation"
  }
}

resource "aws_lambda_function" "transformation_lambda" {
  s3_bucket        = aws_s3_object.transformation_lambda_code.bucket
  s3_key           = aws_s3_object.transformation_lambda_code.key
  function_name    = "transformation_lambda_handler"
  role             = aws_iam_role.transformation_lambda_role.arn
  handler          = "transformation_lambda.transformation_lambda_handler"
  runtime          = "python3.10"
  timeout          = "60"
  source_code_hash = data.archive_file.transformation_lambda_zip.output_base64sha256
  layers           = [aws_lambda_layer_version.lambda_requirements_layer.arn]
  tags = {
    Project = "Northcoders-AWS-ETL-pipeline"
    Lambda  = "Transformation"
  }
}


################################
###  TRANSFORMATION TRIGGER  ###
################################


resource "aws_lambda_function_event_invoke_config" "transformation_trigger" {
  function_name = aws_lambda_function.ingestion_lambda.function_name
  destination_config {
    on_success {
      destination = aws_lambda_function.transformation_lambda.arn
    }
    on_failure {
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


##############################
####  ALARMS AND METRICS  ####
##############################


resource "aws_cloudwatch_log_metric_filter" "transformation_missing_bucket_error_metric" {
  name           = "metric_name"
  pattern        = "Missing Bucket Error"
  log_group_name = aws_cloudwatch_log_group.transformation_log_group.name

  metric_transformation {
    name      = "missing_bucket_metric"
    namespace = "missing_bucket_metric"
    value     = "1"
  }
}

resource "aws_cloudwatch_metric_alarm" "transformation_missing_bucket_error_alarm" {
  alarm_name          = "missing_bucket_error_alarm"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "missing_bucket_metric"
  namespace           = "missing_bucket_metric"
  period              = 60
  statistic           = "Sum"
  threshold           = "1"
  alarm_actions       = [aws_sns_topic.notification_topic.arn]
  tags = {
    Project = "Northcoders-AWS-ETL-pipeline"
    Lambda  = "Transformation"
  }
}

