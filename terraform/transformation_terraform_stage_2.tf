##############################################################
####  TRANSFORMATION STAGE 2 ROLE AND POLICY ATTACHMENTS  ####
##############################################################


resource "aws_iam_role" "transformation_stage_2_lambda_role" {
  name_prefix        = "role-transformation_stage_2-"
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
    Lambda     = "Transformation-stage-2"
  }
}

resource "aws_iam_role_policy_attachment" "transformation_stage_2_s3_write_policy_attachment" {
  role       = aws_iam_role.transformation_stage_2_lambda_role.name
  policy_arn = aws_iam_policy.s3_policy.arn
}

resource "aws_iam_role_policy_attachment" "transformation_stage_2_cw_write_policy_attachment" {
  role       = aws_iam_role.transformation_stage_2_lambda_role.name
  policy_arn = aws_iam_policy.cw_transformation_stage_2_write_policy.arn
}

resource "aws_iam_role_policy_attachment" "transformation_stage_2_lambda_execute_policy_attachment" {
  role       = aws_iam_role.transformation_stage_2_lambda_role.name
  policy_arn = aws_iam_policy.lambda_execute_policy.arn
}

resource "aws_iam_role_policy_attachment" "transformation_stage_2_sns_publish_policy_attachment" {
  role       = aws_iam_role.transformation_stage_2_lambda_role.name
  policy_arn = aws_iam_policy.sns_publish_policy.arn
}


############################################
####  TRANSFORMATION STAGE 2 LOG GROUP  ####
############################################


resource "aws_cloudwatch_log_group" "transformation_stage_2_log_group" {
  name = "/aws/lambda/${aws_lambda_function.transformation_stage_2_lambda.function_name}"
  tags = {
    Repo       = "https://github.com/SpinyKarma/de-AWS-pipeline-project"
    Managed_by = "Terraform"
    Project    = "Northcoders-AWS-ETL-pipeline"
    Lambda     = "Transformation-stage-2"
  }
}


#########################################
####  TRANSFORMATION STAGE 2 LAMBDA  ####
#########################################


data "archive_file" "transformation_stage_2_lambda_zip" {
  type        = "zip"
  source_dir  = "${path.module}/../src/lambda_transformation_stage_2"
  output_path = "${path.module}/../lambda_transformation_stage_2.zip"
}

resource "aws_s3_object" "transformation_stage_2_lambda_code" {
  bucket      = aws_s3_bucket.code_bucket.bucket
  key         = "lambda_transformation_stage_2.zip"
  acl         = "private"
  source      = data.archive_file.transformation_stage_2_lambda_zip.output_path
  source_hash = data.archive_file.transformation_stage_2_lambda_zip.output_base64sha256
  tags = {
    Repo       = "https://github.com/SpinyKarma/de-AWS-pipeline-project"
    Managed_by = "Terraform"
    Project    = "Northcoders-AWS-ETL-pipeline"
    Lambda     = "Transformation-stage-2"
  }
}

resource "aws_lambda_function" "transformation_stage_2_lambda" {
  s3_bucket        = aws_s3_object.transformation_stage_2_lambda_code.bucket
  s3_key           = aws_s3_object.transformation_stage_2_lambda_code.key
  function_name    = "transformation_lambda_handler_stage_2"
  role             = aws_iam_role.transformation_stage_2_lambda_role.arn
  handler          = "transformation_lambda_stage_2.transformation_lambda_handler_stage_2"
  runtime          = "python3.10"
  timeout          = "60"
  source_code_hash = data.archive_file.transformation_stage_2_lambda_zip.output_base64sha256
  layers           = [aws_lambda_layer_version.lambda_requirements_layer_2.arn]
  tags = {
    Repo       = "https://github.com/SpinyKarma/de-AWS-pipeline-project"
    Managed_by = "Terraform"
    Project    = "Northcoders-AWS-ETL-pipeline"
    Lambda     = "Transformation-stage-2"
  }
}


#########################################
###  TRANSFORMATION STAGE 2 TRIGGER  ####
#########################################


resource "aws_lambda_function_event_invoke_config" "transformation_stage_2_trigger" {
  function_name = aws_lambda_function.transformation_lambda.function_name
  destination_config {
    on_success {
      destination = aws_lambda_function.transformation_stage_2_lambda.arn
    }
    on_failure {
      destination = aws_sns_topic.notification_topic.arn
    }
  }
}
resource "aws_lambda_permission" "transformation_stage_2_lambda_event" {
  statement_id  = "AllowExecutionFromTransformationLambda"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.transformation_stage_2_lambda.function_name
  principal     = "lambda.amazonaws.com"
  source_arn    = aws_lambda_function.ingestion_lambda.arn
}


##############################
####  ALARMS AND METRICS  ####
##############################


# resource "aws_cloudwatch_log_metric_filter" "metric_label" {
#   name           = "metric_name"
#   pattern        = "PatternToSearchForInLogs"
#   log_group_name = aws_cloudwatch_log_group.transformation_stage_2_log_group.name

#   metric_transformation_stage_2 {
#     name      = "metric_name"
#     namespace = "metric_name"
#     value     = "1"
#   }
# }

# resource "aws_cloudwatch_metric_alarm" "alarm_label" {
#   alarm_name          = "alarm_name"
#   comparison_operator = "GreaterThanOrEqualToThreshold"
#   evaluation_periods  = 1
#   metric_name         = aws_cloudwatch_log_metric_filter.metric_label.metric_transformation_stage_2[0].name
#   namespace           = aws_cloudwatch_log_metric_filter.metric_label.metric_transformation_stage_2[0].namespace
#   period              = 60
#   statistic           = "Sum"
#   threshold           = "1"
#   alarm_actions       = [aws_sns_topic.notification_topic.arn]
#   tags = {
#     Repo       = "https://github.com/SpinyKarma/de-AWS-pipeline-project"
#     Managed_by = "Terraform"
#     Project    = "Northcoders-AWS-ETL-pipeline"
#     Lambda     = "Transformation-stage-2"
#   }
# }
