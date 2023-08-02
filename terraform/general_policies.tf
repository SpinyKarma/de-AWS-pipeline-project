###########################
####  POLICY CREATION  ####
###########################

data "aws_iam_policy_document" "s3_policy_document" {
  statement {

    actions = ["s3:*",
      "s3-object-lambda:*"
    ]

    resources = [
      "*"
    ]
  }
}

data "aws_iam_policy_document" "secret_read_policy_document" {
  statement {

    actions = ["secretsmanager:GetSecretValue"]

    resources = [
      "arn:aws:secretsmanager:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:secret:*"
    ]
  }
}

data "aws_iam_policy_document" "cw_ingestion_write_policy_document" {
  statement {

    actions = ["logs:CreateLogStream", "logs:PutLogEvents"]

    resources = [
      "${aws_cloudwatch_log_group.ingestion_log_group.arn}:*"
    ]
  }
}

data "aws_iam_policy_document" "cw_transformation_write_policy_document" {
  statement {

    actions = ["logs:CreateLogStream", "logs:PutLogEvents"]

    resources = [
      "${aws_cloudwatch_log_group.transformation_log_group.arn}:*"
    ]
  }
}

data "aws_iam_policy_document" "lambda_policy_document" {
  statement {

    actions = ["lambda:*"]

    resources = [
      "*"
    ]
  }
}

data "aws_iam_policy_document" "sns_publish_policy_document" {
  statement {

    actions = ["sns:*"]

    resources = [
      "*"
    ]
  }
}

resource "aws_iam_policy" "s3_policy" {
  name_prefix = "s3-policy-"
  policy      = data.aws_iam_policy_document.s3_policy_document.json
}

resource "aws_iam_policy" "secret_read_policy" {
  name_prefix = "secret-read-policy-"
  policy      = data.aws_iam_policy_document.secret_read_policy_document.json
}

resource "aws_iam_policy" "cw_ingestion_write_policy" {
  name_prefix = "cw-write-policy-ingestion-"
  policy      = data.aws_iam_policy_document.cw_ingestion_write_policy_document.json
}

resource "aws_iam_policy" "cw_transformation_write_policy" {
  name_prefix = "cw-write-policy-transformation-"
  policy      = data.aws_iam_policy_document.cw_transformation_write_policy_document.json
}

resource "aws_iam_policy" "lambda_execute_policy" {
  name_prefix = "lambda-execute-policy-"
  policy      = data.aws_iam_policy_document.lambda_policy_document.json
}

resource "aws_iam_policy" "sns_publish_policy" {
  name_prefix = "sns-publish-policy-"
  policy      = data.aws_iam_policy_document.sns_publish_policy_document.json
}
