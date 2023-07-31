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

resource "aws_iam_policy" "s3_policy" {
  name_prefix = "s3-policy-"
  policy      = data.aws_iam_policy_document.s3_policy_document.json
}

resource "aws_iam_policy" "secret_read_policy" {
  name_prefix = "secret-read-policy-ingestion-"
  policy      = data.aws_iam_policy_document.secret_read_policy_document.json
}

resource "aws_iam_policy" "cw_write_policy" {
  name_prefix = "cw-write-policy-ingestion-"
  policy      = data.aws_iam_policy_document.cw_ingestion_write_policy_document.json
}