###########################
####  POLICY CREATION  ####
###########################

data "aws_iam_policy_document" "s3_write_policy_document" {
  statement {

    actions = ["s3:PutObject"]

    resources = [
      "${aws_s3_bucket.raw_csv_data_bucket.arn}/*",
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

resource "aws_iam_policy" "s3_write_policy" {
  name_prefix = "s3-write-policy-ingestion-"
  policy      = data.aws_iam_policy_document.s3_write_policy_document.json
}

resource "aws_iam_policy" "secret_read_policy" {
  name_prefix = "secret-read-policy-ingestion-"
  policy      = data.aws_iam_policy_document.secret_read_policy_document.json
}

resource "aws_iam_policy" "cw_write_policy" {
  name_prefix = "cw-write-policy-ingestion-"
  policy      = data.aws_iam_policy_document.cw_ingestion_write_policy_document.json
}

############################
####  INGESTION LAMBDA  ####
############################

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
  policy_arn = aws_iam_policy.s3_write_policy.arn
}

resource "aws_iam_role_policy_attachment" "ingestion_secret_read_policy_attachment" {
  role       = aws_iam_role.ingestion_lambda_role.name
  policy_arn = aws_iam_policy.secret_read_policy.arn
}

resource "aws_iam_role_policy_attachment" "ingestion_cw_write_policy_attachment" {
  role       = aws_iam_role.ingestion_lambda_role.name
  policy_arn = aws_iam_policy.cw_write_policy.arn
}