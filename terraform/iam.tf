###########################
####  POLICY CREATION  ####
###########################

data "aws_iam_policy_document" "s3_write_policy_document" {
  statement {

    actions = ["s3:PutObject"]

    resources = [
      #"${aws_s3_bucket.@@@.arn}/*", #replace @@@ with ingestion bucket label then uncomment
    ]
  }
}

data "aws_iam_policy_document" "secret_read_policy_document" {
  statement {

    actions = ["secretsmanager:GetSecretValue"]

    resources = [
      "arn:aws:secretsmanager:eu-west-2:967289384825:secret:*"
    ]
  }
}

data "aws_iam_policy_document" "cw_write_policy_document" {
  statement {

    actions = ["logs:CreateLogGroup"]

    resources = [
      "arn:aws:logs:eu-west-2:967289384825:*"
    ]
  }

  statement {

    actions = ["logs:CreateLogStream", "logs:PutLogEvents"]

    resources = [
      "arn:aws:logs:eu-west-2:967289384825:log-group:/aws/lambda/${var.lambda_name}:*"
    ]
  }
}

resource "aws_iam_policy" "s3_write_policy" {
  name_prefix = "s3-write-policy-${var.lambda_name}" #change to actual lambda name when lambda is deployed
  policy      = data.aws_iam_policy_document.s3_write_policy_document.json
}

resource "aws_iam_policy" "secret_read_policy" {
  name_prefix = "secret-read-policy-${var.lambda_name}" #change to actual lambda name when lambda is deployed
  policy      = data.aws_iam_policy_document.secret_read_policy_document.json
}

resource "aws_iam_policy" "cw_write_policy" {
  name_prefix = "cw-write-policy-${var.lambda_name}" #change to actual lambda name when lambda is deployed
  policy      = data.aws_iam_policy_document.cw_write_policy_document.json
}

############################
####  INGESTION LAMBDA  ####
############################

resource "aws_iam_role" "ingestion_lambda_role" {
  name_prefix        = "role-${var.lambda_name}" #change to actual lambda name when lambda is deployed
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