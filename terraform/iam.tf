##########################
####  POLICY CREATION ####
##########################

data "aws_iam_policy_document" "s3_read_policy_document" {
  statement {

    actions = ["s3:GetObject"]

    resources = [
      "${aws_s3_bucket.code_bucket.arn}/*",
      "${aws_s3_bucket.data_bucket.arn}/*",
    ]
  }
}

data "aws_iam_policy_document" "cw_write_policy_document" {
  statement {

    actions = ["logs:CreateLogGroup"]

    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*"
    ]
  }

  statement {

    actions = ["logs:CreateLogStream", "logs:PutLogEvents"]

    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${var.lambda_name}:*"
    ]
  }
}

resource "aws_iam_policy" "s3_read_policy" {
  name_prefix = "s3-read-policy-${var.lambda_name}" #change to actual lambda name when lambda is deployed
  policy      = data.aws_iam_policy_document.s3_read_policy_document.json
}


resource "aws_iam_policy" "cw_write_policy" {
  name_prefix = "cw-write-policy-${var.lambda_name}" #change to actual lambda name when lambda is deployed
  policy      = data.aws_iam_policy_document.cw_write_policy_document.json
}

###########################
####  INGESTION LAMBDA ####
###########################

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

resource "aws_iam_role_policy_attachment" "ingestion_s3_read_policy_attachment" {
  role       = aws_iam_role.ingestion_lambda_role.name
  policy_arn = aws_iam_policy.s3_read_policy.arn
}

resource "aws_iam_role_policy_attachment" "ingestion_cw_write_policy_attachment" {
  role       = aws_iam_role.ingestion_lambda_role.name
  policy_arn = aws_iam_policy.cw_write_policy.arn
}