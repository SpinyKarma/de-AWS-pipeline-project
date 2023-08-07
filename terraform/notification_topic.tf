resource "aws_sns_topic" "notification_topic" {
  name = "error_notification"
  tags = {
    Project = "Northcoders-AWS-ETL-pipeline"
  }
}

resource "aws_sns_topic_subscription" "user_updates_sqs_target" {
  topic_arn = aws_sns_topic.notification_topic.arn
  protocol  = "email"
  endpoint  = var.sns_error_email
}
