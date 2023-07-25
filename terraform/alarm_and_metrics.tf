
# these are setup as examples for future manipulation


# resource "aws_cloudwatch_log_metric_filter" "three_error" {
#   name           = "MyAppAccessCount"
#   pattern        = "ERROR"
#   log_group_name = "/aws/lambda/mistaker-test"

#   metric_transformation {
#     name      = "EventCount"
#     namespace = "YourNamespace"
#     value     = "1"
#   }
# }
# # resource "aws_cloudwatch_log_group" "dada" {
# #   name = "MyApp/access.log"
# # }

# resource "aws_cloudwatch_metric_alarm" "alert_errors" {
#   alarm_name                = "terraform-test-foobar5"
#   comparison_operator       = "GreaterThanOrEqualToThreshold"
#   evaluation_periods        = "2"
#   metric_name               = "EventCount"
#   namespace                 = "AWS/EC2"
#   period                    = "120"
#   statistic                 = "Sum"
#   threshold                 = "1"
#   alarm_description         = "This metric monitors ec2 cpu utilization"
#   insufficient_data_actions = []
#   alarm_actions = [aws_sns_topic.notification_topic.arn]
#   treat_missing_data = "ignore"
# }