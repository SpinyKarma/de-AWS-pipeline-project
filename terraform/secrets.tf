resource "aws_secretsmanager_secret" "ingestion_secret" {
  name = "Ingestion_credentials"
  tags = {
    Repo       = "https://github.com/SpinyKarma/de-AWS-pipeline-project"
    Managed_by = "Terraform"
    Project    = "Northcoders-AWS-ETL-pipeline"
  }
}

resource "aws_secretsmanager_secret_version" "ingestion_secret_version" {
  secret_id     = aws_secretsmanager_secret.ingestion_secret.id
  secret_string = jsonencode(var.ingestion_db_credentials)
}



import {
  to = aws_secretsmanager_secret.warehouse_secret
  id = "arn:aws:secretsmanager:eu-west-2:967289384825:secret:Warehouse_credentials-lCICwR"
}

import {
  to = aws_secretsmanager_secret_version.warehouse_secret_version
  id = "arn:aws:secretsmanager:eu-west-2:967289384825:secret:Warehouse_credentials-lCICwR|ad8fe2d9-157f-4715-babc-d1c327bba563"
}

resource "aws_secretsmanager_secret" "warehouse_secret" {
  name = "Warehouse_credentials"
  tags = {
    Repo       = "https://github.com/SpinyKarma/de-AWS-pipeline-project"
    Managed_by = "Terraform"
    Project    = "Northcoders-AWS-ETL-pipeline"
  }
}

resource "aws_secretsmanager_secret_version" "warehouse_secret_version" {
  secret_id     = aws_secretsmanager_secret.warehouse_secret.id
  secret_string = jsonencode(var.data_warehouse_db_credentials)
}
