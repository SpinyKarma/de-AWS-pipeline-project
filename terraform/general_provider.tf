####################
####  PROVIDER  ####
####################


terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "5.7.0"
    }
  }

  backend "s3" {
    bucket = "terraform-backend-95866733950235"
    key    = "terraform.tfstate"
    region = "eu-west-2"
  }
}

provider "aws" {
  region = "eu-west-2"
}


################
####  VARS  ####
################


data "aws_caller_identity" "current" {

}

data "aws_region" "current" {

}

variable "sns_error_email" {
  description = "The email address to send alarm and error notifications to."
  type        = string
  default     = "de.project.banana@gmail.com"
}

variable "ingestion_db_credentials" {
  description = "The postgreSQL credentials for the database that data is being drawn from."
  default = {
    hostname = "nc-data-eng-totesys-production.chpsczt8h1nu.eu-west-2.rds.amazonaws.com"
    port     = "5432"
    db       = "totesys"
    username = "project_user_5"
    password = "FiA0ooxIw4ojnmcJmc8VwrWm"
  }
  type = map(string)
}


variable "data_warehouse_db_credentials" {
  description = "The postgreSQL credentials for the data warehouse that data is being sent to."
  default = {
    hostname = "nc-data-eng-project-dw-prod.chpsczt8h1nu.eu-west-2.rds.amazonaws.com"
    port     = "5432"
    schema   = "project_team_5"
    username = "project_team_5"
    password = "62qE8rzHh8JQmCT"
  }
  type = map(string)
}
