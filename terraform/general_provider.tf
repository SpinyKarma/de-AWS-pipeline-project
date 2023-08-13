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
