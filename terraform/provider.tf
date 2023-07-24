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