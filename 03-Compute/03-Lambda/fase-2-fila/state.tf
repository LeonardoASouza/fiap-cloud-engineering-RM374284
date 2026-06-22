provider "aws" {
  region = "us-east-1"
}
terraform {
  backend "s3" {
    bucket = "base-config-SEU_RM"
    key    = "compute/lambda/fase-2/terraform.tfstate"
    region = "us-east-1"
  }
}
