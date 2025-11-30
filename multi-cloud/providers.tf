terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "6.23.0"
    }
    google = {
      source  = "hashicorp/google"
      version = "7.12.0"
    }
  }
}

# Configure AWS (Ingestion Side)
provider "aws" {
  region = var.aws-region 
}

# Configure GCP (Analytics Side)
provider "google" {
  project = var.gcp-project_name
  region  = var.gcp-project_region           
}