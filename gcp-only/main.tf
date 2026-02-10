provider "google" {
  project = var.gcp_project
  impersonate_service_account = "terraform-gcp@multi-cloud-warehouse.iam.gserviceaccount.com"
}

resource "google_storage_bucket" "data-lake" {
  name          = var.bucket_name
  location      = var.gcp_region
  storage_class = "STANDARD"

  uniform_bucket_level_access = true
  hierarchical_namespace {
    enabled = true
  }

  force_destroy = true
}

# 2. FUNCTION SOURCE BUCKET (Stores the Zipped Python Code)
resource "google_storage_bucket" "function_source" {
  name          = "${var.bucket_name}-source"
  location      = var.gcp_region
  uniform_bucket_level_access = true
  force_destroy = true
}

resource "google_storage_bucket_object" "crm_folder" {

  name = "raw_crm/"
  content = " "

  bucket = google_storage_bucket.data-lake.name
}

resource "google_storage_bucket_object" "erp_folder" {

  name = "raw_erp/"
  content = " "

  bucket = google_storage_bucket.data-lake.name
}

