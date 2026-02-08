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

# 1. THE DATA LAKE (Raw Data)
resource "google_storage_bucket" "data_lake" {
  name          = var.bucket_name
  location      = var.gcp_region
  storage_class = "STANDARD"
  uniform_bucket_level_access = true
  force_destroy = true # For dev only
}

# 2. FUNCTION SOURCE BUCKET (Stores the Zipped Python Code)
resource "google_storage_bucket" "function_source" {
  name          = "${var.bucket_name}-source"
  location      = var.gcp_region
  uniform_bucket_level_access = true
  force_destroy = true
}

# resource "google_storage_folder" "raw_crm" {
#   bucket = google_storage_bucket.data-lake.name
#   name = "raw_crm/"
# }

# resource "google_storage_folder" "raw_erp" {
#   bucket = google_storage_bucket.data-lake.name
#   name = "raw_erp/"
# }

# resource "google_storage_bucket_object" "crm_files" {
#   for_each = fileset("${path.module}/../datasets/crm", "**")

#   name = "raw_crm/${each.value}"

#   source = "${path.module}/../datasets/crm/${each.value}"

#   bucket = google_storage_bucket.data-lake.name
# }

# resource "google_storage_bucket_object" "erp_files" {
#   for_each = fileset("${path.module}/../datasets/erp", "**")

#   name = "raw_erp/${each.value}"

#   source = "${path.module}/../datasets/erp/${each.value}"

#   bucket = google_storage_bucket.data-lake.name
# }

