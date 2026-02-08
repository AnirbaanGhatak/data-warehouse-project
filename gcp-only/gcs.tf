# 1. CREATE IDENTITY (The "ID Badge")
resource "google_service_account" "elt_invoker" {
  account_id   = "elt-invoker-sa"
  display_name = "ELT Cloud Function Service Account"
  description  = "Identity for the Event-Driven ELT Pipeline"
}

# 2. STORAGE PERMISSIONS (Read Only)
# Allows reading the CSV from the Landing Zone
resource "google_project_iam_member" "gcs_viewer" {
  project = var.gcp_project
  role    = "roles/storage.objectViewer"
  member  = "serviceAccount:${google_service_account.elt_invoker.email}"
}

# 3. BIGQUERY DATA PERMISSIONS (Read/Write Data)
# Allows creating Tables, inserting rows, and reading data
# DOES NOT allow deleting Datasets or changing access controls
resource "google_project_iam_member" "bq_data_editor" {
  project = var.gcp_project
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.elt_invoker.email}"
}

# 4. BIGQUERY JOB PERMISSIONS (Execute)
# Allows the account to actually "Run" a Query or Load Job
resource "google_project_iam_member" "bq_job_user" {
  project = var.gcp_project
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.elt_invoker.email}"
}

# 5. CLOUD RUN INVOKER
# Eventarc needs permission to "invoke" the underlying Cloud Run service
resource "google_project_iam_member" "run_invoker" {
  project = var.gcp_project
  role    = "roles/run.invoker"
  member  = "serviceAccount:${google_service_account.elt_invoker.email}"
}