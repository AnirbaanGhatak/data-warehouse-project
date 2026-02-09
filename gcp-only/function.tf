data "archive_file" "source_zip" {
  type        = "zip"
  source_dir  = "${path.module}/src"
  output_path = "${path.module}/src.zip"
}

resource "google_storage_bucket_object" "zip_upload" {
  # The name includes a hash of the file content!
  name   = "src-${data.archive_file.source_zip.output_base64sha256}.zip"
  bucket = google_storage_bucket.function_source.name
  source = data.archive_file.source_zip.output_path
}

resource "google_cloudfunctions2_function" "elt_function" {
  name     = "elt-processor"
  location = var.gcp_region

  build_config {
    runtime     = "python310"
    entry_point = "process_data_pipeline" # Function name inside main.py
    source {
      storage_source {
        bucket = google_storage_bucket.function_source.name
        object = google_storage_bucket_object.zip_upload.name
      }
    }
  }

  service_config {
    max_instance_count = 1 # Keep it simple for now (Avoid race conditions)
    available_memory   = "512M"
    timeout_seconds    = 300 # 5 minutes to process the file

    # SECURITY: Attach the specific ID Badge we made in iam.tf
    service_account_email = google_service_account.elt_invoker.email

    # CONFIGURATION: Inject the Project ID so Python knows where to look
    environment_variables = {
      PROJECT_ID = var.gcp_project
    }
  }

  event_trigger {
    trigger_region        = var.gcp_region
    event_type            = "google.cloud.storage.object.v1.finalized"
    service_account_email = google_service_account.elt_invoker.email

    event_filters {
      attribute = "bucket"
      value     = google_storage_bucket.data_lake.name
    }
  }

  depends_on = [google_project_iam_member.bq_data_editor, google_project_iam_member.bq_job_user, google_project_iam_member.run_invoker, google_project_iam_member.gcs_viewer]

}
