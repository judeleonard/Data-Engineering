resource "google_project_iam_member" "bq_iam" {
  project = var.project_id
  role    = "roles/bigquery.dataOwner"
  member  = "serviceAccount:${var.email}"

}

resource "google_storage_bucket_iam_member" "bucket_iam" {
  bucket = google_storage_bucket.my_bucket.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${var.email}"
}
