resource "google_bigquery_dataset" "bq_dataset" {
  dataset_id                  = "${var.dataset}"
  friendly_name               = "test"
  description                 = "This is the dataset for cricket project"
  location                    = "EU"
  default_table_expiration_ms = 3600000

  labels = {
    env = "${var.env}"
  }

}

# create cloud bucket
resource "google_storage_bucket" "my_bucket" {
  name          = var.bucket_name
  location      = var.region
  force_destroy = true            

  uniform_bucket_level_access = true

}