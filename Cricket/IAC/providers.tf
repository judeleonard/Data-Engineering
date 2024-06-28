provider "google" {
  credentials = file("${path.module}/config/config.json")
  project = var.project_id
  region  = "${var.region}"
  zone    = "${var.zone}"
}
