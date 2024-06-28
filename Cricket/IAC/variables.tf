variable "project_id" {
  type        = string
  description = "project_id on GCP"
}
variable "env" {
  type        = string
  description = "current environment"
}

variable "dataset" {
  type        = string
  description = "project dataset name on GCP"
}
variable "bucket_name" {
  type =  string
  description = "cloud storage bucket to store data to be loaded to BQ"
}
variable "service_account" {
  type = string
  description = "service account to interact with cloud services"
  
}
variable "region" {}
variable "zone" {}
variable "email" {}