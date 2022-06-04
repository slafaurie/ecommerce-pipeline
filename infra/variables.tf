locals {
    data_lake_bucket = "sl-olist-ecommerce"
}

variable "project" {
    description = "GCP project ID"

}

variable "region" {
    description = "default"
    default = "europe-west3"
    type = string
}

variable "storage_class" {
    description = "Storage class type for you bucket"
    default = "STANDARD"
}

variable "BQ_DATASET" {
    description = "BigQuery dataset that raw data from GCS will be written to"
    default = "olist_ecommerce"
    type = string
}