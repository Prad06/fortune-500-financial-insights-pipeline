# Change these variables
variable "project" {
  default = "data-engineering-finance"
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default     = "us-east1"
  type        = string
}

# service account syntax: GCP_SERVICE_ACCOUNT_USER@GCP_PROJECT_ID.iam.gserviceaccount.com
variable "service_account" {
  description = "Name of service account"
  type        = string
  default     = "speaking-agent-de@data-engineering-finance.iam.gserviceaccount.com"
}

# Do not change the following
locals {
  data_lake_bucket = "finance-raw-ingest"
  dataproc_bucket  = "finance-spark-staging"
}

variable "credentials" {
  description = "path to the correct google service account"
  default     = "../.google/credentials/google_credentials.json"
  type        = string
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default     = "STANDARD"
}

variable "BQ_DATASET" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type        = string
  default     = "finance_data_all"
}

variable "DATAPROC_CLUSTER" {
  description = "Name of dataproc cluster for spark jobs"
  type        = string
  default     = "finance-spark-cluster"
}