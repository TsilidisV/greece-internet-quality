variable "project_id" {
  description = "The GCP Project ID"
  type        = string
  default     = "new-test-owwowow"
}

variable "region" {
  description = "The GCP region for resources (Must be us-central1, us-west1, or us-east1 for Free Tier GCS)"
  type        = string
  default     = "us-east1"
}

variable "bucket_name" {
  description = "Globally unique name for the GCS bucket"
  type        = string
  default     = "a-unique-bucket-anaaaame-a-wowow"
}

variable "dataset_id" {
  description = "The BigQuery Dataset ID"
  type        = string
  default     = "data_warehouse"
}

variable "docker_image" {
  description = "The Docker Hub image URL (e.g., docker.io/username/image:tag)"
  type        = string
  default     = "docker.io/vtsilidis/spark-example4:latest"
}

variable "job_name" {
  description = "Name of the Cloud Run Job"
  type        = string
  default     = "daily-etl-job"
}