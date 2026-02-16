output "GOOGLE_CLOUD_PROJECT" {
  value = var.project_id
}

output "GCS_BUCKET" {
  value = google_storage_bucket.data_lake.name
}

output "BQ_DATASET" {
  value = google_bigquery_dataset.warehouse.dataset_id
}

output "INGESTOR_SA_EMAIL" {
  description = "The email of the Ingestor Service Account"
  value       = google_service_account.ingestor_sa.email
}