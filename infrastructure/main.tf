terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 5.0.0"
    }
    time = {
      source  = "hashicorp/time"
      version = ">= 0.9.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# --------------------------------------------------------------------------------
# 1. Enable Required APIs
# --------------------------------------------------------------------------------
resource "google_project_service" "enabled_apis" {
  for_each = toset([
    "run.googleapis.com",
    "compute.googleapis.com",
    "cloudscheduler.googleapis.com",
    "bigquery.googleapis.com",
    "storage.googleapis.com",
    "iam.googleapis.com"
  ])
  service            = each.key
  disable_on_destroy = false
}

# --------------------------------------------------------------------------------
# 2. Service Accounts & Security
# --------------------------------------------------------------------------------
# Create a dedicated Service Account for the ETL Job
resource "google_service_account" "etl_sa" {
  account_id   = "etl-job-sa"
  display_name = "ETL Job Service Account"
  depends_on   = [google_project_service.enabled_apis]
}

resource "time_sleep" "wait_for_sa" {
  depends_on = [google_service_account.etl_sa]
  create_duration = "30s"
}

# --------------------------------------------------------------------------------
# 3. Data Lake (GCS)
# --------------------------------------------------------------------------------
resource "google_storage_bucket" "data_lake" {
  name          = var.bucket_name
  location      = var.region
  force_destroy = true # Allows deleting bucket even if it has files (careful!)

  uniform_bucket_level_access = true
  storage_class               = "STANDARD" # Required for Free Tier

  depends_on = [google_project_service.enabled_apis]
}

# Grant the ETL Service Account access to read/write GCS
resource "google_storage_bucket_iam_member" "sa_gcs_admin" {
  bucket = google_storage_bucket.data_lake.name
  role   = "roles/storage.admin"
  member = "serviceAccount:${google_service_account.etl_sa.email}"
}

# --------------------------------------------------------------------------------
# 4. Data Warehouse (BigQuery)
# --------------------------------------------------------------------------------
resource "google_bigquery_dataset" "warehouse" {
  dataset_id  = var.dataset_id
  location    = var.region
  description = "Medallion architecture warehouse"

  depends_on = [google_project_service.enabled_apis]
}

# Grant the ETL Service Account access to write to BigQuery
resource "google_project_iam_member" "sa_bq_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.etl_sa.email}"
}

resource "google_project_iam_member" "sa_bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.etl_sa.email}"
}

# --------------------------------------------------------------------------------
# 5. Cloud Run Job (The ETL Runner)
# --------------------------------------------------------------------------------
resource "google_cloud_run_v2_job" "etl_job" {
  name     = var.job_name
  location = var.region

  template {
    template {
      service_account = google_service_account.etl_sa.email
      
      containers {
        image = var.docker_image
        
        # Optimization for Free Tier:
        # Keep resources just high enough for Spark/Pandas but low enough to save credits.
        resources {
          limits = {
            cpu    = "1"
            memory = "1Gi" 
          }
        }
        
        # Pass environment variables if your code needs them
        env {
          name  = "GCS_BUCKET"
          value = google_storage_bucket.data_lake.name
        }
        env {
          name  = "BQ_DATASET"
          value = google_bigquery_dataset.warehouse.dataset_id
        }
      }
    }
  }

  depends_on = [
    google_project_service.enabled_apis,
    time_sleep.wait_for_sa
  ]
}

# --------------------------------------------------------------------------------
# 6. Cloud Scheduler (The Trigger)
# --------------------------------------------------------------------------------
# Create a Service Account specifically for the Scheduler to invoke Cloud Run
resource "google_service_account" "scheduler_sa" {
  account_id   = "scheduler-invoker"
  display_name = "Cloud Scheduler Invoker SA"
  depends_on   = [google_project_service.enabled_apis]
}

# Allow the Scheduler SA to invoke the Cloud Run Job
resource "google_cloud_run_v2_job_iam_member" "scheduler_invoke_permissions" {
  name     = google_cloud_run_v2_job.etl_job.name
  location = google_cloud_run_v2_job.etl_job.location
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.scheduler_sa.email}"
}

resource "google_cloud_scheduler_job" "daily_trigger" {
  name             = "trigger-etl-daily"
  description      = "Triggers the ETL Cloud Run Job once daily"
  schedule         = "0 8 * * *" # Runs at 8:00 AM UTC daily
  time_zone        = "Etc/UTC"
  attempt_deadline = "320s"
  region           = var.region

  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/${var.job_name}:run"

    oauth_token {
      service_account_email = google_service_account.scheduler_sa.email
    }
  }

  depends_on = [google_cloud_run_v2_job.etl_job]
}

# --------------------------------------------------------------------------------
# 7. Ingestion Identity (For GitHub Actions)
# --------------------------------------------------------------------------------

# Create the Service Account for the Ingestor
resource "google_service_account" "ingestor_sa" {
  account_id   = "ingestor-sa"
  display_name = "Ingestor Service Account (GitHub Actions)"
}

# Grant the Ingestor permission to upload to the Data Lake
# "objectAdmin" allows writing, overwriting, and deleting files in this specific bucket
resource "google_storage_bucket_iam_member" "ingestor_gcs_writer" {
  bucket = google_storage_bucket.data_lake.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.ingestor_sa.email}"
}