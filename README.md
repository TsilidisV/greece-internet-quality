## Getting Started
- we need the key of job-etl-sa inside transform



select project, write it in variables
get keys for the etl job, put them in ./transform/.secrets 
winget install jqlang.jq

### Prerequisites
- [Google Cloud CLI](https://docs.cloud.google.com/sdk/docs/install-sdk)
- [Terraform](https://developer.hashicorp.com/terraform/install)
- [Make](https://www.gnu.org/software/make/)

### Infrastructure
```bash
make infra-up
```
Logs you in to gcloud cli and build infrastructure by creating a service account for uploading data to the lake, a service account for reading the lake and writing to BigQuery, a GCP bucket, a BigQuery dataset, and a Google Cloud Run Job.

### Ingest
```bash
make create-ingestor-key
```
downloads service account key for ingest with writing to bucket permissions and creates a .env file with INGESTOR_GCP_KEY and GCS_BUCKET_NAME which are read in python.