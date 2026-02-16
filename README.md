## Getting Started
- we need the key of job-etl-sa inside transform



select project, write it in variables
get keys for the etl job, put them in ./transform/.secrets 
winget install jqlang.jq


## Ingest
make create-ingestor-key: downloads service account key for ingest with writing to bucket permissions and creates a .env file with INGESTOR_GCP_KEY and GCS_BUCKET_NAME which are read in python.