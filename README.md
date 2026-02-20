## Getting Started

### Prerequisites
- [Google Cloud CLI](https://docs.cloud.google.com/sdk/docs/install-sdk)
- [Terraform](https://developer.hashicorp.com/terraform/install)
- [Make](https://www.gnu.org/software/make/)

### Infrastructure
Edit `infrastructure/variables.tf` with your desired project ID, bucket name, dataset name, etc.

```bash
make infra-up
```
Logs you in to gcloud cli and build infrastructure by creating a service account for uploading data to the lake, a service account for reading the lake and writing to BigQuery, a GCP bucket, a BigQuery dataset, and a Google Cloud Run Job.

### Ingest
```bash
make create-ingestor-key
```
downloads service account key for ingest with writing to bucket permissions and creates a `.env` file with INGESTOR_GCP_KEY and GCS_BUCKET_NAME which are read in python.

```python
uv run ingest/main.py daily
```
uploads files to the GCS bucket.

To have automated ingestion through github actions add an `INGESTOR_GCP_KEY` and a `GCS_BUCKET_NAME` secret to your forked github repository, with the contents of the `transform-gcp-key.json` and your chosen name of the GCS bucket, respectively.

### Transform
```bash
make create-transform-key
```
downloads service account key for load and transform with reading to bucket and writing to dataset permissions and creates a `.env` file with TRANSFORM_GCP_KEY, GCS_BUCKET_NAME and BQ_DATASET which are read in the `docker-compose.yml`.

```bash
make docker-run
```
Builds and locally runs the dockerfile

```bash
make image-push
```
Builds and pushes image to docker hub