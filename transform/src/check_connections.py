import os
import sys
import logging
import google.auth
from google.cloud import storage
from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import Forbidden, NotFound

# Configure Logging (Better than print for Cloud Logs)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def get_credentials():
    key_path = os.getenv("INGESTOR_GCP_KEY")
    
    # CASE 1: Local Docker / Development
    if key_path and os.path.exists(key_path):
        logger.info(f"🔑 FOUND KEY: Using Service Account Key file at: {key_path}")
        return service_account.Credentials.from_service_account_file(key_path)
    
    # CASE 2: GCP Environment (Cloud Run, GKE, etc.)
    else:
        logger.info("☁️  NO KEY FOUND: Attempting Application Default Credentials (ADC)...")
        credentials, project = google.auth.default(
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        logger.info(f"✅ Loaded ADC for project: {project}")
        return credentials

def check_permissions():
    # Load targets
    bucket_name = os.getenv("GCS_BUCKET_NAME")
    dataset_id = os.getenv("BQ_DATASET")

    if not all([bucket_name, dataset_id]):
        logger.error("❌ Critical Error: Missing GCS_BUCKET_NAME or BQ_DATASET environment variables.")
        sys.exit(1) # <--- Container will exit with error code 1

    # Authenticate
    try:
        credentials = get_credentials()
    except Exception as e:
        logger.error(f"❌ Authentication Critical Failure: {e}")
        sys.exit(1)

    # --- Check GCS Access ---
    logger.info(f"Checking GCS Bucket Access ({bucket_name})...")
    try:
        storage_client = storage.Client(credentials=credentials)
        bucket = storage_client.bucket(bucket_name)
        blobs = list(bucket.list_blobs(max_results=1))
        logger.info(f"✅ GCS Success: Service account can access '{bucket_name}'.")
        
    except Exception as e:
        logger.error(f"❌ GCS Permission Failed: {e}")
        sys.exit(1) # <--- Stop here, do not continue

    # --- Check BigQuery Access ---
    logger.info(f"Checking BigQuery Dataset Access ({dataset_id})...")
    try:
        project_id = credentials.project_id if hasattr(credentials, "project_id") else None
        bq_client = bigquery.Client(credentials=credentials, project=project_id)
        tables = list(bq_client.list_tables(dataset_id, max_results=1))
        logger.info(f"✅ BigQuery Success: Service account can access '{dataset_id}'.")
        
    except Exception as e:
        logger.error(f"❌ BigQuery Permission Failed: {e}")
        sys.exit(1) # <--- Stop here

if __name__ == "__main__":
    check_permissions()