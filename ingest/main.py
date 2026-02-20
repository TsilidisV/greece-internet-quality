import io
import logging
import os
from datetime import datetime, timedelta

import pandas as pd
import requests
import typer
from dotenv import load_dotenv
from google.cloud import storage
from google.oauth2 import service_account
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

# --- Setup & Configuration ---
load_dotenv()

KEY_PATH = os.getenv("INGESTOR_GCP_KEY")
BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
API_URL = "https://data.gov.gr/api/v1/query/hyperion"

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

app = typer.Typer()

# --- Helper Functions ---


def get_storage_client():
    """
    Initializes the GCS client.
    - Locally: Uses the explicit JSON key path from .env.
    - GitHub Actions: Uses Application Default Credentials (ADC) 
      configured by the 'google-github-actions/auth' step.
    """
    try:
        # 1. Check if we are running locally with a specific key path
        if KEY_PATH:
            logger.info(f"Authenticating with local key file: {KEY_PATH}")
            credentials = service_account.Credentials.from_service_account_file(KEY_PATH)
            return storage.Client(credentials=credentials)
        
        # 2. Otherwise, assume we are in a secure environment (GitHub Actions)
        # The client will automatically find credentials set by the auth action.
        logger.info("Authenticating with Application Default Credentials (ADC)")
        return storage.Client()
        
    except Exception as e:
        logger.error(f"Failed to initialize GCS client: {e}")
        raise


def upload_parquet_to_gcp(df: pd.DataFrame, destination_blob_name: str):
    """
    Uploads a Pandas DataFrame as a parquet file to GCS.
    """
    try:
        client = get_storage_client()
        bucket = client.bucket(BUCKET_NAME)
        blob = bucket.blob(destination_blob_name)

        # Convert DataFrame to in-memory Parquet buffer
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False, engine="pyarrow")
        buffer.seek(0)  # Reset buffer position

        blob.upload_from_file(buffer, content_type="application/octet-stream")
        logger.info(
            f"Successfully uploaded: gs://{BUCKET_NAME}/{destination_blob_name}"
        )

    except Exception as e:
        logger.error(f"Error uploading {destination_blob_name} to GCP: {e}")
        raise


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    before_sleep=lambda retry_state: logger.warning(
        f"Request failed, retrying... (Attempt {retry_state.attempt_number})"
    ),
)
def fetch_data_with_retry(date_from: str, date_to: str):
    """
    Fetches data from the API with exponential backoff retry logic.
    """
    params = {"date_from": date_from, "date_to": date_to}

    response = requests.get(API_URL, params=params)
    response.raise_for_status()  # Trigger retry on 4xx/5xx errors
    return response.json()


def process_and_upload_day(target_date_str: str):
    """
    Orchestrates fetching, processing, and uploading for a single day.
    """
    logger.info(f"Processing data for date: {target_date_str}")

    try:
        # 1. Fetch Data
        data = fetch_data_with_retry(target_date_str, target_date_str)

        if not data:
            logger.warning(f"No data found for {target_date_str}. Skipping.")
            return

        # 2. Process Data (Convert to DF, Add Timestamp)
        df = pd.DataFrame(data)

        # Add ingestion timestamp
        df["ingested_at"] = datetime.utcnow()

        # Ensure date columns are proper datetime objects (optional but recommended for Parquet)
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])

        # 3. Generate Hive-style Path
        # Parse the date to extract year, month, day
        date_obj = datetime.strptime(target_date_str, "%Y-%m-%d")
        hive_path = (
            f"hyperion/"
            f"year={date_obj.year}/"
            f"month={date_obj.month:02d}/"
            f"day={date_obj.day:02d}/"
            f"data.parquet"
        )

        # 4. Upload
        upload_parquet_to_gcp(df, hive_path)

    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP Error fetching data for {target_date_str}: {e}")
        raise
    except Exception as e:
        logger.exception(f"Unexpected error processing {target_date_str}: {e}")
        raise


# --- CLI Commands ---


@app.command()
def backfill(start_date_str: str, end_date_str: str):
    """
    Iterates through a date range, downloading and uploading each day separately
    to maintain Hive partitioning.
    Format: 'YYYY-MM-DD'
    """
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

    current_date = start_date

    logger.info(f"Starting backfill from {start_date_str} to {end_date_str}")

    while current_date <= end_date:
        day_str = current_date.strftime("%Y-%m-%d")
        process_and_upload_day(day_str)
        current_date += timedelta(days=1)

    logger.info("Backfill completed.")


@app.command()
def daily():
    """
    Calculates yesterday's date and runs the ingestion pipeline.
    """
    yesterday = datetime.now() - timedelta(days=1)
    yesterday_str = yesterday.strftime("%Y-%m-%d")

    logger.info("Starting daily ingestion job.")
    process_and_upload_day(yesterday_str)
    logger.info("Daily ingestion job finished.")


if __name__ == "__main__":
    app()
