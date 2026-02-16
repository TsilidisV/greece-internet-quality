import os
import sys
from google.cloud import storage
from google.cloud import bigquery
from google.api_core.exceptions import Forbidden, NotFound

def test_gcs_connection():
    bucket_name = os.getenv("GCS_BUCKET")
    if not bucket_name:
        print("⚠️ SKIPPING GCS TEST: 'GCS_BUCKET' env var not set.")
        return False

    print(f"\n>>> 🪣 Testing GCS Connection to '{bucket_name}'...")
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        
        # CHANGED: Instead of bucket.exists(), we try to list files.
        # This confirms we have access to the *contents*, which is what matters.
        blobs = list(client.list_blobs(bucket_name, max_results=1))
        
        print(f"   ✅ SUCCESS: Service Account can access objects in '{bucket_name}'.")
        return True

    except Forbidden:
        print(f"   ❌ PERMISSION DENIED: Service Account cannot list objects in '{bucket_name}'.")
        return False
    except Exception as e:
        # If the bucket doesn't exist, list_blobs throws a NotFound (404) error
        if "404" in str(e): 
             print(f"   ❌ FAILURE: Bucket '{bucket_name}' not found.")
        else:
             print(f"   ❌ ERROR: {e}")
        return False

def test_bq_connection():
    dataset_id = os.getenv("BQ_DATASET")
    project_id = os.getenv("GOOGLE_CLOUD_PROJECT") # Cloud Run sets this automatically
    
    if not dataset_id:
        print("⚠️ SKIPPING BQ TEST: 'BQ_DATASET' env var not set.")
        return False

    print(f"\n>>> 🔎 Testing BigQuery Connection to '{dataset_id}'...")
    try:
        client = bigquery.Client(project=project_id)
        
        # Test 1: Simple Query (Requires bigquery.jobs.create)
        query = "SELECT 1"
        query_job = client.query(query)
        query_job.result()
        print("   ✅ SUCCESS: Can execute SQL queries.")

        # Test 2: Check Dataset (Requires bigquery.datasets.get)
        dataset_ref = client.dataset(dataset_id)
        try:
            client.get_dataset(dataset_ref)
            print(f"   ✅ SUCCESS: Dataset '{dataset_id}' is accessible.")
            return True
        except NotFound:
            print(f"   ❌ FAILURE: Dataset '{dataset_id}' not found.")
            return False
    except Forbidden:
        print("   ❌ PERMISSION DENIED: Service Account missing BigQuery roles.")
        return False
    except Exception as e:
        print(f"   ❌ ERROR: {e}")
        return False

if __name__ == "__main__":
    print(">>> 📡 STARTING INFRASTRUCTURE CONNECTIVITY CHECK")
    
    gcs_ok = test_gcs_connection()
    bq_ok = test_bq_connection()

    if gcs_ok and bq_ok:
        print("\n>>> 🎉 ALL SYSTEMS GO. INFRASTRUCTURE IS READY.")
        sys.exit(0)
    else:
        print("\n>>> 💥 CONNECTIVITY TEST FAILED.")
        sys.exit(1)