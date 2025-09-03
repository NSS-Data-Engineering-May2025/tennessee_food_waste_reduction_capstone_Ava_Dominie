import datetime
from io import BytesIO
import json
import os
import logging
from time import timezone
import pandas as pd
import io
import requests
import minio
from snowflake.connector.pandas_tools import write_pandas
from minio.error import S3Error
import snowflake.connector
from dotenv import load_dotenv
from minio import Minio
import polars as pl


load_dotenv()


# Logging setup
VSCODE_WORKSPACE_FOLDER = os.getenv('VSCODE_WORKSPACE_FOLDER', os.getcwd())
LOG_DIR = os.path.join(VSCODE_WORKSPACE_FOLDER, 'src/logs')
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, 'data_ingestion.log')

logging.basicConfig(
	level=logging.INFO,
	format='%(asctime)s - %(levelname)s - %(message)s',
	handlers=[
		logging.FileHandler(LOG_FILE),
		logging.StreamHandler()
	]
)
logger = logging.getLogger("data_ingestion")
logger.info("Logger initialized successfully")




# Create a script to programmatically download api endpoints using requests into minio


# I have both of these pulling from an API so the data is as up to date as possible 

# Three endpoints for NASS quick stats
# Field yields, Acres harvested, and Production


# Function to process any NASS Quick Stats endpoint and upload the result as CSV to MinIO
def process_nass_quick_stats(minio_client, bucket_name, query_url, object_name):
    """
    Downloads data from a NASS Quick Stats API endpoint, converts it to CSV, and uploads to MinIO.
    Args:
        minio_client: Minio client object
        bucket_name: Name of the MinIO bucket
        query_url: API endpoint URL
        object_name: Desired CSV filename in MinIO
    Returns:
        The parsed JSON data from the API response
    """
    try:
        logger.info(f"Starting data ingestion from {query_url}")
        # Request data from the API endpoint
        response = requests.get(query_url)
        response.raise_for_status()
        data = response.json()

        # Convert JSON to DataFrame and then to CSV
        if 'data' in data:
            df = pd.DataFrame(data['data'])
        else:
            df = pd.DataFrame(data)
        # Convert DataFrame to CSV in memory
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_bytes = io.BytesIO(csv_buffer.getvalue().encode())
        csv_size = csv_bytes.getbuffer().nbytes

        # Upload to MinIO from memory
        csv_bytes.seek(0)
        minio_client.put_object(
            bucket_name,
            object_name,
            csv_bytes,
            csv_size,
            content_type="text/csv"
        )
        logger.info(f"Data uploaded to MinIO bucket '{bucket_name}' as '{object_name}'")
        logger.info("Data retrieved and uploaded successfully")
        return data
    except requests.RequestException as e:
        logger.error(f"Error occurred while fetching data: {e}")
        return None





def main():
    minio_client = Minio(
        os.getenv("MINIO_URL"),
        access_key=os.getenv("MINIO_ACCESS_KEY"),
        secret_key=os.getenv("MINIO_SECRET_KEY"),
        secure=False
    )

    bucket_name = os.getenv("MINIO_BUCKET_NAME")

    # List of (endpoint URL, output CSV filename) tuples
    endpoints = [
        (os.getenv("NASS_QUICK_STATS_FIELD_YIELDS"), "field_yields.csv"),
        (os.getenv("NASS_QUICK_STATS_ACRES_HARVESTED"), "acres_harvested.csv"),
        (os.getenv("NASS_QUICK_STATS_PRODUCTION"), "production.csv"),
        (os.getenv("WEATHER_OPEN_METEO"), "weather_open_meteo.csv"),
    ]

    # Process each endpoint and upload to MinIO
    for query_url, object_name in endpoints:
        process_nass_quick_stats(minio_client, bucket_name, query_url, object_name)




if __name__ == "__main__":
    main()