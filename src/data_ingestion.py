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

# Field Yields
def process_field_yields(minio_client, bucket_name, object_name="job_posting_data.csv", conn=None, snowflake_db=None, snowflake_schema=None):
    QUERY_URL = os.getenv('NASS_QUICK_STATS_FIELD_YIELDS')
    try:
        logger.info(f"Starting data ingestion for Field Yields from {QUERY_URL}")
        response = requests.get(QUERY_URL)
        response.raise_for_status()
        data = response.json()
        csv_bytes = io.BytesIO(response.content)
        csv_size = len(response.content)

        # Upload to MinIO from memory
        csv_bytes.seek(0)
        minio_client.put_object(
            bucket_name,
            object_name,
            csv_bytes,
            csv_size,
            content_type="text/csv"
        )
        logger.info(f"Job posting data uploaded to MinIO bucket '{bucket_name}' as '{object_name}'")
        logger.info("Field yields data retrieved successfully")
        return data
    except requests.RequestException as e:
        logger.error(f"Error occurred while fetching field yields data: {e}")
        return None




def main():
    minio_client = Minio(
        os.getenv("MINIO_URL"),
        access_key=os.getenv("MINIO_ACCESS_KEY"),
        secret_key=os.getenv("MINIO_SECRET_KEY"),
        secure=False
    )

    bucket_name = os.getenv("MINIO_BUCKET_NAME")
    object_name = "field_yields.csv"

    process_field_yields(minio_client, bucket_name, object_name)




if __name__ == "__main__":
    main()