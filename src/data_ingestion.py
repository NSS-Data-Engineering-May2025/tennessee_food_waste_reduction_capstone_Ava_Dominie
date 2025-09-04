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


# Load all minio into Snowflake
def load_minio_to_snowflake(minio_client, bucket_name, conn, table_name, object_name):
    """
    Loads a specific CSV from MinIO into a specific Snowflake table.
    """
    try:
        logger.info(f"Starting data load from MinIO '{object_name}' to Snowflake table '{table_name}'")
        minio_response = minio_client.get_object(bucket_name, object_name)
        csv_bytes = BytesIO(minio_response.read())
        minio_response.close()
        minio_response.release_conn()
        csv_bytes.seek(0)
        df = pd.read_csv(csv_bytes)
        df["SOURCE_FILE"] = object_name
        df["LOAD_TIMESTAMP_UTC"] = datetime.datetime.now(datetime.timezone.utc)
        df.columns = df.columns.str.upper()
        # Use env vars for database and schema
        snowflake_db = os.getenv('SNOWFLAKE_DATABASE')
        snowflake_schema = os.getenv('SNOWFLAKE_SCHEMA_BRONZE')
        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name=table_name,
            database=snowflake_db,
            schema=snowflake_schema,
            auto_create_table=True,
            overwrite=True,
            quote_identifiers=True,
            use_logical_type=True
        )
        if success:
            logger.info(f"Data from {object_name} written to Snowflake table {snowflake_db}.{snowflake_schema}.{table_name} - {nrows} rows in {nchunks} chunks.")
        else:
            logger.error(f"Failed to write {object_name} to Snowflake table {table_name}.")
    except Exception as e:
        logger.error(f"Error occurred while loading data into Snowflake: {e}")




def main():


    # Load environment variables once and use them throughout
    MINIO_EXTERNAL_URL = os.getenv('MINIO_EXTERNAL_URL')
    MINIO_BUCKET_NAME = os.getenv('MINIO_BUCKET_NAME')
    MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY')
    MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY')
    SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
    SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
    SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
    SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE')
    SNOWFLAKE_SCHEMA_BRONZE = os.getenv('SNOWFLAKE_SCHEMA_BRONZE')
    SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
    SNOWFLAKE_ROLE = os.getenv('SNOWFLAKE_ROLE')

    minio_client = Minio(
        MINIO_EXTERNAL_URL,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

    bucket_name = MINIO_BUCKET_NAME


    # List of (endpoint URL, output CSV filename) tuples, add static CSV (no API) as (None, ...)
    endpoints = [
        (os.getenv("NASS_QUICK_STATS_FIELD_YIELDS"), "field_yields.csv"),
        (os.getenv("NASS_QUICK_STATS_ACRES_HARVESTED"), "acres_harvested.csv"),
        (os.getenv("NASS_QUICK_STATS_PRODUCTION"), "production.csv"),
        (os.getenv("WEATHER_OPEN_METEO"), "weather_open_meteo.csv"),
        (None, "tennessee_county_crop_production_2022.csv"),
    ]

    # Process each endpoint and upload to MinIO (skip if no API URL, i.e., just process from MinIO)
    for query_url, object_name in endpoints:
        if query_url:
            process_nass_quick_stats(minio_client, bucket_name, query_url, object_name)

    # Snowflake connection
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA_BRONZE,
        role=SNOWFLAKE_ROLE
    )


    # Table names for each dataset (add static CSV table)
    table_names = [
        "FIELD_YIELDS",
        "ACRES_HARVESTED",
        "PRODUCTION",
        "WEATHER_OPEN_METEO",
        "TENNESSEE_COUNTY_CROP_PRODUCTION_2022"
    ]

    # Load all MinIO data into Snowflake (only the main files, with correct table/database logic)
    for ((query_url, object_name), table_name) in zip(endpoints, table_names):
        load_minio_to_snowflake(
            minio_client,
            bucket_name,
            conn,
            table_name,
            object_name
        )


if __name__ == "__main__":
    main()