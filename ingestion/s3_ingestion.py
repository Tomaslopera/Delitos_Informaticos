import boto3
import pandas as pd
from dotenv import load_dotenv
import os
import io
from prefect import task, flow, get_run_logger

from secop_extraction import extraction_workflow, update_state_workflow
from snowflake_load import snowflake_load_flow

load_dotenv()

S3_BUCKET_NAME = 'delitos-informaticos-tomaslopera'
S3_KEY = 'extracted_data/delitos_data.parquet'
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_KEY')

s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)

@task(name="Upload Parquet to S3")
def upload_parquet_to_s3(data, bucket, key):
    logger = get_run_logger()
    
    if data.empty:
        logger.warning("DataFrame is empty. Skipping upload.")
        return False
    
    buffer = io.BytesIO()
    data.to_parquet(buffer, index=False)
    buffer.seek(0)
    
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=buffer.getvalue()
    )
    logger.info(f"Uploaded {len(data)} rows to s3://{bucket}/{key}")
    return True

@task(name="Append to S3 Parquet")
def append_to_s3_parquet(new_data, bucket, key):
    logger = get_run_logger()
    
    if new_data.empty:
        logger.warning("No new data to append")
        return False
    
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        existing_data = pd.read_parquet(io.BytesIO(response['Body'].read()))
        
        combined_data = pd.concat([existing_data, new_data], ignore_index=True)
        
        buffer = io.BytesIO()
        combined_data.to_parquet(buffer, index=False)
        buffer.seek(0)
        
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=buffer.getvalue()
        )
        logger.info(f"Appended {len(new_data)} rows. Total rows: {len(combined_data)}")
        return True
        
    except s3.exceptions.NoSuchKey:
        logger.info("File doesn't exist. Creating new parquet file...")
        return upload_parquet_to_s3(new_data, bucket, key)
    except Exception as e:
        logger.error(f"Error appending to S3: {e}")
        raise

@flow(name="Complete ETL Pipeline with Snowflake")
def etl_pipeline():
    logger = get_run_logger()
    
    try:
        logger.info("Step 1: Starting data extraction...")
        data = extraction_workflow()
        
        if data is None:
            logger.warning("No data extracted. Exiting...")
            return
        
        logger.info(f"Extracted {len(data)} rows")

        logger.info("Step 2: Uploading to S3...")
        success = append_to_s3_parquet(data, S3_BUCKET_NAME, S3_KEY)
        
        if not success:
            logger.error("S3 upload failed. Pipeline stopped.")
            return
        
        logger.info("S3 upload completed")

        logger.info("Step 3: Updating state metadata...")
        last_processed_date = data['fecha_hecho'].iloc[-1]
        update_state_workflow(last_processed_date)
        logger.info(f"State updated to {last_processed_date}")

        logger.info("Step 4: Loading to Snowflake...")
        snowflake_result = snowflake_load_flow()
        logger.info(f"Snowflake load completed: {snowflake_result}")
        
        logger.info("Complete ETL pipeline finished successfully!")
        logger.info(f"Extracted: {len(data)} rows")
        logger.info(f"Loaded to Snowflake: {snowflake_result.get('rows_loaded', 0)} rows")
        logger.info(f"Total in Snowflake: {snowflake_result.get('total_rows', 0)} rows")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise

if __name__ == "__main__":
    etl_pipeline()