import json
import boto3
import os
import pandas as pd
from sodapy import Socrata
from dotenv import load_dotenv
from prefect import task, flow, get_run_logger

load_dotenv()

S3_BUCKET_NAME = 'delitos-informaticos-tomaslopera'
STATE_KEY = 'metadata/state.json'
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_KEY')

s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)

@task(name="Create or Update State")
def create_or_update_state(last_processed_date):
    logger = get_run_logger()
    state_data = {'last_processed_date': last_processed_date}
    
    json_data = json.dumps(state_data, indent=2)
    
    s3.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=STATE_KEY,
        Body=json_data,
        ContentType='application/json'
    )
    logger.info(f"State updated: {state_data}")

@task(name="Get State from S3")
def get_state():
    logger = get_run_logger()
    try:
        response = s3.get_object(Bucket=S3_BUCKET_NAME, Key=STATE_KEY)
        state_data = json.loads(response['Body'].read().decode('utf-8'))
        return state_data
    except s3.exceptions.NoSuchKey:
        logger.warning("State file doesn't exist yet")
        return None
    except Exception as e:
        logger.error(f"Error reading state: {e}")
        return None

@task(name="Socrata Data Extraction")  
def sodapy_extraction(last_processed_date):
    logger = get_run_logger()
    client = Socrata("www.datos.gov.co", None)

    results = client.get("4v6r-wu98", where=f"fecha_hecho > '{last_processed_date}'", limit=2000)

    data = pd.DataFrame.from_records(results)
    logger.info(f"Retrieved {len(data)} records from Socrata")
    
    return data

@flow(name="Data Extraction Workflow")
def extraction_workflow():
    logger = get_run_logger()
    
    current_state = get_state()
    
    if current_state is None:
        logger.info("No existing state found. Initializing...")
        initial_date = '2006-01-01T00:00:00.000'
        create_or_update_state(initial_date)
        current_state = {'last_processed_date': initial_date}
    
    logger.info(f"Starting extraction from: {current_state['last_processed_date']}")
    
    data = sodapy_extraction(current_state['last_processed_date'])
    
    if data.empty:
        logger.warning("No new data to process")
        return None
    
    logger.info(f"Retrieved {len(data)} records")
    
    return data

@flow(name="Update State Workflow")
def update_state_workflow(last_processed_date):
    logger = get_run_logger()
    create_or_update_state(last_processed_date)
    logger.info(f"Processing complete. Last processed date: {last_processed_date}")