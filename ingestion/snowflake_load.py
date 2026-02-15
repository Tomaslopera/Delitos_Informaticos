import snowflake.connector
import os
from dotenv import load_dotenv
from prefect import task, flow, get_run_logger

load_dotenv()

@task(name="Connect to Snowflake")
def connect_snowflake():
    logger = get_run_logger()
    
    try:
        conn = snowflake.connector.connect(
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            schema=os.getenv('SNOWFLAKE_SCHEMA'),
            role=os.getenv('SNOWFLAKE_ROLE')
        )
        logger.info("Successfully connected to Snowflake")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to Snowflake: {e}")
        raise

@task(name="Load Data to Snowflake")
def load_to_snowflake(conn):
    logger = get_run_logger()
    cursor = conn.cursor()
    
    try:
        logger.info("Loading data from S3 to Snowflake...")

        cursor.execute("""
            COPY INTO delitos_raw
            FROM @s3_delitos_stage
            FILE_FORMAT = (TYPE = 'PARQUET')
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            FORCE = FALSE
        """)

        result = cursor.fetchone()
        
        if result:
            rows_loaded = result[1]
            logger.info(f"Successfully loaded {rows_loaded} rows")

        cursor.execute("SELECT COUNT(*) FROM delitos_raw")
        total_count = cursor.fetchone()[0]
        logger.info(f"Total rows in table: {total_count}")
        
        return {
            'rows_loaded': rows_loaded if result else 0,
            'total_rows': total_count
        }
        
    except Exception as e:
        logger.error(f"Error loading to Snowflake: {e}")
        raise
    finally:
        cursor.close()

@task(name="Close Snowflake Connection")
def close_connection(conn):
    logger = get_run_logger()
    if conn:
        conn.close()
        logger.info("Snowflake connection closed")

@flow(name="Snowflake Data Load")
def snowflake_load_flow():
    logger = get_run_logger()
    
    conn = None
    try:
        conn = connect_snowflake()
        
        result = load_to_snowflake(conn)
        
        logger.info(f"Load completed: {result}")
        return result
        
    except Exception as e:
        logger.error(f"Snowflake load flow failed: {e}")
        raise
    finally:
        if conn:
            close_connection(conn)

if __name__ == "__main__":
    snowflake_load_flow()