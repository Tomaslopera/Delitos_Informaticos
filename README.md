# Delitos Informáticos Colombia - End-to-End Data Pipeline

A complete automated data engineering pipeline that extracts Colombian cybercrime data from government open data APIs, loads it into a cloud data warehouse and transforms it into analytics-ready datasets

## Project Overview

This project demonstrates a production-grade ELT (Extract, Load, Transform) pipeline for analyzing cybercrime trends in Colombia using data from [Datos Abiertos Colombia](https://www.datos.gov.co/)

### Key Features

- **Automated Data Extraction**: Incremental data loading from Socrata API
- **Cloud Storage**: Parquet files stored in AWS S3 with state management
- **Data Warehouse**: Snowflake for scalable data storage and processing
- **Data Transformation**: dbt for SQL-based data modeling (Bronze → Silver → Gold)
- **Orchestration**: Prefect for workflow scheduling and monitoring
- **Automation**: Cron-based scheduling for continuous data ingestion

## Architecture


## Tech Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Language** | Python 3.11 | Core programming |
| **API Client** | Sodapy | Socrata API integration |
| **Cloud Storage** | AWS S3 | Data lake for Parquet files |
| **Data Warehouse** | Snowflake | Centralized data storage |
| **Transformation** | dbt | SQL-based transformations |
| **Orchestration** | Prefect | Workflow management |
| **Scheduling** | Cron | Automated execution |
| **Format** | Parquet | Efficient columnar storage |

## Project Structure
```
Delitos_Informaticos/
├── ingestion/
│   ├── secop_extraction.py     # Socrata API extraction with state management
│   ├── s3_ingestion.py         # S3 upload and Snowflake orchestration
│   └── snowflake_load.py       # Snowflake data loading
├── snowflake_connection/       # dbt project
│   ├── models/
│   │   ├── silver/
│   │   │   ├── sources.yml                   # Source definitions
│   │   │   ├── stg_delitos.sql               # Base cleaning & standardization
│   │   │   └── stg_delitos_categorizado.sql  # Crime categorization
│   │   └── gold/
│   │       ├── delitos_por_departamento.sql  # Geographic aggregations
│   │       ├── delitos_por_mes.sql           # Time-series analysis
│   │       ├── top_municipios_delitos.sql    # Top municipalities ranking
│   │       ├── tendencia_ciberdelitos.sql    # Cybercrime trends
│   │       └── resumen_general.sql           # Executive summary
│   ├── dbt_project.yml
│   └── profiles.yml
├── logs/                      # ETL execution logs
├── run_etl.sh                 # Shell script for cron execution
├── pyproject.toml             # Python dependencies
└── README.md
```

## Workflow

### 1. Create Virtual Environment
```bash
uv init
source venv/bin/activate
```

### 2. Install Dependencies
```bash
uv add -r pyproject.toml
```

### 3. Configure Environment Variables

Create a `.env` file:
```bash
# AWS Credentials
AWS_ACCESS_KEY=your_aws_access_key
AWS_SECRET_KEY=your_aws_secret_key

# Snowflake Credentials
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ACCOUNT=your_account.region
SNOWFLAKE_WAREHOUSE=DELITOS_INFORMATICOS_WH
SNOWFLAKE_DATABASE=DELITOS_INFORMATICOS_DB
SNOWFLAKE_SCHEMA=BRONZE
SNOWFLAKE_ROLE=DELITOS_INFORMATICOS_ROLE
```

### 4. Set Up AWS S3
```bash
# Create S3 bucket
aws s3 mb s3://your-bucket-name

# Verify bucket
aws s3 ls
```

### 5. Set Up Snowflake
```sql
-- Run in Snowflake worksheet
USE ROLE ACCOUNTADMIN;

-- Create warehouse
CREATE WAREHOUSE DELITOS_INFORMATICOS_WH WITH WAREHOUSE_SIZE='X-SMALL';

-- Create database
CREATE DATABASE DELITOS_INFORMATICOS_DB;

-- Create role
CREATE ROLE DELITOS_INFORMATICOS_ROLE;

-- Grant permissions
GRANT USAGE ON WAREHOUSE DELITOS_INFORMATICOS_WH TO ROLE DELITOS_INFORMATICOS_ROLE;
GRANT ALL ON DATABASE DELITOS_INFORMATICOS_DB TO ROLE DELITOS_INFORMATICOS_ROLE;
GRANT ROLE DELITOS_INFORMATICOS_ROLE TO USER your_username;

-- Create schemas
USE ROLE DELITOS_INFORMATICOS_ROLE;
CREATE SCHEMA DELITOS_INFORMATICOS_DB.BRONZE;
CREATE SCHEMA DELITOS_INFORMATICOS_DB.SILVER;
CREATE SCHEMA DELITOS_INFORMATICOS_DB.GOLD;

-- Create raw table
USE SCHEMA DELITOS_INFORMATICOS_DB.BRONZE;
CREATE TABLE delitos_raw (
    cantidad INTEGER,
    cod_depto VARCHAR(10),
    cod_muni VARCHAR(10),
    departamento VARCHAR(100),
    descripcion_conducta VARCHAR(500),
    fecha_hecho TIMESTAMP,
    municipio VARCHAR(100),
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);
```

### 6. Configure S3 to Snowflake Integration

#### AWS IAM Role

##### Create Policy 

```bash
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:GetObjectVersion",
                "s3:ListBucket",
                "s3:GetBucketLocation"
            ],
            "Resource": [
                "arn:aws:s3:::bucket-name/*",
                "arn:aws:s3:::bucket-name"
            ]
        }
    ]
}
```

##### Create Role

- **Role**
- **AWS Account**
- **Select Policy**
- **Create Role**

##### Snowflake Integration

```bash
USE ROLE ACCOUNTADMIN;

CREATE STORAGE INTEGRATION s3_delitos_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::AWS_ACCOUNT_ID:role/IAM-ROLE'
  STORAGE_ALLOWED_LOCATIONS = ('s3://bucket-name/');

-- Get Snowflake's identity
DESC STORAGE INTEGRATION s3_delitos_integration;
```

> **STORAGE_AWS_IAM_USER**

> **STORAGE_AWS_EXTERNAL_ID**

##### IAM Role → Trust Relationships → Edit

```bash
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "AWS": "PASTE_YOUR_STORAGE_AWS_IAM_USER_ARN_HERE"
            },
            "Action": "sts:AssumeRole",
            "Condition": {
                "StringEquals": {
                    "sts:ExternalId": "PASTE_YOUR_STORAGE_AWS_EXTERNAL_ID_HERE"
                }
            }
        }
    ]
}
```

##### Update Snowflake Account

```bash
ALTER STORAGE INTEGRATION s3_delitos_integration SET STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::AWS_ACCOUNT_ID:role/IAM-ROLE';

-- Verify it updated
DESC STORAGE INTEGRATION s3_delitos_integration;
```

### 7. Configure Prefect
```bash
# Login to Prefect Cloud
prefect cloud login

# Verify connection
prefect profile list
```

### 8. Set Up Automated Scheduling

> **nano run_etl.sh**

```bash
#!/bin/bash

# Go to your project folder
cd /Users/loperatomas410/Documents/WorkSpace/Proyectos/Delitos_Informaticos

# Activate your virtual environment
source .venv/bin/activate

# Run your ETL script
python ingestion/s3_ingestion.py
```

```bash
# Make script executable
chmod +x run_etl.sh

# Run manually
./run_etl.sh
```

```bash
# Edit crontab
crontab -e

# Runs every 2 hours
0 */2 * * * /path/to/project/run_etl.sh >> /path/to/project/logs/etl.log 2>&1
```

### 9. SetUp dbt

**`dbt init [name]` → `snowflake` → `locator` → `user` → `password (Account password)` → `role` → `warehouse` → `db` → `schema`**

```bash
cd snowflake_connection

# Test connection
dbt debug

# Run all models
dbt run

# Run tests
dbt test

# Generate documentation
dbt docs generate
dbt docs serve
```
