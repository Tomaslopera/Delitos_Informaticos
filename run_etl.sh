#!/bin/bash

# Go to your project folder
cd /Users/loperatomas410/Documents/WorkSpace/Proyectos/Delitos_Informaticos

# Activate your virtual environment
source .venv/bin/activate

# Run your ETL script
python ingestion/s3_ingestion.py

# Done!
