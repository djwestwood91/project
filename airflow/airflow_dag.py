"""
Sample Airflow DAG for Pokemon Card Data Processing Pipeline

This DAG orchestrates the ETL pipeline for processing Pokemon trading card data,
including database setup, landing layer processing, lookup table loading, and data normalization.

Environment Variables Required:
- PIPELINE_CREATE_MODEL: Create database model (default: True)
- PIPELINE_CLEAR_LANDING: Clear landing table (default: False)
- PIPELINE_LIST_S3: List S3 objects (default: True)
- PIPELINE_UPLOAD_S3: Upload files to S3 (default: True)
- PIPELINE_DOWNLOAD_S3: Download files from S3 (default: True)
"""

from datetime import datetime, timedelta
import sys
import os

# Add project path to sys.path FIRST
PROJECT_PATH = r"c:\Users\j_har\OneDrive\Documents\Dan\test_scripts\project"
sys.path.insert(0, PROJECT_PATH)

from airflow.decorators import dag, task

# DAG Configuration
@dag(
    dag_id='pokemon_card_processing_pipeline',
    default_args={
        'owner': 'data-team',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    start_date=datetime(2026, 4, 21),
    schedule_interval='0 2 * * *',
    tags=['pokemon', 'etl', 'data-processing'],
    max_active_runs=1,
    catchup=False,
    description='ETL pipeline for Pokemon card data processing with AWS S3 and PostgreSQL',
)
def pokemon_card_processing_pipeline():
    """Pokemon card processing pipeline DAG"""
    
    @task
    def run_pipeline():
        """Run the Pokemon card processing pipeline"""
        from main import run_poke_pipeline
        run_poke_pipeline()
    
    run_pipeline()

# Instantiate the DAG
pokemon_card_processing_pipeline()
