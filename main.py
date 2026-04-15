from lookup.lookup_values_load import (
    insert_grade_description_lookup_data, 
    insert_grading_company_lookup_data, 
    insert_language_lookup_data, 
    insert_rarity_lookup_data, 
    insert_set_lookup_data,
    insert_currency_lookup_data,
    insert_purchase_source_lookup_data
)
from references import *
from utils.aws_s3_utils import *
from landing.pokemon_cards_landing import *
from card import *
from card_instance import *
from grade import *
from utils.db_utils import *
import os

# Load pipeline flags from environment variables
PIPELINE_CREATE_MODEL = os.getenv("PIPELINE_CREATE_MODEL", "True").lower() == "true"
PIPELINE_CLEAR_LANDING = os.getenv("PIPELINE_CLEAR_LANDING", "False").lower() == "true"
PIPELINE_LIST_S3 = os.getenv("PIPELINE_LIST_S3", "True").lower() == "true"
PIPELINE_UPLOAD_S3 = os.getenv("PIPELINE_UPLOAD_S3", "True").lower() == "true"
PIPELINE_DOWNLOAD_S3 = os.getenv("PIPELINE_DOWNLOAD_S3", "False").lower() == "true"

# Status indicators for logging
STATUS_OK = "[OK]"
STATUS_FAIL = "[FAIL]"

def run_db_utils():
    try:
        logger.info("=" * 60)
        logger.info("DATABASE SETUP PHASE")
        logger.info("=" * 60)
        
        # Step 1: Create database model
        if PIPELINE_CREATE_MODEL:
            logger.info("[Step 1/2] Creating database model...")
            try:
                create_model(create_model_flag=True)
                logger.info(f"[Step 1/2] {STATUS_OK} Database model created successfully")
            except Exception as e:
                logger.error(f"[Step 1/2] {STATUS_FAIL} Failed to create database model: {e}")
                raise
        else:
            logger.info("[Step 1/2] Skipping database model creation (PIPELINE_CREATE_MODEL=False)")
        
        # Step 2: Clear landing table
        if PIPELINE_CLEAR_LANDING:
            logger.info("[Step 2/2] Clearing landing table...")
            try:
                clear_landing_table(clear_landing_table_flag=True)
                logger.info(f"[Step 2/2] {STATUS_OK} Landing table cleared successfully")
            except Exception as e:
                logger.error(f"[Step 2/2] {STATUS_FAIL} Failed to clear landing table: {e}")
                raise
        else:
            logger.info("[Step 2/2] Skipping landing table clear (PIPELINE_CLEAR_LANDING=False)")
            
        logger.info("=" * 60)
        logger.info("DATABASE SETUP PHASE COMPLETE")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"Error in database setup: {e}")
        raise

def run_poke_pipeline():
    try:
        # Run database utilities first
        run_db_utils()
        
        logger.info("=" * 60)
        logger.info("POKEMON CARD DATA PROCESSING PIPELINE")
        logger.info("=" * 60)
        
        step = 0
        total_steps = 12
        
        # Step 1: List S3 objects
        step += 1
        if PIPELINE_LIST_S3:
            logger.info(f"[Step {step}/{total_steps}] Listing S3 objects...")
            list_objects(list_objects_flag=True)
            logger.info(f"[Step {step}/{total_steps}] {STATUS_OK} S3 objects listed")
        else:
            logger.info(f"[Step {step}/{total_steps}] Skipping S3 list (PIPELINE_LIST_S3=False)")
        
        # Step 2: Upload to S3
        step += 1
        if PIPELINE_UPLOAD_S3:
            logger.info(f"[Step {step}/{total_steps}] Uploading file to S3...")
            upload_file_to_s3(upload_flag=True)
            logger.info(f"[Step {step}/{total_steps}] {STATUS_OK} File uploaded to S3")
        else:
            logger.info(f"[Step {step}/{total_steps}] Skipping S3 upload (PIPELINE_UPLOAD_S3=False)")
        
        # Step 3: Load landing data
        step += 1
        logger.info(f"[Step {step}/{total_steps}] Loading landing data...")
        load_pokemon_cards_landing()
        logger.info(f"[Step {step}/{total_steps}] {STATUS_OK} Landing data loaded")
        
        # Step 4-9: Load lookup tables
        lookup_tables = [
            (f"[Step {step+1}/{total_steps}]", "Language", insert_language_lookup_data),
            (f"[Step {step+2}/{total_steps}]", "Card Set", insert_set_lookup_data),
            (f"[Step {step+3}/{total_steps}]", "Grading Company", insert_grading_company_lookup_data),
            (f"[Step {step+4}/{total_steps}]", "Grade Description", insert_grade_description_lookup_data),
            (f"[Step {step+5}/{total_steps}]", "Rarity", insert_rarity_lookup_data),
            (f"[Step {step+6}/{total_steps}]", "Currency", insert_currency_lookup_data),
            (f"[Step {step+7}/{total_steps}]", "Purchase Source", insert_purchase_source_lookup_data),
        ]
        
        for step_label, table_name, insert_func in lookup_tables:
            logger.info(f"{step_label} Loading {table_name} lookup...")
            insert_func()
            logger.info(f"{step_label} {STATUS_OK} {table_name} lookup loaded")
            step += 1
        
        # Step 11: Insert card data
        step += 1
        logger.info(f"[Step {step}/{total_steps}] Inserting card data...")
        insert_card_data()
        logger.info(f"[Step {step}/{total_steps}] {STATUS_OK} Card data inserted")
        
        # Step 12: Insert card instance data
        step += 1
        logger.info(f"[Step {step}/{total_steps}] Inserting card instance data...")
        insert_card_instance_data()
        logger.info(f"[Step {step}/{total_steps}] {STATUS_OK} Card instance data inserted")
        
        # Step 13: Insert grade data
        step += 1
        total_steps = 14
        logger.info(f"[Step {step}/{total_steps}] Inserting grade data...")
        insert_grade_data()
        logger.info(f"[Step {step}/{total_steps}] {STATUS_OK} Grade data inserted")
        
        # Step 14: Download from S3
        step += 1
        if PIPELINE_DOWNLOAD_S3:
            logger.info(f"[Step {step}/{total_steps}] Downloading from S3...")
            download_file_from_s3(download_flag=True)
            logger.info(f"[Step {step}/{total_steps}] {STATUS_OK} File downloaded from S3")
        else:
            logger.info(f"[Step {step}/{total_steps}] Skipping S3 download (PIPELINE_DOWNLOAD_S3=False)")
        
        logger.info("=" * 60)
        logger.info("PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("=" * 60)
            
    except Exception as e:
        logger.error(f"{STATUS_FAIL} Pipeline failed at step {step}: {e}")
        raise

if __name__ == "__main__":
    run_poke_pipeline()
