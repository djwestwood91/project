from lookup.lookup_values_load import insert_grade_description_lookup_data, insert_grading_company_lookup_data, insert_language_lookup_data, insert_rarity_lookup_data, insert_set_lookup_data
from references import *
from utils.aws_s3_utils import *
from landing.pokemon_cards_landing import *
from card import *
from grade import *
from utils.db_utils import create_model

def run_db_utils():
    try:
        # create the database model - adjust the flag to True if you want to test the model creation functionality, 
        # but ensure the model.sql file exists in the specified location to avoid errors
        create_model(create_model_flag=True)
        # clear the landing table - adjust the flag to True if you want to clear the landing table before loading new data, 
        # but ensure this is what you want to do to avoid unintended data loss
        clear_landing_table(clear_landing_table_flag=False)
    except Exception as e:
        logger.error(f"Error creating database model: {e}")
        raise

def run_poke_pipeline():
    try:
        # run the db utils to create the model and clear the landing table if needed
        run_db_utils()
        logger.info("Starting the Pokemon card data processing pipeline...")
        # list objects in S3 - adjust the flag to True if you want to test the listing functionality, but ensure the file exists in the specified S3 location to avoid errors
        list_objects(list_objects_flag=True)
        # upload file to s3 - adjust the flag to True if you want to test the upload functionality, but ensure the file exists in the specified S3 location to avoid errors
        upload_file_to_s3(upload_flag=True)
        # landing data processing
        load_pokemon_cards_landing()
        # lookup values processing
        insert_language_lookup_data()
        insert_set_lookup_data()
        insert_grading_company_lookup_data()
        insert_grade_description_lookup_data()
        insert_rarity_lookup_data()
        # card data processing
        insert_card_data()
        # grade data processing
        insert_grade_data()
        # seller data processing
        # insert_seller_data()
        # purchase data processing
        # insert_purchase_data()
        # download file from s3 - adjust the flag to True if you want to test the download functionality, but ensure the file exists in the specified S3 location to avoid errors
        download_file_from_s3(download_flag=False)
        logger.info("Pokemon card data processing pipeline completed successfully.")
    except Exception as e:
        logger.error(f"Error in pipeline execution: {e}")
        raise

if __name__ == "__main__":
    run_db_utils()
    run_poke_pipeline()
