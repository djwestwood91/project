from lookup.lookup_values_load import insert_grade_description_lookup_data, insert_grading_company_lookup_data, insert_language_lookup_data, insert_rarity_lookup_data, insert_set_lookup_data
from references import *
from aws_s3_utils import *
from landing.pokemon_cards_landing import *
from card import *
from grade import *

def run_pipeline():
    try:
        logger.info("Starting the Pokemon card data processing pipeline...")
        # upload file to s3
        list_objects(list_objects_flag=True)
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
        # seller data processing
        # card_grade data processing
        # card_seller data processing
        # download file from s3
        download_file_from_s3(download_flag=False)
        logger.info("Pokemon card data processing pipeline completed successfully.")
    except Exception as e:
        logger.error(f"Error in pipeline execution: {e}")
        raise

run_pipeline()
