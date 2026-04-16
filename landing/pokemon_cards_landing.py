from references import *
from utils.db_utils import truncate_table
import pandas as pd

def load_pokemon_cards_landing():
    """
    Load Pokemon card data from an Excel file into a landing database table.
    This function reads Pokemon card information from a specified Excel file,
    transforms the source column names to standardized landing table column names,
    assigns unique row identifiers, and inserts the data into the 'landing_pokemon_card'
    database table.
    The function performs the following steps:
    1. Reads the Excel file from the configured file path and sheet name
    2. Maps source column names to standardized landing table column names using a
        predefined mapping dictionary
    3. Adds a 'row_id' column with sequential integer values (1 to n) to uniquely
        identify each record in the landing table
    4. Appends the transformed data to the 'landing_pokemon_card' table in the
        landing database schema
    Returns:
         None
    Raises:
         Exception: If any error occurs during file reading, data transformation,
                        or database insertion. Errors are logged before re-raising.
    Side Effects:
         - Logs info messages about the load process start and completion
         - Logs error messages if any exception occurs
         - Inserts data into the database table
    """
    try:
        logger.info(f"Reading source data from {POKEMON_CARD_FILE_PATH + POKEMON_CARD_FILE_NAME}")
        df = pd.read_excel(POKEMON_CARD_FILE_PATH + POKEMON_CARD_FILE_NAME, sheet_name=POKEMON_CARD_SHEET_NAME)
        
        # Define column mapping if needed 
        # this is to highlight the transformation from the source file column names to the landing table column names, 
        # adjust the mapping as per the actual column names in the source file and the landing table
        landing_column_mapping = {
                                    'Card': 'card',
                                    'Set': 'card_set',
                                    'Year': 'card_year',
                                    'Graded': 'card_graded',
                                    'Grade': 'grade',
                                    'Grading Company': 'grading_company',
                                    'Grading Company Full Name': 'grading_company_full_name',
                                    'Grade Description': 'grade_description',
                                    'Grading Certification Number': 'grading_certification_number',
                                    'Graded Card URL': 'graded_card_url',
                                    'Graded Date': 'graded_date',
                                    'Holo': 'card_holo_flag',
                                    '1st Edition': 'card_first_edition_flag',
                                    'Promo': 'card_promo_flag',
                                    'Language': 'card_language',
                                    'Rarity': 'card_rarity',
                                    'Additional Details 1': 'card_additional_details_1',
                                    'Additional Details 2': 'card_additional_details_2',
                                    'Additional Details 3': 'card_additional_details_3',
                                    'Purchase Price': 'card_purchase_price',
                                    'Purchase Price Currency': 'card_purchase_price_currency',
                                    'Postage Fees': 'postage_fees',
                                    'Postage Fees Currency': 'postage_fees_currency',
                                    'Total': 'card_total',
                                    'Total Currency': 'card_total_currency',
                                    'Date Purchased': 'card_date_purchased',
                                    'Source': 'card_source',
                                    'Seller': 'card_seller',
                                    'Website': 'website',
                                    'Seller Country': 'seller_country'
                                }

        # Rename columns in the DataFrame to determine landing table schema
        df_with_updated_mapping = df.rename(columns=landing_column_mapping)
        
        # Add row_id to uniquely identify each landing record
        df_with_updated_mapping.insert(0, 'row_id', range(1, len(df_with_updated_mapping) + 1))
        
        df_with_updated_mapping.to_sql('landing_pokemon_card', con=ENGINE, schema=DB_LANDING_SCHEMA, if_exists='append', index=False)
        logger.info(f"Pokemon cards from {POKEMON_CARD_FILE_PATH + POKEMON_CARD_FILE_NAME} loaded successfully")
    except Exception as e:
        logger.error(f"Error loading pokemon cards: {e}")
        raise

def clear_landing_table(clear_landing_table_flag=True):
    try:
        if clear_landing_table_flag:
            truncate_table(DB_LANDING_SCHEMA, DB_LANDING_TABLE, restart_identity=True)
            logger.info(f"{DB_LANDING_TABLE} landing table cleared successfully")
    except Exception as e:
        logger.error(f"Error clearing landing table: {e}")
        raise
