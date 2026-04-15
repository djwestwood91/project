from references import *
from utils.db_utils import truncate_table
import pandas as pd

def load_pokemon_cards_landing():
    try:
        logger.info(f"Reading source data from {pokemon_card_file_path + pokemon_card_file_name}")
        df = pd.read_excel(pokemon_card_file_path + pokemon_card_file_name, sheet_name=pokemon_card_sheet_name)
        
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
                                    'Website': 'website'
                                }

        # Rename columns in the DataFrame to determine landing table schema
        df_with_updated_mapping = df.rename(columns=landing_column_mapping)
        
        # Add row_id to uniquely identify each landing record
        df_with_updated_mapping.insert(0, 'row_id', range(1, len(df_with_updated_mapping) + 1))
        
        df_with_updated_mapping.to_sql('landing_pokemon_card', con=engine, schema=db_landing_schema, if_exists='append', index=False)
        logger.info(f"Pokemon cards from {pokemon_card_file_path + pokemon_card_file_name} loaded successfully")
    except Exception as e:
        logger.error(f"Error loading pokemon cards: {e}")
        raise

def clear_landing_table(clear_landing_table_flag=True):
    try:
        if clear_landing_table_flag:
            truncate_table(db_landing_schema, db_landing_table, restart_identity=True)
            logger.info(f"{db_landing_table} landing table cleared successfully")
    except Exception as e:
        logger.error(f"Error clearing landing table: {e}")
        raise
