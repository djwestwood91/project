from references import *
import pandas as pd

def load_pokemon_cards_landing():
    try:
        logger.info(f"Reading source data from {pokemon_card_file_path + pokemon_card_file_name}")
        df = pd.read_excel(pokemon_card_file_path + pokemon_card_file_name, sheet_name=pokemon_card_sheet_name)
        
        # Define column mapping if needed
        landing_column_mapping = {'Card': 'card',
                                  'Set': 'card_set',
                                  'Year': 'card_year',
                                  'Graded': 'card_graded',
                                  'Grade': 'grade',
                                  'Grading Company': 'grading_company',
                                  'Grading Company Full Name': 'grading_company_full_name',
                                  'Grade Description': 'grade_description',
                                  'Grading Certification Number': 'grading_certification_number',
                                  'Graded Card URL': 'graded_card_url',
                                  'Holo': 'card_holo_flag',
                                  '1st Edition': 'card_first_edition_flag',
                                  'Promo': 'card_promo_flag',
                                  'Language': 'card_language',
                                  'Rarity': 'card_rarity',
                                  'Additional Details 1': 'card_additional_details_1',
                                  'Additional Details 2': 'card_additional_details_2',
                                  'Additional Details 3': 'card_additional_details_3',
                                  'Purchase Price GBP': 'card_purchase_price',
                                  'Postage Fees GBP': 'postage_fees',
                                  'Total GBP': 'card_total_gbp',
                                  'Date Purchased': 'card_date_purchased',
                                  'Source': 'card_source',
                                  'Seller': 'card_seller',
                                  'Website': 'website'}

        df_with_updated_mapping = df.rename(columns=landing_column_mapping)
        
        df_with_updated_mapping.to_sql('landing_pokemon_card', con=engine, schema=db_landing_schema, if_exists='replace', index=False)
        logger.info(f"Pokemon cards from {pokemon_card_file_path + pokemon_card_file_name} loaded successfully")
    except Exception as e:
        logger.error(f"Error loading pokemon cards: {e}")
        raise
