from references import *
from utils.db_utils import validate_identifiers
import os

def convert_timezone_aware_datetimes(df):
    """
    Convert timezone-aware datetime columns to timezone-naive.
    Excel does not support timezone-aware datetimes, so this function
    removes timezone information before writing to Excel.
    """
    for col in df.columns:
        if pd.api.types.is_datetime64tz_dtype(df[col]):
            df[col] = df[col].dt.tz_localize(None)
    return df

def read_card_data():
    """Read card data from the database"""
    validate_identifiers(DB_MAIN_SCHEMA, DB_CARD_TABLE)
    query = f"""SELECT card_id,
                       row_id, 
                       card, 
                       card_set_id, 
                       card_holo_flag, 
                       card_first_edition_flag, 
                       card_promo_flag, 
                       card_language_id, 
                       card_rarity_id, 
                       extra_details, 
                       card_image_reference
                FROM {DB_MAIN_SCHEMA}.{DB_CARD_TABLE}"""
    df = pd.read_sql(query, con=ENGINE)
    return df

def read_card_instance_data():
    """Read card instance data from the database"""
    validate_identifiers(DB_MAIN_SCHEMA, DB_CARD_INSTANCE_TABLE)
    query = f"""SELECT card_instance_id,
                       row_id, 
                       card_id
                FROM {DB_MAIN_SCHEMA}.{DB_CARD_INSTANCE_TABLE}"""
    df = pd.read_sql(query, con=ENGINE)
    return df

def read_card_grade_data():
    """Read card grade data from the database"""
    validate_identifiers(DB_MAIN_SCHEMA, DB_CARD_GRADE_TABLE)
    query = f"""SELECT grade_id,
                       card_instance_id, 
                       row_id, 
                       grade_description_id, 
                       grading_certification_number, 
                       graded_card_url
                FROM {DB_MAIN_SCHEMA}.{DB_CARD_GRADE_TABLE}"""
    df = pd.read_sql(query, con=ENGINE)
    return df

def read_seller_data():
    """Read seller data from the database"""
    validate_identifiers(DB_MAIN_SCHEMA, DB_SELLER_TABLE)
    query = f"""SELECT seller_id,
                       row_id, 
                       seller, 
                       website, 
                       country_id
                FROM {DB_MAIN_SCHEMA}.{DB_SELLER_TABLE}"""
    df = pd.read_sql(query, con=ENGINE)
    return df

def read_purchase_data():
    """Read purchase data from the database"""
    validate_identifiers(DB_MAIN_SCHEMA, DB_PURCHASE_TABLE)
    query = f"""SELECT purchase_id,
                       row_id, 
                       card_instance_id, 
                       grade_id, 
                       seller_id, 
                       purchase_price, 
                       postage_fees, 
                       total_price, 
                       currency_id, 
                       purchase_source_id, 
                       date_purchased
                FROM {DB_MAIN_SCHEMA}.{DB_PURCHASE_TABLE}"""
    df = pd.read_sql(query, con=ENGINE)
    return df

def output_all_data_to_excel_for_tableau():
    # Write all tables to a single Excel file with multiple sheets
    try:
        file_path = POKEMON_CARD_OUTPUT_FILE_PATH + POKEMON_CARD_OUTPUT_FILE_NAME
        
        # Create the output directory if it doesn't exist
        os.makedirs(POKEMON_CARD_OUTPUT_FILE_PATH, exist_ok=True)
        
        with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
            # Card data
            logger.info(f"Reading {DB_CARD_TABLE} data...")
            card_df = read_card_data()
            card_df = convert_timezone_aware_datetimes(card_df)
            card_df.to_excel(writer, sheet_name=DB_CARD_TABLE, index=False)
            logger.info(f"{DB_CARD_TABLE} data written to Excel")
            
            # Card Instance data
            logger.info(f"Reading {DB_CARD_INSTANCE_TABLE} data...")
            card_instance_df = read_card_instance_data()
            card_instance_df = convert_timezone_aware_datetimes(card_instance_df)
            card_instance_df.to_excel(writer, sheet_name=DB_CARD_INSTANCE_TABLE, index=False)
            logger.info(f"{DB_CARD_INSTANCE_TABLE} data written to Excel")
            
            # Card Grade data
            logger.info(f"Reading {DB_CARD_GRADE_TABLE} data...")
            card_grade_df = read_card_grade_data()
            card_grade_df = convert_timezone_aware_datetimes(card_grade_df)
            card_grade_df.to_excel(writer, sheet_name=DB_CARD_GRADE_TABLE, index=False)
            logger.info(f"{DB_CARD_GRADE_TABLE} data written to Excel")
            
            # Seller data
            logger.info(f"Reading {DB_SELLER_TABLE} data...")
            seller_df = read_seller_data()
            seller_df = convert_timezone_aware_datetimes(seller_df)
            seller_df.to_excel(writer, sheet_name=DB_SELLER_TABLE, index=False)
            logger.info(f"{DB_SELLER_TABLE} data written to Excel")
            
            # Purchase data
            logger.info(f"Reading {DB_PURCHASE_TABLE} data...")
            purchase_df = read_purchase_data()
            purchase_df = convert_timezone_aware_datetimes(purchase_df)
            purchase_df.to_excel(writer, sheet_name=DB_PURCHASE_TABLE, index=False)
            logger.info(f"{DB_PURCHASE_TABLE} data written to Excel")
        
        logger.info(f"All data successfully written to {file_path}")
    except Exception as e:
        logger.error(f"Error outputting data to Excel: {str(e)}", exc_info=True)
        raise
