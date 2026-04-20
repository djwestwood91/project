from references import *
from utils.db_utils import validate_identifiers

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

def output_card_data_to_excel_for_tableau():
    try:
        # Validate database identifiers to prevent SQL injection
        validate_identifiers(DB_MAIN_SCHEMA, DB_CARD_INSTANCE_TABLE)
        
        # Read the card instance data from the main database table
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
        logger.info(f"{DB_CARD_TABLE} data read successfully for Excel output")
        
        # Convert timezone-aware datetimes to timezone-naive for Excel compatibility
        df = convert_timezone_aware_datetimes(df)
        
        # Output the DataFrame to an Excel file
        df.to_excel(POKEMON_CARD_OUTPUT_FILE_PATH + POKEMON_CARD_OUTPUT_FILE_NAME, index=False, sheet_name=DB_CARD_TABLE)
        logger.info(f"Card data successfully written to {POKEMON_CARD_OUTPUT_FILE_PATH + POKEMON_CARD_OUTPUT_FILE_NAME}")
    except Exception as e:
        logger.error(f"Error outputting card data to Excel: {str(e)}", exc_info=True)
        raise

def output_card_instance_data_to_excel_for_tableau():
    try:
        # Validate database identifiers to prevent SQL injection
        validate_identifiers(DB_MAIN_SCHEMA, DB_CARD_INSTANCE_TABLE)
        
        # Read the card instance data from the main database table
        query = f"""SELECT card_instance_id,
                           row_id, 
                           card_id
                    FROM {DB_MAIN_SCHEMA}.{DB_CARD_INSTANCE_TABLE}"""
        df = pd.read_sql(query, con=ENGINE)
        logger.info(f"{DB_CARD_INSTANCE_TABLE} data read successfully for Excel output")
        
        # Convert timezone-aware datetimes to timezone-naive for Excel compatibility
        df = convert_timezone_aware_datetimes(df)
        
        # Output the DataFrame to an Excel file
        df.to_excel(POKEMON_CARD_OUTPUT_FILE_PATH + POKEMON_CARD_OUTPUT_FILE_NAME, index=False, sheet_name=DB_CARD_INSTANCE_TABLE)
        logger.info(f"Card instance data successfully written to {POKEMON_CARD_OUTPUT_FILE_PATH + POKEMON_CARD_OUTPUT_FILE_NAME}")
    except Exception as e:
        logger.error(f"Error outputting card instance data to Excel: {str(e)}", exc_info=True)
        raise

def output_card_grade_data_to_excel_for_tableau():
    try:
        # Validate database identifiers to prevent SQL injection
        validate_identifiers(DB_MAIN_SCHEMA, DB_CARD_GRADE_TABLE)
        
        # Read the card grade data from the main database table
        query = f"""SELECT grade_id,
                           card_instance_id, 
                           row_id, 
                           grade_description_id, 
                           grading_certification_number, 
                           graded_card_url
                    FROM {DB_MAIN_SCHEMA}.{DB_CARD_GRADE_TABLE}"""
        query = f"""SELECT * FROM {DB_MAIN_SCHEMA}.{DB_CARD_GRADE_TABLE}"""
        df = pd.read_sql(query, con=ENGINE)
        logger.info(f"{DB_CARD_GRADE_TABLE} data read successfully for Excel output")
        
        # Convert timezone-aware datetimes to timezone-naive for Excel compatibility
        df = convert_timezone_aware_datetimes(df)
        
        
        # Output the DataFrame to an Excel file
        df.to_excel(POKEMON_CARD_OUTPUT_FILE_PATH + POKEMON_CARD_OUTPUT_FILE_NAME, index=False, sheet_name=DB_CARD_GRADE_TABLE)
        logger.info(f"Card grade data successfully written to {POKEMON_CARD_OUTPUT_FILE_PATH + POKEMON_CARD_OUTPUT_FILE_NAME}")
    except Exception as e:
        logger.error(f"Error outputting card grade data to Excel: {str(e)}", exc_info=True)
        raise

def output_seller_data_to_excel_for_tableau():
    try:
        # Validate database identifiers to prevent SQL injection
        validate_identifiers(DB_MAIN_SCHEMA, DB_SELLER_TABLE)
                   
        # Read the seller data from the main database table
        query = f"""SELECT seller_id,
                           row_id, 
                           seller, 
                           website, 
                           country_id
                    FROM {DB_MAIN_SCHEMA}.{DB_SELLER_TABLE}"""
        df = pd.read_sql(query, con=ENGINE)
        logger.info(f"{DB_SELLER_TABLE} data read successfully for Excel output")
        
        # Convert timezone-aware datetimes to timezone-naive for Excel compatibility
        df = convert_timezone_aware_datetimes(df)
        
        # Output the DataFrame to an Excel file
        df.to_excel(POKEMON_CARD_OUTPUT_FILE_PATH + POKEMON_CARD_OUTPUT_FILE_NAME, index=False, sheet_name=DB_SELLER_TABLE)
        logger.info(f"Seller data successfully written to {POKEMON_CARD_OUTPUT_FILE_PATH + POKEMON_CARD_OUTPUT_FILE_NAME}")
    except Exception as e:
        logger.error(f"Error outputting seller data to Excel: {str(e)}", exc_info=True)
        raise

def output_purchase_data_to_excel_for_tableau():
    try:
        # Validate database identifiers to prevent SQL injection
        validate_identifiers(DB_MAIN_SCHEMA, DB_PURCHASE_TABLE)
        
        # Read the purchase data from the main database table
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
        logger.info(f"{DB_PURCHASE_TABLE} data read successfully for Excel output")
        
        # Convert timezone-aware datetimes to timezone-naive for Excel compatibility
        df = convert_timezone_aware_datetimes(df)
        
        # Output the DataFrame to an Excel file
        df.to_excel(POKEMON_CARD_OUTPUT_FILE_PATH + POKEMON_CARD_OUTPUT_FILE_NAME, index=False, sheet_name=DB_PURCHASE_TABLE)
        logger.info(f"Purchase data successfully written to {POKEMON_CARD_OUTPUT_FILE_PATH + POKEMON_CARD_OUTPUT_FILE_NAME}")
    except Exception as e:
        logger.error(f"Error outputting purchase data to Excel: {str(e)}", exc_info=True)
        raise
