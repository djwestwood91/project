from references import *
from utils.db_utils import validate_identifiers
import os
from openpyxl.utils import get_column_letter

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

def read_card_set_lookup():
    """Read card set lookup data from the database"""
    validate_identifiers(DB_MAIN_SCHEMA, DB_SET_LOOKUP_TABLE)
    query = f"""SELECT card_set_id,
                       "name",
                       year 
                FROM {DB_MAIN_SCHEMA}.{DB_SET_LOOKUP_TABLE}"""
    df = pd.read_sql(query, con=ENGINE)
    return df

def read_language_lookup():
    """Read language lookup data from the database"""
    validate_identifiers(DB_MAIN_SCHEMA, DB_LANGUAGE_LOOKUP_TABLE)
    query = f"""SELECT language_id,
                       "name"
                FROM {DB_MAIN_SCHEMA}.{DB_LANGUAGE_LOOKUP_TABLE}"""
    df = pd.read_sql(query, con=ENGINE)
    return df

def read_rarity_lookup():
    """Read rarity lookup data from the database"""
    validate_identifiers(DB_MAIN_SCHEMA, DB_RARITY_LOOKUP_TABLE)
    query = f"""SELECT rarity_id,
                       rarity
                FROM {DB_MAIN_SCHEMA}.{DB_RARITY_LOOKUP_TABLE}"""
    df = pd.read_sql(query, con=ENGINE)
    return df

def read_grading_company_lookup():
    """Read grading company lookup data from the database"""
    validate_identifiers(DB_MAIN_SCHEMA, DB_GRADING_COMPANY_LOOKUP_TABLE)
    query = f"""SELECT grading_company_id,
                       company,
                       company_full_name
    FROM {DB_MAIN_SCHEMA}.{DB_GRADING_COMPANY_LOOKUP_TABLE}"""
    df = pd.read_sql(query, con=ENGINE)
    return df

def read_grade_description_lookup():
    """Read grade description lookup data from the database"""
    validate_identifiers(DB_MAIN_SCHEMA, DB_GRADE_DESCRIPTION_LOOKUP_TABLE)
    query = f"""SELECT grade_description_id,
                       grading_company_id,
                       grade,
                       grade_description
                FROM {DB_MAIN_SCHEMA}.{DB_GRADE_DESCRIPTION_LOOKUP_TABLE}"""
    df = pd.read_sql(query, con=ENGINE)
    return df

def read_currency_lookup():
    """Read currency lookup data from the database"""
    validate_identifiers(DB_MAIN_SCHEMA, DB_CURRENCY_LOOKUP_TABLE)
    query = f"""SELECT currency_id,
                       currency_code
                FROM {DB_MAIN_SCHEMA}.{DB_CURRENCY_LOOKUP_TABLE}"""
    df = pd.read_sql(query, con=ENGINE)
    return df

def read_purchase_source_lookup():
    """Read purchase source lookup data from the database"""
    validate_identifiers(DB_MAIN_SCHEMA, DB_PURCHASE_SOURCE_LOOKUP_TABLE)
    query = f"""SELECT purchase_source_id,
                       source                
                FROM {DB_MAIN_SCHEMA}.{DB_PURCHASE_SOURCE_LOOKUP_TABLE}"""
    df = pd.read_sql(query, con=ENGINE)
    return df

def read_country_lookup():
    """Read country lookup data from the database"""
    validate_identifiers(DB_MAIN_SCHEMA, DB_COUNTRY_LOOKUP_TABLE)
    query = f"""SELECT country_id,
                       country
                FROM {DB_MAIN_SCHEMA}.{DB_COUNTRY_LOOKUP_TABLE}"""
    df = pd.read_sql(query, con=ENGINE)
    return df

def output_all_data_to_excel_for_tableau():
    """Write all tables to a single Excel file with multiple sheets"""
    try:
        file_path = POKEMON_CARD_OUTPUT_FILE_PATH + POKEMON_CARD_OUTPUT_FILE_NAME
        
        # Create the output directory if it doesn't exist
        os.makedirs(POKEMON_CARD_OUTPUT_FILE_PATH, exist_ok=True)
        
        with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
            # ===== FACT TABLES =====
            
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
            
            # ===== DIMENSIONAL TABLES =====
            
            # Card Set lookup
            logger.info(f"Reading {DB_SET_LOOKUP_TABLE} lookup data...")
            card_set_df = read_card_set_lookup()
            card_set_df = convert_timezone_aware_datetimes(card_set_df)
            card_set_df.to_excel(writer, sheet_name=DB_SET_LOOKUP_TABLE, index=False)
            logger.info(f"{DB_SET_LOOKUP_TABLE} lookup written to Excel")
            
            # Language lookup
            logger.info(f"Reading {DB_LANGUAGE_LOOKUP_TABLE} lookup data...")
            language_df = read_language_lookup()
            language_df = convert_timezone_aware_datetimes(language_df)
            language_df.to_excel(writer, sheet_name=DB_LANGUAGE_LOOKUP_TABLE, index=False)
            logger.info(f"{DB_LANGUAGE_LOOKUP_TABLE} lookup written to Excel")
            
            # Rarity lookup
            logger.info(f"Reading {DB_RARITY_LOOKUP_TABLE} lookup data...")
            rarity_df = read_rarity_lookup()
            rarity_df = convert_timezone_aware_datetimes(rarity_df)
            rarity_df.to_excel(writer, sheet_name=DB_RARITY_LOOKUP_TABLE, index=False)
            logger.info(f"{DB_RARITY_LOOKUP_TABLE} lookup written to Excel")
            
            # Grading Company lookup
            logger.info(f"Reading {DB_GRADING_COMPANY_LOOKUP_TABLE} lookup data...")
            grading_company_df = read_grading_company_lookup()
            grading_company_df = convert_timezone_aware_datetimes(grading_company_df)
            grading_company_df.to_excel(writer, sheet_name=DB_GRADING_COMPANY_LOOKUP_TABLE, index=False)
            logger.info(f"{DB_GRADING_COMPANY_LOOKUP_TABLE} lookup written to Excel")
            
            # Grade Description lookup
            logger.info(f"Reading {DB_GRADE_DESCRIPTION_LOOKUP_TABLE} lookup data...")
            grade_description_df = read_grade_description_lookup()
            grade_description_df = convert_timezone_aware_datetimes(grade_description_df)
            grade_description_df.to_excel(writer, sheet_name=DB_GRADE_DESCRIPTION_LOOKUP_TABLE, index=False)
            logger.info(f"{DB_GRADE_DESCRIPTION_LOOKUP_TABLE} lookup written to Excel")
            
            # Currency lookup
            logger.info(f"Reading {DB_CURRENCY_LOOKUP_TABLE} lookup data...")
            currency_df = read_currency_lookup()
            currency_df = convert_timezone_aware_datetimes(currency_df)
            currency_df.to_excel(writer, sheet_name=DB_CURRENCY_LOOKUP_TABLE, index=False)
            logger.info(f"{DB_CURRENCY_LOOKUP_TABLE} lookup written to Excel")
            
            # Purchase Source lookup
            logger.info(f"Reading {DB_PURCHASE_SOURCE_LOOKUP_TABLE} lookup data...")
            purchase_source_df = read_purchase_source_lookup()
            purchase_source_df = convert_timezone_aware_datetimes(purchase_source_df)
            purchase_source_df.to_excel(writer, sheet_name=DB_PURCHASE_SOURCE_LOOKUP_TABLE, index=False)
            logger.info(f"{DB_PURCHASE_SOURCE_LOOKUP_TABLE} lookup written to Excel")
            
            # Country lookup
            logger.info(f"Reading {DB_COUNTRY_LOOKUP_TABLE} lookup data...")
            country_df = read_country_lookup()
            country_df = convert_timezone_aware_datetimes(country_df)
            country_df.to_excel(writer, sheet_name=DB_COUNTRY_LOOKUP_TABLE, index=False)
            logger.info(f"{DB_COUNTRY_LOOKUP_TABLE} lookup written to Excel")
            
            # Autosize columns for all sheets
            logger.info("Autosizing columns for all sheets...")
            for sheet_name, sheet in writer.sheets.items():
                logger.info(f"Autosizing columns for sheet: {sheet_name}")
                for column in sheet.columns:
                    max_length = 0
                    column_letter = get_column_letter(column[0].column)
                    for cell in column:
                        try:
                            if len(str(cell.value)) > max_length:
                                max_length = len(str(cell.value))
                        except:
                            pass
                    # Set column width with some padding
                    adjusted_width = (max_length + 2)
                    sheet.column_dimensions[column_letter].width = adjusted_width
        
        logger.info(f"All data successfully written to {file_path}")
    except Exception as e:
        logger.error(f"Error outputting data to Excel: {str(e)}", exc_info=True)
        raise
