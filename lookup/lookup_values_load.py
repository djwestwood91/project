from references import *
from utils.db_utils import truncate_table

# Functions to read from landing table and insert into lookup tables
def read_landing_table_for_card_language_lookup():
    try:
        # Read the landing table from the database
        query = f"""SELECT distinct card_language as name
                    FROM {DB_LANDING_SCHEMA}.{DB_LANDING_TABLE}
                    WHERE nullif(card_language, '') IS NOT NULL"""

        df = pd.read_sql(query, con=ENGINE)
        logger.info(f"{DB_LANDING_TABLE} table read successfully")
        return df
    except Exception as e:
        logger.error(f"Error reading landing table: {e}")
        raise
    
def insert_language_lookup_data():
    try:
        # Read the landing table data
        df = read_landing_table_for_card_language_lookup()
        if df is not None:
            # Clean/reload lookup table
            truncate_table(DB_DIMENSIONS_SCHEMA, DB_LANGUAGE_LOOKUP_TABLE, restart_identity=True)

            # Write the DataFrame to the main database table
            df.to_sql(DB_LANGUAGE_LOOKUP_TABLE, con=ENGINE, schema=DB_DIMENSIONS_SCHEMA, if_exists='append', index=False)
            logger.info("Pokemon card language lookup data inserted successfully into main database")
    except Exception as e:
        logger.error(f"Error inserting pokemon card language lookup data: {str(e)}", exc_info=True)
        raise

# Functions for card set lookup
def read_landing_table_for_card_set_lookup():
    try:
        # Read the landing table from the database
        query = f"""SELECT distinct card_set as name, 
                           card_year as year
                    FROM {DB_LANDING_SCHEMA}.{DB_LANDING_TABLE}
                    WHERE nullif(card_set, '') IS NOT NULL
                    ORDER BY name, year"""
        
        df = pd.read_sql(query, con=ENGINE)
        logger.info(f"{DB_LANDING_TABLE} table read successfully")
        return df
    except Exception as e:
        logger.error(f"Error reading landing table: {e}")
        raise
    
def insert_set_lookup_data():
    try:
        # Read the landing table data
        df = read_landing_table_for_card_set_lookup()
        if df is not None:
            # Clean/reload lookup table
            truncate_table(DB_DIMENSIONS_SCHEMA, DB_SET_LOOKUP_TABLE, restart_identity=True)

            # Write the DataFrame to the main database table
            df.to_sql(DB_SET_LOOKUP_TABLE, con=ENGINE, schema=DB_DIMENSIONS_SCHEMA, if_exists='append', index=False)
            logger.info("Pokemon card set lookup data inserted successfully into main database")
    except Exception as e:
        logger.error(f"Error inserting pokemon card set lookup data: {str(e)}", exc_info=True)
        raise

def read_landing_table_for_grading_company_lookup():
    try:
        # Read the landing table from the database
        query = f"""SELECT distinct grading_company as company,
                           grading_company_full_name as company_full_name
                    FROM {DB_LANDING_SCHEMA}.{DB_LANDING_TABLE}
                    WHERE nullif(grading_company, '') IS NOT NULL"""
        
        df = pd.read_sql(query, con=ENGINE)
        logger.info(f"{DB_LANDING_TABLE} table read successfully")
        return df
    except Exception as e:
        logger.error(f"Error reading landing table: {str(e)}", exc_info=True)
        raise
    
def insert_grading_company_lookup_data():
    try:
        # Read the landing table data
        df = read_landing_table_for_grading_company_lookup()
        if df is not None:
            # Clean/reload lookup table
            truncate_table(DB_DIMENSIONS_SCHEMA, DB_GRADING_COMPANY_LOOKUP_TABLE, restart_identity=True)

            # Write the DataFrame to the main database table
            df.to_sql(DB_GRADING_COMPANY_LOOKUP_TABLE, con=ENGINE, schema=DB_DIMENSIONS_SCHEMA, if_exists='append', index=False)
            logger.info("Pokemon card grading company lookup data inserted successfully into main database")
    except Exception as e:
        logger.error(f"Error inserting pokemon card grading company lookup data: {str(e)}", exc_info=True)
        raise

def read_landing_table_for_grading_description_lookup():
    try:
        # Read the landing table from the database
        query = f"""SELECT distinct gc.grading_company_id, 
                           lpc.grade,
                           lpc.grade_description
                    FROM {DB_LANDING_SCHEMA}.{DB_LANDING_TABLE} lpc
                    JOIN {DB_DIMENSIONS_SCHEMA}.{DB_GRADING_COMPANY_LOOKUP_TABLE} gc ON lpc.grading_company = gc.company
                    WHERE nullif(lpc.grade_description, '') IS NOT NULL"""
        
        df = pd.read_sql(query, con=ENGINE)
        logger.info(f"{DB_LANDING_TABLE} table read successfully")
        return df
    except Exception as e:
        logger.error(f"Error reading landing table: {str(e)}", exc_info=True)
        raise
    
def insert_grade_description_lookup_data():
    try:
        # Read the landing table data
        df = read_landing_table_for_grading_description_lookup()
        if df is not None:
            # Clean/reload lookup table
            truncate_table(DB_DIMENSIONS_SCHEMA, DB_GRADE_DESCRIPTION_LOOKUP_TABLE, restart_identity=True)

            # Write the DataFrame to the main database table
            df.to_sql(DB_GRADE_DESCRIPTION_LOOKUP_TABLE, con=ENGINE, schema=DB_DIMENSIONS_SCHEMA, if_exists='append', index=False)
            logger.info("Pokemon card grading description lookup data inserted successfully into main database")
    except Exception as e:
        logger.error(f"Error inserting pokemon card grading description lookup data: {str(e)}", exc_info=True)
        raise

def read_landing_table_for_rarity_lookup():
    try:
        # Read the landing table from the database
        query = f"""SELECT distinct card_rarity as rarity
                    FROM {DB_LANDING_SCHEMA}.{DB_LANDING_TABLE}
                    WHERE nullif(card_rarity, '') IS NOT NULL"""
        
        df = pd.read_sql(query, con=ENGINE)
        logger.info(f"{DB_LANDING_TABLE} table read successfully")
        return df
    except Exception as e:
        logger.error(f"Error reading landing table: {str(e)}", exc_info=True)
        raise
    
def insert_rarity_lookup_data():
    try:
        # Read the landing table data
        df = read_landing_table_for_rarity_lookup()
        if df is not None:
            # Clean/reload lookup table
            truncate_table(DB_DIMENSIONS_SCHEMA, DB_RARITY_LOOKUP_TABLE, restart_identity=True)

            # Write the DataFrame to the main database table
            df.to_sql(DB_RARITY_LOOKUP_TABLE, con=ENGINE, schema=DB_DIMENSIONS_SCHEMA, if_exists='append', index=False)
            logger.info("Pokemon card rarity lookup data inserted successfully into main database")
    except Exception as e:
        logger.error(f"Error inserting pokemon card rarity lookup data: {str(e)}", exc_info=True)
        raise

# Functions for currency lookup
def read_landing_table_for_currency_lookup():
    try:
        # Read the landing table from the database
        query = f"""SELECT distinct card_currency as currency_code
                    FROM {DB_LANDING_SCHEMA}.{DB_LANDING_TABLE}
                    WHERE nullif(card_currency, '') IS NOT NULL
                    ORDER BY currency_code"""
        
        df = pd.read_sql(query, con=ENGINE)
        logger.info(f"Currency lookup data read successfully from {DB_LANDING_TABLE}")
        return df
    except Exception as e:
        logger.error(f"Error reading landing table for currency: {str(e)}", exc_info=True)
        raise
    
def insert_currency_lookup_data():
    try:
        # Read the landing table data
        df = read_landing_table_for_currency_lookup()
        if df is not None and not df.empty:
            # Clean/reload lookup table
            truncate_table(DB_DIMENSIONS_SCHEMA, DB_CURRENCY_LOOKUP_TABLE, restart_identity=True)

            # Write the DataFrame to the main database table
            df.to_sql(DB_CURRENCY_LOOKUP_TABLE, con=ENGINE, schema=DB_DIMENSIONS_SCHEMA, if_exists='append', index=False)
            logger.info("Currency lookup data inserted successfully into main database")
        else:
            logger.warning("No currency data found in landing table")
    except Exception as e:
        logger.error(f"Error inserting currency lookup data: {str(e)}", exc_info=True)
        raise

# Functions for purchase source lookup
def read_landing_table_for_purchase_source_lookup():
    try:
        # Read the landing table from the database
        query = f"""SELECT distinct card_source as source
                    FROM {DB_LANDING_SCHEMA}.{DB_LANDING_TABLE}
                    WHERE nullif(card_source, '') IS NOT NULL
                    ORDER BY source"""
        
        df = pd.read_sql(query, con=ENGINE)
        logger.info(f"Purchase source lookup data read successfully from {DB_LANDING_TABLE}")
        return df
    except Exception as e:
        logger.error(f"Error reading landing table for purchase source: {str(e)}", exc_info=True)
        raise
    
def insert_purchase_source_lookup_data():
    try:
        # Read the landing table data
        df = read_landing_table_for_purchase_source_lookup()
        if df is not None and not df.empty:
            # Clean/reload lookup table
            truncate_table(DB_DIMENSIONS_SCHEMA, DB_PURCHASE_SOURCE_LOOKUP_TABLE, restart_identity=True)

            # Write the DataFrame to the main database table
            df.to_sql(DB_PURCHASE_SOURCE_LOOKUP_TABLE, con=ENGINE, schema=DB_DIMENSIONS_SCHEMA, if_exists='append', index=False)
            logger.info("Purchase source lookup data inserted successfully into main database")
        else:
            logger.warning("No purchase source data found in landing table")
    except Exception as e:
        logger.error(f"Error inserting purchase source lookup data: {str(e)}", exc_info=True)
        raise

def read_landing_table_for_country_lookup():
    try:
        # Read the landing table from the database
        query = f"""SELECT distinct seller_country as country
                    FROM {DB_LANDING_SCHEMA}.{DB_LANDING_TABLE}
                    WHERE nullif(seller_country, '') IS NOT NULL
                    ORDER BY seller_country"""
        
        df = pd.read_sql(query, con=ENGINE)
        logger.info(f"Country lookup data read successfully from {DB_LANDING_TABLE}")
        return df
    except Exception as e:
        logger.error(f"Error reading landing table for country lookup: {str(e)}", exc_info=True)
        raise

def insert_country_lookup_data():
    try:
        # Read the landing table data
        df = read_landing_table_for_country_lookup()
        if df is not None and not df.empty:
            # Clean/reload country lookup table
            truncate_table(DB_DIMENSIONS_SCHEMA, DB_COUNTRY_LOOKUP_TABLE, restart_identity=True)

            # Write the DataFrame to the main database table
            df.to_sql(DB_COUNTRY_LOOKUP_TABLE, con=ENGINE, schema=DB_DIMENSIONS_SCHEMA, if_exists='append', index=False)
            logger.info("Country lookup data inserted successfully into main database")
        else:
            logger.warning("No country lookup data found in landing table")
    except Exception as e:
        logger.error(f"Error inserting country lookup data: {str(e)}", exc_info=True)
        raise
