from references import *
import pandas as pd

def read_landing_table_for_seller_data():
    try:
        # Read the landing table from the database
        query = f"""SELECT distinct on (row_id, trim(lower(card_seller)), trim(lower(website)))
                           row_id,
                           card_seller as seller,
                           website,
                           country_id
                    FROM {DB_LANDING_SCHEMA}.{DB_LANDING_TABLE} lpc
                    LEFT JOIN {DB_MAIN_SCHEMA}.{DB_COUNTRY_LOOKUP_TABLE} cl ON lpc.seller_country = cl.country
                    WHERE nullif((trim(lower(card_seller))), '') IS NOT NULL
                    ORDER BY row_id, trim(lower(card_seller)), trim(lower(website));"""
        df = pd.read_sql(query, con=ENGINE)
        logger.info(f"{DB_LANDING_TABLE} table read successfully")
        return df
    except Exception as e:
        logger.error(f"Error reading landing table: {e}")
        raise

def perform_quality_checks_on_seller_data():
    try:
        # Example quality check: Ensure no null values in critical columns
        query = f"""SELECT COUNT(*) FROM {DB_MAIN_SCHEMA}.{DB_SELLER_TABLE} WHERE seller IS NULL OR country_id IS NULL"""
        result = pd.read_sql(query, con=ENGINE)
        # if the count of records with null values in critical columns is greater than 0, log a warning, otherwise log that the quality check passed
        if result.iloc[0, 0] > 0:
            logger.warning(f"{DB_SELLER_TABLE} Quality Check Failed: Found {result.iloc[0, 0]} records with null values in critical columns")
        else:
            logger.info(f"{DB_SELLER_TABLE} Quality Check Passed: No null values found in critical columns")
    except Exception as e:
        logger.error(f"Error performing quality checks on seller data: {e}")
        raise
    
def insert_seller_data():
    try:
        # Read the landing table data
        df = read_landing_table_for_seller_data()
        if df is not None:
            # Write the DataFrame to the main database table
            df.to_sql(DB_SELLER_TABLE, con=ENGINE, schema=DB_MAIN_SCHEMA, if_exists='append', index=False)
            logger.info(f"{DB_SELLER_TABLE} data inserted successfully into main database")
            perform_quality_checks_on_seller_data()
    except Exception as e:
        logger.error(f"Error inserting {DB_SELLER_TABLE} data: {e}")
        raise
