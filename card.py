from references import *
import pandas as pd
import json

def read_landing_table_for_card_data():
    try:
        # Read the landing table from the database
        query = f"""SELECT distinct on (row_id)
                           row_id,
                           card,
                           card_set_id,
                           card_holo_flag,
                           card_first_edition_flag,
                           card_promo_flag,
                           language_id as card_language_id,
                           rarity_id as card_rarity_id,
                           json_build_object(
                               'detail_1', card_additional_details_1,
                               'detail_2', card_additional_details_2,
                               'detail_3', card_additional_details_3
                           ) as extra_details
                    FROM {db_landing_schema}.{db_landing_table} lpc
                    JOIN {db_main_schema}.{db_set_lookup_table} cs ON lpc.card_set = cs.name
                    JOIN {db_main_schema}.{db_language_lookup_table} cl ON lpc.card_language = cl.name
                    JOIN {db_main_schema}.{db_rarity_lookup_table} cr ON lpc.card_rarity = cr.rarity
                    WHERE nullif(card, '') IS NOT NULL
                    ORDER BY row_id;"""
        df = pd.read_sql(query, con=engine)
        logger.info(f"{db_landing_table} table read successfully")
        return df
    except Exception as e:
        logger.error(f"Error reading landing table: {e}")
        raise
    
def perform_quality_checks_on_card_data():
    try:
        # Example quality check: Ensure no null values in critical columns
        query = f"""SELECT COUNT(*) FROM {db_main_schema}.{db_card_table} WHERE card IS NULL OR card_set_id IS NULL"""
        result = pd.read_sql(query, con=engine)
        # if the count of records with null values in critical columns is greater than 0, log a warning, otherwise log that the quality check passed
        if result.iloc[0, 0] > 0:
            logger.warning(f"{db_card_table} Quality Check Failed: Found {result.iloc[0, 0]} records with null values in critical columns")
        else:
            logger.info(f"{db_card_table} Quality Check Passed: No null values found in critical columns")
    except Exception as e:
        logger.error(f"Error performing quality checks on card data: {e}")
        raise
    
def insert_card_data():
    try:
        # Read the landing table data
        df = read_landing_table_for_card_data()
        if df is not None:
            # Write the DataFrame to the main database table
            # Convert the extra_details column to JSON string format before inserting into the database
            df['extra_details'] = df['extra_details'].apply(lambda x: json.dumps(x) if isinstance(x, dict) else x)
            df.to_sql(db_card_table, con=engine, schema=db_main_schema, if_exists='append', index=False)
            logger.info(f"{db_card_table} data inserted successfully into main database")
            perform_quality_checks_on_card_data()
    except Exception as e:
        logger.error(f"Error inserting {db_card_table} data: {e}")
        raise
