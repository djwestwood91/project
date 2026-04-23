from references import *

# Validate database identifiers to prevent SQL injection

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
                           ) as extra_details,
                           image_reference as card_image_reference
                    FROM {DB_LANDING_SCHEMA}.{DB_LANDING_TABLE} lpc
                    JOIN {DB_DIMENSIONS_SCHEMA}.{DB_SET_LOOKUP_TABLE} cs ON lpc.card_set = cs.name
                    JOIN {DB_DIMENSIONS_SCHEMA}.{DB_LANGUAGE_LOOKUP_TABLE} cl ON lpc.card_language = cl.name
                    JOIN {DB_DIMENSIONS_SCHEMA}.{DB_RARITY_LOOKUP_TABLE} cr ON lpc.card_rarity = cr.rarity
                    WHERE nullif(card, '') IS NOT NULL
                    ORDER BY row_id;"""
        df = pd.read_sql(query, con=ENGINE)
        logger.info(f"{DB_LANDING_TABLE} table read successfully")
        return df
    except Exception as e:
        logger.error(f"Error reading landing table: {str(e)}", exc_info=True)
        raise
    
def perform_quality_checks_on_card_data():
    try:        
        # Example quality check: Ensure no null values in critical columns
        query = f"""SELECT COUNT(*) FROM {DB_FACTS_SCHEMA}.{DB_CARD_TABLE} WHERE card IS NULL OR card_set_id IS NULL"""
        result = pd.read_sql(query, con=ENGINE)
        # if the count of records with null values in critical columns is greater than 0, log a warning, otherwise log that the quality check passed
        if result.iloc[0, 0] > 0:
            logger.warning(f"{DB_CARD_TABLE} Quality Check Failed: Found {result.iloc[0, 0]} records with null values in critical columns")
        else:
            logger.info(f"{DB_CARD_TABLE} Quality Check Passed: No null values found in critical columns")
    except Exception as e:
        logger.error(f"Error performing quality checks on card data: {str(e)}", exc_info=True)
        raise
    
def insert_card_data():
    try:
        # Read the landing table data
        df = read_landing_table_for_card_data()
        if df is not None:
            # Write the DataFrame to the main database table
            # Convert the extra_details column to JSON string format before inserting into the database
            df['extra_details'] = df['extra_details'].apply(lambda x: json.dumps(x) if isinstance(x, dict) else x)
            df.to_sql(DB_CARD_TABLE, con=ENGINE, schema=DB_FACTS_SCHEMA, if_exists='append', index=False)
            logger.info(f"{DB_CARD_TABLE} data inserted successfully into main database")
            perform_quality_checks_on_card_data()
    except Exception as e:
        logger.error(f"Error inserting {DB_CARD_TABLE} data: {str(e)}", exc_info=True)
        raise
