from references import *

def read_landing_table_for_purchase_data():
    try:
        # Read the landing table and match to card_instance_id - join on row_id for 1:1 mapping
        query = f"""select distinct
                    lpc.row_id,
                    ci.card_instance_id, 
                    cg.grade_id,
                    s.seller_id,
                    lpc.card_purchase_price as purchase_price,
                    lpc.postage_fees,
                    lpc.total_price,
                    cur.currency_id,
                    ps.purchase_source_id,
                    lpc.card_date_purchased as date_purchased
                FROM {DB_LANDING_SCHEMA}.{DB_LANDING_TABLE} lpc
                JOIN {DB_FACTS_SCHEMA}.{DB_CARD_TABLE} c ON c.card = lpc.card
                AND c.row_id = lpc.row_id
                JOIN {DB_FACTS_SCHEMA}.{DB_CARD_INSTANCE_TABLE} ci ON ci.card_id = c.card_id
                LEFT JOIN {DB_FACTS_SCHEMA}.{DB_CARD_GRADE_TABLE} cg ON cg.card_instance_id = ci.card_instance_id
                JOIN {DB_FACTS_SCHEMA}.{DB_SELLER_TABLE} s ON s.seller = lpc.card_seller 
                AND s.row_id = lpc.row_id
                LEFT JOIN {DB_DIMENSIONS_SCHEMA}.{DB_CURRENCY_LOOKUP_TABLE} cur ON cur.currency_code = lpc.card_currency
                LEFT JOIN {DB_DIMENSIONS_SCHEMA}.{DB_PURCHASE_SOURCE_LOOKUP_TABLE} ps ON ps."source" = lpc.card_source;"""
        df = pd.read_sql(query, con=ENGINE)
        logger.info(f"{DB_LANDING_TABLE} table read successfully for purchase data")
        return df
    except Exception as e:
        logger.error(f"Error reading landing table for purchase data: {str(e)}", exc_info=True)
        raise
    
def perform_quality_checks_on_purchase_data():
    try:
        # Example quality check: Ensure no null values in critical columns
        query = f"""SELECT COUNT(*) FROM {DB_FACTS_SCHEMA}.{DB_PURCHASE_TABLE} WHERE card_instance_id IS NULL OR seller_id IS NULL OR purchase_price IS NULL"""
        result = pd.read_sql(query, con=ENGINE)
        # if the count of records with null values in critical columns is greater than 0, log a warning, otherwise log that the quality check passed
        if result.iloc[0, 0] > 0:
            logger.warning(f"{DB_PURCHASE_TABLE} Quality Check Failed: Found {result.iloc[0, 0]} records with null values in critical columns")
        else:
            logger.info(f"{DB_PURCHASE_TABLE} Quality Check Passed: No null values found in critical columns")
    except Exception as e:
        logger.error(f"Error performing quality checks on purchase data: {str(e)}", exc_info=True)
        raise

def insert_purchase_data():
    try:
        # Read the landing table data
        df = read_landing_table_for_purchase_data()
        if df is not None:
            # Write the DataFrame to the main database table
            df.to_sql(DB_PURCHASE_TABLE, con=ENGINE, schema=DB_FACTS_SCHEMA, if_exists='append', index=False)
            logger.info(f"{DB_PURCHASE_TABLE} data inserted successfully into main database")
            perform_quality_checks_on_purchase_data()
    except Exception as e:
        logger.error(f"Error inserting {DB_PURCHASE_TABLE} data: {str(e)}", exc_info=True)
        raise
