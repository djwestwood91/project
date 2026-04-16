from references import *

def read_landing_table_for_card_instance_data():
    try:
        # Read the landing table from the database
        query = f"""SELECT distinct on (lpc.row_id)
                           lpc.row_id,
                           card_id
                    FROM {DB_LANDING_SCHEMA}.{DB_LANDING_TABLE} lpc
                    JOIN {DB_MAIN_SCHEMA}.{DB_CARD_TABLE} cs ON lpc.card = cs.card
                    ORDER BY lpc.row_id;"""
        df = pd.read_sql(query, con=ENGINE)
        logger.info(f"{DB_LANDING_TABLE} table read successfully")
        return df
    except Exception as e:
        logger.error(f"Error reading landing table: {e}")
        raise
    
def insert_card_instance_data():
    try:
        # Read the landing table data
        df = read_landing_table_for_card_instance_data()
        if df is not None:
            # Write the DataFrame to the main database table
            df.to_sql(DB_CARD_INSTANCE_TABLE, con=ENGINE, schema=DB_MAIN_SCHEMA, if_exists='append', index=False)
            logger.info(f"{DB_CARD_INSTANCE_TABLE} data inserted successfully into main database")
    except Exception as e:
        logger.error(f"Error inserting {DB_CARD_INSTANCE_TABLE} data: {e}")
        raise
