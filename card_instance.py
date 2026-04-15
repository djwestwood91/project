from references import *
import pandas as pd
import json

def read_landing_table_for_card_instance_data():
    try:
        # Read the landing table from the database
        query = f"""SELECT distinct on (lpc.row_id)
                           lpc.row_id,
                           card_id
                    FROM {db_landing_schema}.{db_landing_table} lpc
                    JOIN {db_main_schema}.{db_card_table} cs ON lpc.card = cs.card
                    ORDER BY lpc.row_id;"""
        df = pd.read_sql(query, con=engine)
        logger.info(f"{db_landing_table} table read successfully")
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
            df.to_sql(db_card_instance_table, con=engine, schema=db_main_schema, if_exists='append', index=False)
            logger.info(f"{db_card_instance_table} data inserted successfully into main database")
    except Exception as e:
        logger.error(f"Error inserting {db_card_instance_table} data: {e}")
        raise
