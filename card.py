from references import *
import pandas as pd

def read_landing_table_for_card_data():
    try:
        # Read the landing table from the database
        query = f"""SELECT card,
                           card_set,
                           card_year,
                           card_holo_flag,
                           card_first_edition_flag,
                           card_language,
                           card_additional_details_1,
                           card_additional_details_2
                    FROM {db_landing_schema}.{db_landing_table}"""
        df = pd.read_sql(query, con=engine)
        logger.info(f"{db_landing_table} table read successfully")
        return df
    except Exception as e:
        logger.error(f"Error reading landing table: {e}")
        return None
    
def insert_card_data():
    try:
        # Read the landing table data
        df = read_landing_table_for_card_data()
        if df is not None:
            # Write the DataFrame to the main database table
            df.to_sql(db_card_table, con=engine, schema=db_main_schema, if_exists='append', index=False)
            logger.info("Pokemon card data inserted successfully into main database")
    except Exception as e:
        logger.error(f"Error inserting pokemon card data: {e}")
