from references import *
import pandas as pd
from sqlalchemy import text

# Helper: truncate a target table (keeps schema, resets identities)
def truncate_table(schema: str, table: str, restart_identity: bool = True, cascade: bool = True):
    stmt = f"TRUNCATE TABLE {schema}.{table}"
    if restart_identity:
        stmt += " RESTART IDENTITY"
    if cascade:
        stmt += " CASCADE"

    with engine.begin() as conn:
        conn.execute(text(stmt))
    logger.info(f"Truncated table {schema}.{table}")

# Functions to read from landing table and insert into lookup tables
def read_landing_table_for_card_language_lookup():
    try:
        # Read the landing table from the database
        query = f"""SELECT distinct card_language as name
                    FROM {db_landing_schema}.{db_landing_table}
                    WHERE nullif(card_language, '') IS NOT NULL"""

        df = pd.read_sql(query, con=engine)
        logger.info(f"{db_landing_table} table read successfully")
        return df
    except Exception as e:
        logger.error(f"Error reading landing table: {e}")
        return None
    
def insert_language_lookup_data():
    try:
        # Read the landing table data
        df = read_landing_table_for_card_language_lookup()
        if df is not None:
            # Clean/reload lookup table
            truncate_table(db_main_schema, db_language_lookup_table, restart_identity=True)

            # Write the DataFrame to the main database table
            df.to_sql(db_language_lookup_table, con=engine, schema=db_main_schema, if_exists='append', index=False)
            logger.info("Pokemon card language lookup data inserted successfully into main database")
    except Exception as e:
        logger.error(f"Error inserting pokemon card language lookup data: {e}")

# Functions for card set lookup
def read_landing_table_for_card_set_lookup():
    try:
        # Read the landing table from the database
        query = f"""SELECT distinct card_set as name, 
                           card_year as year
                    FROM {db_landing_schema}.{db_landing_table}
                    WHERE nullif(card_set, '') IS NOT NULL
                    ORDER BY name, year"""
        
        df = pd.read_sql(query, con=engine)
        logger.info(f"{db_landing_table} table read successfully")
        return df
    except Exception as e:
        logger.error(f"Error reading landing table: {e}")
        return None
    
def insert_set_lookup_data():
    try:
        # Read the landing table data
        df = read_landing_table_for_card_set_lookup()
        if df is not None:
            # Clean/reload lookup table
            truncate_table(db_main_schema, db_set_lookup_table, restart_identity=True)

            # Write the DataFrame to the main database table
            df.to_sql(db_set_lookup_table, con=engine, schema=db_main_schema, if_exists='append', index=False)
            logger.info("Pokemon card set lookup data inserted successfully into main database")
    except Exception as e:
        logger.error(f"Error inserting pokemon card set lookup data: {e}")

def read_landing_table_for_grading_company_lookup():
    try:
        # Read the landing table from the database
        query = f"""SELECT distinct grading_company as name
                    FROM {db_landing_schema}.{db_landing_table}
                    WHERE nullif(grading_company, '') IS NOT NULL"""
        
        df = pd.read_sql(query, con=engine)
        logger.info(f"{db_landing_table} table read successfully")
        return df
    except Exception as e:
        logger.error(f"Error reading landing table: {e}")
        return None
    
def insert_grading_company_lookup_data():
    try:
        # Read the landing table data
        df = read_landing_table_for_grading_company_lookup()
        if df is not None:
            # Clean/reload lookup table
            truncate_table(db_main_schema, db_grading_company_lookup_table, restart_identity=True)

            # Write the DataFrame to the main database table
            df.to_sql(db_grading_company_lookup_table, con=engine, schema=db_main_schema, if_exists='append', index=False)
            logger.info("Pokemon card grading company lookup data inserted successfully into main database")
    except Exception as e:
        logger.error(f"Error inserting pokemon card grading company lookup data: {e}")

def read_landing_table_for_rarity_lookup():
    try:
        # Read the landing table from the database
        query = f"""SELECT distinct card_rarity as name
                    FROM {db_landing_schema}.{db_landing_table}
                    WHERE nullif(card_rarity, '') IS NOT NULL"""
        
        df = pd.read_sql(query, con=engine)
        logger.info(f"{db_landing_table} table read successfully")
        return df
    except Exception as e:
        logger.error(f"Error reading landing table: {e}")
        return None
    
def insert_rarity_lookup_data():
    try:
        # Read the landing table data
        df = read_landing_table_for_rarity_lookup()
        if df is not None:
            # Clean/reload lookup table
            truncate_table(db_main_schema, db_rarity_lookup_table, restart_identity=True)

            # Write the DataFrame to the main database table
            df.to_sql(db_rarity_lookup_table, con=engine, schema=db_main_schema, if_exists='append', index=False)
            logger.info("Pokemon card rarity lookup data inserted successfully into main database")
    except Exception as e:
        logger.error(f"Error inserting pokemon card rarity lookup data: {e}")
