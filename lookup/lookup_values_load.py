from references import *
import pandas as pd
from utils.db_utils import truncate_table

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
        raise
    
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
        raise

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
        raise
    
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
        raise

def read_landing_table_for_grading_company_lookup():
    try:
        # Read the landing table from the database
        query = f"""SELECT distinct grading_company as company,
                           grading_company_full_name as company_full_name
                    FROM {db_landing_schema}.{db_landing_table}
                    WHERE nullif(grading_company, '') IS NOT NULL"""
        
        df = pd.read_sql(query, con=engine)
        logger.info(f"{db_landing_table} table read successfully")
        return df
    except Exception as e:
        logger.error(f"Error reading landing table: {e}")
        raise
    
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
        raise

def read_landing_table_for_grading_description_lookup():
    try:
        # Read the landing table from the database
        query = f"""SELECT distinct gc.grading_company_id, 
                           lpc.grade,
                           lpc.grade_description
                    FROM {db_landing_schema}.{db_landing_table} lpc
                    JOIN {db_main_schema}.{db_grading_company_lookup_table} gc ON lpc.grading_company = gc.company
                    WHERE nullif(lpc.grade_description, '') IS NOT NULL"""
        
        df = pd.read_sql(query, con=engine)
        logger.info(f"{db_landing_table} table read successfully")
        return df
    except Exception as e:
        logger.error(f"Error reading landing table: {e}")
        raise
    
def insert_grade_description_lookup_data():
    try:
        # Read the landing table data
        df = read_landing_table_for_grading_description_lookup()
        if df is not None:
            # Clean/reload lookup table
            truncate_table(db_main_schema, db_grade_description_lookup_table, restart_identity=True)

            # Write the DataFrame to the main database table
            df.to_sql(db_grade_description_lookup_table, con=engine, schema=db_main_schema, if_exists='append', index=False)
            logger.info("Pokemon card grading description lookup data inserted successfully into main database")
    except Exception as e:
        logger.error(f"Error inserting pokemon card grading description lookup data: {e}")
        raise

def read_landing_table_for_rarity_lookup():
    try:
        # Read the landing table from the database
        query = f"""SELECT distinct card_rarity as rarity
                    FROM {db_landing_schema}.{db_landing_table}
                    WHERE nullif(card_rarity, '') IS NOT NULL"""
        
        df = pd.read_sql(query, con=engine)
        logger.info(f"{db_landing_table} table read successfully")
        return df
    except Exception as e:
        logger.error(f"Error reading landing table: {e}")
        raise
    
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
        raise
