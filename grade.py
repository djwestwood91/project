from references import *
import pandas as pd

def read_landing_table_for_grade_data():
    try:
        # Read the landing table from the database
        query = f"""select 
                    pc.card_id,
                    gd.grade_description_id,
                    lpc.grading_certification_number,
                    lpc.graded_card_url,
                    -- to do: 'graded_at' - add this to the landing table
                    null as graded_at
                from {db_landing_schema}.{db_landing_table} lpc 
                join {db_main_schema}.{db_set_lookup_table} cs on cs."name" = lpc.card_set
                join {db_main_schema}.{db_card_table} pc on cs.card_set_id = pc.card_set_id 
                 and pc.card = lpc.card
                join {db_main_schema}.{db_grade_description_lookup_table} gd on gd.grade_description = lpc.grade_description 
                join {db_main_schema}.{db_grading_company_lookup_table} gc on gc.grading_company_id = gd.grading_company_id
                 and gc.company = lpc.grading_company
                where lpc.grade is not null
                order by pc.card_id;"""
        df = pd.read_sql(query, con=engine)
        logger.info(f"{db_landing_table} table read successfully")
        return df
    except Exception as e:
        logger.error(f"Error reading landing table: {e}")
        raise
    
def perform_quality_checks_on_grade_data():
    try:
        # Example quality check: Ensure no null values in critical columns
        query = f"""SELECT COUNT(*) FROM {db_main_schema}.{db_grade_table} WHERE card_id IS NULL OR grade_description_id IS NULL"""
        result = pd.read_sql(query, con=engine)
        # if the count of records with null values in critical columns is greater than 0, log a warning, otherwise log that the quality check passed
        if result.iloc[0, 0] > 0:
            logger.warning(f"Quality Check Failed: Found {result.iloc[0, 0]} records with null values in critical columns")
        else:
            logger.info("Quality Check Passed: No null values found in critical columns")
    except Exception as e:
        logger.error(f"Error performing quality checks on grade data: {e}")
        raise

def insert_grade_data():
    try:
        # Read the landing table data
        df = read_landing_table_for_grade_data()
        if df is not None:
            # Write the DataFrame to the main database table
            df.to_sql(db_grade_table, con=engine, schema=db_main_schema, if_exists='append', index=False)
            logger.info("Pokemon grade data inserted successfully into main database")
            perform_quality_checks_on_grade_data()
    except Exception as e:
        logger.error(f"Error inserting pokemon grade data: {e}")
        raise
