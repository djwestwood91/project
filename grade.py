from references import *

def read_landing_table_for_grade_data():
    try:
        # Read the landing table and match to card_instance_id - join on row_id for 1:1 mapping
        query = f"""select distinct on (lpc.row_id)
                    ci.card_instance_id,
                    lpc.row_id,
                    gd.grade_description_id,
                    lpc.grading_certification_number,
                    lpc.graded_card_url
                from {DB_LANDING_SCHEMA}.{DB_LANDING_TABLE} lpc 
                join {DB_DIMENSIONS_SCHEMA}.{DB_SET_LOOKUP_TABLE} cs on cs."name" = lpc.card_set
                join {DB_FACTS_SCHEMA}.{DB_CARD_TABLE} pc on cs.card_set_id = pc.card_set_id 
                 and pc.card = lpc.card
                join {DB_FACTS_SCHEMA}.{DB_CARD_INSTANCE_TABLE} ci on ci.row_id = lpc.row_id
                join {DB_DIMENSIONS_SCHEMA}.{DB_GRADE_DESCRIPTION_LOOKUP_TABLE} gd on gd.grade_description = lpc.grade_description 
                join {DB_DIMENSIONS_SCHEMA}.{DB_GRADING_COMPANY_LOOKUP_TABLE} gc on gc.grading_company_id = gd.grading_company_id
                and gc.company = lpc.grading_company
                where lpc.grade is not null
                order by lpc.row_id;"""
        df = pd.read_sql(query, con=ENGINE)
        logger.info(f"{DB_LANDING_TABLE} table read successfully for grade data")
        return df
    except Exception as e:
        logger.error(f"Error reading landing table: {str(e)}", exc_info=True)
        raise
    
def perform_quality_checks_on_grade_data():
    try:
        # Example quality check: Ensure no null values in critical columns
        query = f"""SELECT COUNT(*) FROM {DB_FACTS_SCHEMA}.{DB_CARD_GRADE_TABLE} WHERE card_instance_id IS NULL OR grade_description_id IS NULL"""
        result = pd.read_sql(query, con=ENGINE)
        # if the count of records with null values in critical columns is greater than 0, log a warning, otherwise log that the quality check passed
        if result.iloc[0, 0] > 0:
            logger.warning(f"{DB_CARD_GRADE_TABLE} Quality Check Failed: Found {result.iloc[0, 0]} records with null values in critical columns")
        else:
            logger.info(f"{DB_CARD_GRADE_TABLE} Quality Check Passed: No null values found in critical columns")
    except Exception as e:
        logger.error(f"Error performing quality checks on grade data: {str(e)}", exc_info=True)
        raise

def insert_grade_data():
    try:
        # Read the landing table data
        df = read_landing_table_for_grade_data()
        if df is not None:
            # Write the DataFrame to the main database table
            df.to_sql(DB_CARD_GRADE_TABLE, con=ENGINE, schema=DB_FACTS_SCHEMA, if_exists='append', index=False)
            logger.info(f"{DB_CARD_GRADE_TABLE} data inserted successfully into main database")
            perform_quality_checks_on_grade_data()
    except Exception as e:
        logger.error(f"Error inserting {DB_CARD_GRADE_TABLE} data: {str(e)}", exc_info=True)
        raise
