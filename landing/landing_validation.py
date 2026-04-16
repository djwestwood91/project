from references import *

def run_landing_data_validation():
    try:
        # Perform validation checks on the landing table data
        # Example check: Ensure no null values in critical columns
        query = f"""SELECT COUNT(*) FROM {DB_LANDING_SCHEMA}.{DB_LANDING_TABLE} WHERE card IS NULL OR card_seller IS NULL"""
        result = pd.read_sql(query, con=ENGINE)
        if result.iloc[0, 0] > 0:
            logger.warning(f"Landing Table Validation Failed: Found {result.iloc[0, 0]} records with null values in critical columns")
            query = f"""SELECT * FROM {DB_LANDING_SCHEMA}.{DB_LANDING_TABLE} WHERE card IS NULL OR card_seller IS NULL"""
            result = pd.read_sql(query, con=ENGINE)
            logger.warning(f"Records with null values:\n{result}")
        else:
            logger.info("Landing Table Validation Passed: No null values found in critical columns")
    except Exception as e:
        logger.error(f"Error validating landing table data: {str(e)}", exc_info=True)
        raise

# Note: The landing validation phase can be expanded with additional checks as needed, 
# such as data type validations, range checks, or consistency checks across related columns.
