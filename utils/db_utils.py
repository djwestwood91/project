from references import *

# Helper: validate database identifiers to prevent SQL injection
def validate_identifiers(*identifiers):
    """Validate database identifiers (schema/table names) to prevent SQL injection attacks."""
    try:
        if not identifiers:
            raise ValueError("No identifiers provided for validation")
        for identifier in identifiers:
            if not identifier or not all(c.isalnum() or c == '_' for c in identifier):
                raise ValueError(f"Invalid identifier: {identifier}")
        logger.info(f"Validated {len(identifiers)} identifier(s)")
    except Exception as e:
        logger.error(f"Identifier validation error: {str(e)}", exc_info=True)
        raise

# Helper: truncate a target table (keeps schema, resets identities)
def truncate_table(schema, table, restart_identity=True, cascade=True):
    try:
        # Validate identifiers to prevent SQL injection
        validate_identifiers(schema, table)
        
        stmt = f"TRUNCATE TABLE {schema}.{table}"
        if restart_identity:
            stmt += " RESTART IDENTITY"
        if cascade:
            stmt += " CASCADE"
        
        with ENGINE.begin() as conn:
            conn.execute(text(stmt))
        
        log_msg = f"Truncated {schema}.{table}"
        if restart_identity:
            log_msg += " (restarted identity)"
        if cascade:
            log_msg += " (cascade)"
        logger.info(log_msg)
    except Exception as e:
        logger.error(f"Error truncating {schema}.{table}: {str(e)}", exc_info=True)
        raise

# run the model.sql file to create the database model in the target database
def create_model(create_model_flag):
    try:
        if create_model_flag:
            # Read the SQL model file from environment variable
            sql_file_path = DB_MODEL_FILE_PATH
            with open(sql_file_path, 'r') as f:
                sql_script = f.read()
            
            # Execute the SQL script
            with ENGINE.begin() as conn:
                conn.execute(text(sql_script))
            
            logger.info("Database model created successfully from model.sql")
        else:
            logger.info("Create model flag is set to False. Skipping database model creation.")
    except Exception as e:
        logger.error(f"Error creating database model: {str(e)}", exc_info=True)
        raise
