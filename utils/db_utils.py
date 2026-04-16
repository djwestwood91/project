from references import *
from sqlalchemy import text

# Helper: truncate a target table (keeps schema, resets identities)
def truncate_table(schema, table, restart_identity=True, cascade=True):
    try:
        # Validate identifiers to prevent SQL injection
        if not schema or not table or not all(c.isalnum() or c == '_' for c in schema + table):
            raise ValueError(f"Invalid schema or table name: {schema}.{table}")
        
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
