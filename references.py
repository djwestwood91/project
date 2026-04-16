from asyncio.log import logger
from sqlalchemy import create_engine
import logging
import os
from dotenv import load_dotenv
import sys
import pandas as pd
import boto3
from botocore.exceptions import ClientError

load_dotenv()

# logger initialization
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(name)s %(message)s',
    handlers=[
        logging.FileHandler("db.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("PokeLog")

# Configure UTF-8 encoding for console output on Windows
# Windows is 'nt', other OSes are 'posix'
if os.name == 'nt':
    # Reconfigure stderr to use UTF-8
    if hasattr(sys.stderr, 'reconfigure'):
        sys.stderr.reconfigure(encoding='utf-8')
    # Update the StreamHandler encoding
    for handler in logging.root.handlers:
        if isinstance(handler, logging.StreamHandler):
            handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(name)s %(message)s'))

# Status indicators for logging
STATUS_OK = "[OK]"
STATUS_FAIL = "[FAIL]"

# Database configuration from environment variables
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "postgres")

# Database schemas and tables
DB_LANDING_SCHEMA = os.getenv("DB_LANDING_SCHEMA", "pokemon_landing")
DB_MAIN_SCHEMA = os.getenv("DB_MAIN_SCHEMA", "pokemon")

DB_LANDING_TABLE = os.getenv("DB_LANDING_TABLE", "landing_pokemon_card")
DB_CARD_TABLE = os.getenv("DB_CARD_TABLE", "card")
DB_CARD_INSTANCE_TABLE = os.getenv("DB_CARD_INSTANCE_TABLE", "card_instance")
DB_CARD_GRADE_TABLE = os.getenv("DB_CARD_GRADE_TABLE", "card_grade")
DB_SELLER_TABLE = os.getenv("DB_SELLER_TABLE", "seller")
DB_PURCHASE_TABLE = os.getenv("DB_PURCHASE_TABLE", "purchase")

# lookup (dimensional) tables
DB_LANGUAGE_LOOKUP_TABLE = os.getenv("DB_LANGUAGE_LOOKUP_TABLE", "")
DB_SET_LOOKUP_TABLE = os.getenv("DB_SET_LOOKUP_TABLE", "")
DB_GRADING_COMPANY_LOOKUP_TABLE = os.getenv("DB_GRADING_COMPANY_LOOKUP_TABLE", "")
DB_GRADE_DESCRIPTION_LOOKUP_TABLE = os.getenv("DB_GRADE_DESCRIPTION_LOOKUP_TABLE", "")
DB_RARITY_LOOKUP_TABLE = os.getenv("DB_RARITY_LOOKUP_TABLE", "")
DB_CURRENCY_LOOKUP_TABLE = os.getenv("DB_CURRENCY_LOOKUP_TABLE", "")
DB_PURCHASE_SOURCE_LOOKUP_TABLE = os.getenv("DB_PURCHASE_SOURCE_LOOKUP_TABLE", "")
DB_COUNTRY_LOOKUP_TABLE = os.getenv("DB_COUNTRY_LOOKUP_TABLE", "")

# source file configuration
POKEMON_CARD_FILE_PATH = os.getenv("FILE_PATH", "")
POKEMON_CARD_FILE_NAME = os.getenv("FILE_NAME", "")
POKEMON_CARD_SHEET_NAME = os.getenv("FILE_SHEET_NAME", "")

# output file configuration
POKEMON_CARD_OUTPUT_FILE_PATH = os.getenv("OUTPUT_FILE_PATH", "")
POKEMON_CARD_OUTPUT_FILE_NAME = os.getenv("OUTPUT_FILE_NAME", "")

# model configuration
DB_MODEL_FILE_PATH = os.getenv("DB_MODEL_FILE_PATH", "model_references/model.sql")

# Load pipeline flags from environment variables
PIPELINE_CREATE_MODEL = os.getenv("PIPELINE_CREATE_MODEL", "True").lower() == "true"
PIPELINE_CLEAR_LANDING = os.getenv("PIPELINE_CLEAR_LANDING", "False").lower() == "true"
PIPELINE_LIST_S3 = os.getenv("PIPELINE_LIST_S3", "True").lower() == "true"
PIPELINE_UPLOAD_S3 = os.getenv("PIPELINE_UPLOAD_S3", "True").lower() == "true"
PIPELINE_DOWNLOAD_S3 = os.getenv("PIPELINE_DOWNLOAD_S3", "False").lower() == "true"

# AWS Configuration
AWS_REGION = os.getenv("AWS_REGION")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
S3_BUCKET_PREFIX = os.getenv("S3_BUCKET_PREFIX", "")

logger.info(f"Initializing database connection to {DB_NAME}...")

# password = getpass.getpass("Enter database password: ")
DB_URL = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
ENGINE = create_engine(DB_URL)

# Test the engine connection
try:
    with ENGINE.connect() as connection:
        logger.info(f"Database Connection to {DB_NAME} successful")
except Exception as e:
    logger.error(f"Connection failed: {str(e)}", exc_info=True)
    raise
