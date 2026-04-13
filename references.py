from asyncio.log import logger
from sqlalchemy import create_engine
import logging
import os
from dotenv import load_dotenv

# logger initialization
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(name)s %(message)s',
    handlers=[
        logging.FileHandler("db.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("PokeLog")

logger.info("Initializing database connection...")
load_dotenv()

db_user = os.getenv("DB_USER", "postgres")
db_password = os.getenv("DB_PASSWORD", "")
db_host = os.getenv("DB_HOST", "localhost")
db_port = os.getenv("DB_PORT", "5432")
db_name = os.getenv("DB_NAME", "postgres")

db_landing_schema = os.getenv("DB_LANDING_SCHEMA", "pokemon_landing")
db_main_schema = os.getenv("DB_MAIN_SCHEMA", "pokemon")

db_landing_table = os.getenv("DB_LANDING_TABLE", "landing_pokemon_card")
db_card_table = os.getenv("DB_CARD_TABLE", "card")
db_grade_table = os.getenv("DB_GRADE_TABLE", "grade")
db_seller_table = os.getenv("DB_SELLER_TABLE", "seller")
db_card_grade_table = os.getenv("DB_CARD_GRADE_TABLE", "card_grade")
db_card_seller_table = os.getenv("DB_CARD_SELLER_TABLE", "card_seller")

# lookup tables
db_language_lookup_table = os.getenv("DB_LANGUAGE_LOOKUP_TABLE", "")
db_set_lookup_table = os.getenv("DB_SET_LOOKUP_TABLE", "")
db_grading_company_lookup_table = os.getenv("DB_GRADING_COMPANY_LOOKUP_TABLE", "")
db_grade_description_lookup_table = os.getenv("DB_GRADE_DESCRIPTION_LOOKUP_TABLE", "")
db_rarity_lookup_table = os.getenv("DB_RARITY_LOOKUP_TABLE", "")

# source file configuration
pokemon_card_file_path = os.getenv("FILE_PATH", "")
pokemon_card_file_name = os.getenv("FILE_NAME", "")
pokemon_card_sheet_name = os.getenv("FILE_SHEET_NAME", "")

# output file configuration
pokemon_card_output_file_path = os.getenv("OUTPUT_FILE_PATH", "")
pokemon_card_output_file_name = os.getenv("OUTPUT_FILE_NAME", "")

logger.info(f"Initializing database connection to {db_name}...")

# password = getpass.getpass("Enter database password: ")
db_url = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
engine = create_engine(db_url)

# Test the engine connection
try:
    with engine.connect() as connection:
        logger.info(f"Database Connection to {db_name} successful")
except Exception as e:
    logger.error(f"Connection failed: {e}")
