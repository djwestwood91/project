from asyncio.log import logger

from sqlalchemy import create_engine
import getpass
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
logger = logging.getLogger("pokelog")

logger.info("Initializing database connection...")
load_dotenv()

db_user = os.getenv("DB_USER", "postgres")
db_password = os.getenv("DB_PASSWORD", "")
db_host = os.getenv("DB_HOST", "localhost")
db_port = os.getenv("DB_PORT", "5432")
db_name = os.getenv("DB_NAME", "postgres")

# password = getpass.getpass("Enter database password: ")
db_url = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
engine = create_engine(db_url)

# Test the engine connection
try:
    with engine.connect() as connection:
        logger.info(f"Database Connection to {db_name} successful")
except Exception as e:
    logger.error(f"Connection failed: {e}")

