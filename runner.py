from references import *
from pokemon_cards_landing import *
from card import *

logger.info("Starting the Pokemon card data processing pipeline...")
# landing data processing
load_pokemon_cards_landing()
# card data processing
read_landing_table_for_card_data()
insert_card_data()
# grade data processing
# seller data processing
# card_grade data processing
# card_seller data processing
logger.info("Pokemon card data processing pipeline completed successfully.")
