from lookup_values_load import insert_grading_company_lookup_data, insert_language_lookup_data, insert_set_lookup_data
from references import *
from pokemon_cards_landing import *
from card import *

logger.info("Starting the Pokemon card data processing pipeline...")
# landing data processing
load_pokemon_cards_landing()
# lookup values processing
insert_language_lookup_data()
insert_set_lookup_data()
insert_grading_company_lookup_data()
# card data processing
insert_card_data()
# grade data processing
# seller data processing
# card_grade data processing
# card_seller data processing
logger.info("Pokemon card data processing pipeline completed successfully.")
