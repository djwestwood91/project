from references import *
import pandas as pd

def load_pokemon_cards_landing():
    try:
        # Load the Excel file into a DataFrame and write it to the database
        logger.info(f"Reading source data from {pokemon_card_file_path + pokemon_card_file_name}")
        df = pd.read_excel(pokemon_card_file_path + pokemon_card_file_name, sheet_name=pokemon_card_sheet_name)
        # Write the DataFrame to the database
        df.to_sql('landing_pokemon_card', con=engine, schema=db_landing_schema, if_exists='replace', index=False)
        logger.info(f"Pokemon cards from {pokemon_card_file_path + pokemon_card_file_name} loaded successfully")
    except Exception as e:
        logger.error(f"Error loading pokemon cards: {e}")
