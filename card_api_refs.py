from references import *
from tcgdexsdk import TCGdex, Query
import time

def write_tcgdex_card_to_db(card_data, row, tcgdex_id):
    """
    Write TCGdex card data to database
    
    Args:
        card_data: Card data from TCGdex API
        row: Row from current_prices_df
        tcgdex_id: Card ID from TCGdex
    """
    try:
        # Insert card data into a TCGdex reference table
        insert_query = text(f"""
        INSERT INTO {DB_API_SCHEMA}.{DB_API_TGCDEX_CARD_TABLE}  
        (card_id, language_id, tcgdex_id, tcgdex_localid, tcgdex_name, tcgdex_image, created_at, updated_at)
        VALUES (:card_id, :language_id, :tcgdex_id, :tcgdex_localid, :tcgdex_name, :tcgdex_image, now(), now())
        ON CONFLICT (tcgdex_id) DO UPDATE SET updated_at = now()
        """)

        params = {
            'card_id': row['card_id'],
            'language_id': row['language_id'],
            'tcgdex_id': tcgdex_id,
            'tcgdex_localid': card_data.localId if hasattr(card_data, 'localId') and card_data.localId is not None else None,
            'tcgdex_name': card_data.name if hasattr(card_data, 'name') and card_data.name is not None else None,
            'tcgdex_image': (card_data.image + "/" + DEFAULT_IMAGE_QUALITY + "." + DEFAULT_IMAGE_FILE_TYPE) if hasattr(card_data, 'image') and card_data.image is not None else None,
        }
        
        with ENGINE.connect() as conn:
            conn.execute(insert_query, params)
            conn.commit()
            logger.info(f"Wrote card '{row['card']}' (ID: {tcgdex_id}) to database")
    
    except Exception as e:
        logger.error(f"Error writing card to database: {e}")

def check_source_card_price():
    try:
        query = f"""
                SELECT distinct
                            c.card_id,
                            c.card, 
                            cs."name" as card_set,
                            l.language_id,
                            l."name" as language,
                            r.rarity,
                            gd.grade,
                            p.purchase_price as raw_price_excl_fees, 
                            cr.currency_code as currency,
                            p.date_purchased
                FROM {DB_FACTS_SCHEMA}.{DB_CARD_TABLE} c 
                JOIN {DB_DIMENSIONS_SCHEMA}.{DB_SET_LOOKUP_TABLE} cs ON cs.card_set_id = c.card_set_id
                JOIN {DB_DIMENSIONS_SCHEMA}.{DB_RARITY_LOOKUP_TABLE} r ON r.rarity_id = c.card_rarity_id
                JOIN {DB_DIMENSIONS_SCHEMA}.{DB_LANGUAGE_LOOKUP_TABLE} l ON l.language_id = c.card_language_id
                JOIN {DB_FACTS_SCHEMA}.{DB_CARD_INSTANCE_TABLE} ci ON ci.card_id = c.card_id
                LEFT JOIN {DB_FACTS_SCHEMA}.{DB_PURCHASE_TABLE} p ON ci.card_instance_id = p.card_instance_id
                LEFT JOIN {DB_FACTS_SCHEMA}.{DB_CARD_GRADE_TABLE} cg ON cg.grade_id = p.grade_id
                LEFT JOIN {DB_DIMENSIONS_SCHEMA}.{DB_GRADE_DESCRIPTION_LOOKUP_TABLE} gd ON gd.grade_description_id = cg.grade_description_id
                LEFT JOIN {DB_DIMENSIONS_SCHEMA}.{DB_CURRENCY_LOOKUP_TABLE} cr ON cr.currency_id = p.currency_id
                WHERE p.purchase_price IS NOT NULL
                ORDER BY c.card_id;
                """
        
        df = pd.read_sql(query, con=ENGINE)
            
        logger.info(f"Total rows fetched: {len(df)}")
            
        return df
    except Exception as e:
        logger.error(f"Error checking card prices: {str(e)}", exc_info=True)
        raise

def run_card_api_checks():
    try:
        current_prices_df = check_source_card_price()

        # Map language strings to Language enum values
        language_map = {
            'English': "en",
            'Japanese': "ja",
            'German': "de"
            # add more mappings as needed
        }
        current_prices_df['language'] = current_prices_df['language'].map(language_map)
        
        tcgdex = TCGdex() # Initialize with default language (English)

        logger.info("Testing basic API query...")
        all_cards = tcgdex.card.listSync()  # Get ALL cards
        logger.info(f"Total cards available: {len(all_cards)}")

        # Iterate through dataframe and use set appropriate language per row
        for idx, row in current_prices_df.iterrows():
            tcgdex.setLanguage(row['language'])
            logger.info(f"Row {idx}: Using TCGdex instance for language {row['language']}")

            card_data_list = tcgdex.card.listSync(Query().equal("name", row['card']))

            logger.info(f"Row {idx}: Found {len(card_data_list)} result(s) for '{row['card']}'")
            
            # Write each card result to database
            for card_data in card_data_list:
                write_tcgdex_card_to_db(card_data, row, card_data.id)
                logger.info(f"Row {idx}: Wrote card data for '{row['card']}' to database")

            # Add delay to avoid hitting API rate limits
            time.sleep(5)  # Adjust the sleep time as needed based on TCGdex API rate limits

    except Exception as e:
        logger.error(f"Error running card price checks: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    run_card_api_checks();
