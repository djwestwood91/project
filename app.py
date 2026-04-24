from references import *
from flask import Flask, render_template, request

app = Flask(__name__)

# Route to display all cards with pagination
@app.route('/')
def index():
    try:
        # Pagination settings
        page = request.args.get('page', 1, type=int)
        per_page = 25  # Cards per page
        offset = (page - 1) * per_page
        
        # Get total count
        count_query = f"""
        SELECT COUNT(*) as total
        FROM {DB_FACTS_SCHEMA}.{DB_CARD_TABLE} c
        """
        count_df = pd.read_sql(count_query, con=ENGINE)
        total_cards = count_df['total'][0]
        total_pages = (total_cards + per_page - 1) // per_page  # Ceiling division
        
        # Get paginated data
        query = f"""
        SELECT 
            c.card_id,
            c.card,
            cs."name" as card_set,
            l."name" as language,
            r.rarity
        FROM {DB_FACTS_SCHEMA}.{DB_CARD_TABLE} c
        JOIN {DB_DIMENSIONS_SCHEMA}.card_set cs ON cs.card_set_id = c.card_set_id
        JOIN {DB_DIMENSIONS_SCHEMA}.language l ON l.language_id = c.card_language_id
        JOIN {DB_DIMENSIONS_SCHEMA}.rarity r ON r.rarity_id = c.card_rarity_id
        OFFSET {offset} LIMIT {per_page}
        """
        
        df = pd.read_sql(query, con=ENGINE)
        cards = df.to_dict('records')
        
        return render_template('index.html', 
                             cards=cards, 
                             page=page, 
                             total_pages=total_pages,
                             total_cards=total_cards)
    except Exception as e:
        logger.error(f"Error fetching cards: {str(e)}", exc_info=True)
        return f"Error: {str(e)}", 500

# Route to display single card details
@app.route('/card/<int:card_id>')
def card_detail(card_id):
    try:
        query = text(f"""
        SELECT DISTINCT 
                c.card_id,
                c.card, 
                c.card_holo_flag,
                c.card_first_edition_flag,
                c.card_promo_flag,        
                cs."name" as card_set,
                l."name" as language,
                r.rarity,
                gd.grade_description as grade,
                gc.company, 
                p.purchase_price as raw_price_excl_fees,
                p.postage_fees,
                p.total_price, 
                cr.currency_code as currency,
                p.date_purchased
        FROM {DB_FACTS_SCHEMA}.{DB_CARD_TABLE} c
        JOIN {DB_FACTS_SCHEMA}.{DB_CARD_INSTANCE_TABLE} ci ON c.card_id = ci.card_id
        LEFT JOIN {DB_FACTS_SCHEMA}.{DB_PURCHASE_TABLE} p ON ci.card_instance_id = p.card_instance_id
        LEFT JOIN {DB_FACTS_SCHEMA}.{DB_CARD_GRADE_TABLE} cg ON cg.grade_id = p.grade_id
        LEFT JOIN {DB_DIMENSIONS_SCHEMA}.{DB_GRADE_DESCRIPTION_LOOKUP_TABLE} gd ON gd.grade_description_id = cg.grade_description_id
        LEFT JOIN {DB_DIMENSIONS_SCHEMA}.{DB_GRADING_COMPANY_LOOKUP_TABLE} gc ON gc.grading_company_id = gd.grading_company_id
        LEFT JOIN {DB_DIMENSIONS_SCHEMA}.{DB_CURRENCY_LOOKUP_TABLE} cr ON cr.currency_id = p.currency_id
        LEFT JOIN {DB_DIMENSIONS_SCHEMA}.card_set cs ON cs.card_set_id = c.card_set_id
        LEFT JOIN {DB_DIMENSIONS_SCHEMA}.language l ON l.language_id = c.card_language_id
        LEFT JOIN {DB_DIMENSIONS_SCHEMA}.rarity r ON r.rarity_id = c.card_rarity_id
        WHERE c.card_id = :card_id
        """)
        
        df = pd.read_sql(query, con=ENGINE, params={'card_id': card_id})
        if df.empty:
            return "Card not found", 404
        
        card = df.to_dict('records')[0]
        return render_template('card_detail.html', card=card)
    except Exception as e:
        logger.error(f"Error fetching card: {str(e)}", exc_info=True)
        return f"Error: {str(e)}", 500
    
@app.route('/tcgdex_detail/<card_name>')
def tcgdex_detail(card_name):
    try:
        # Pagination settings
        page = request.args.get('page', 1, type=int)
        per_page = 25  # Cards per page
        offset = (page - 1) * per_page
        
        # Get total count
        count_query = text(f"""
                            SELECT COUNT(*) as total
                            FROM {DB_API_SCHEMA}.{DB_API_TGCDEX_CARD_TABLE}
                            WHERE card_name = :card_name;
                            """)
        count_df = pd.read_sql(count_query, con=ENGINE, params={'card_name': card_name})
        total_cards = count_df['total'][0]
        total_pages = (total_cards + per_page - 1) // per_page  # Ceiling division
        
        # Get paginated data
        query = text(f"""
                    SELECT 
                        card_name,
                        language,
                        tcgdex_id,
                        tcgdex_localid,
                        tcgdex_name,
                        tcgdex_image
                    FROM {DB_API_SCHEMA}.{DB_API_TGCDEX_CARD_TABLE}
                    WHERE card_name = :card_name
                    OFFSET {offset} LIMIT {per_page};
                    """)
        df = pd.read_sql(query, con=ENGINE, params={'card_name': card_name})
        if df.empty:
            return "Card not found", 404
        
        tcgdex_cards = df.to_dict('records')
        return render_template('tcgdex_detail.html', 
                               tcgdex_cards=tcgdex_cards, 
                               page=page, 
                               total_pages=total_pages, 
                               total_cards=total_cards)
    except Exception as e:
        logger.error(f"Error fetching TCGDex details: {str(e)}", exc_info=True)
        return f"Error: {str(e)}", 500

if __name__ == '__main__':
    app.run(debug=True, host='localhost', port=5000)
