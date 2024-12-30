from flask import Flask, render_template, jsonify
from sqlalchemy import create_engine, text
from config import DB_SERVER, DB_NAME, DB_USER, DB_PASSWORD

app = Flask(__name__)

# Create SQLAlchemy engine - add this at the top level
engine = create_engine(f'mssql+pyodbc://{DB_USER}:{DB_PASSWORD}@{DB_SERVER}/{DB_NAME}?driver=SQL+Server+Native+Client+11.0')

@app.route('/')
def index():
    try:
        # First get trending coins (top 20 by 24h change)
        trending_query = """
        SELECT TOP 20 crypto_id
        FROM coingecko_crypto_daily_data
        WHERE price_date = (
            SELECT MAX(price_date) 
            FROM coingecko_crypto_daily_data
        )
        ORDER BY price_change_24h DESC
        """
        
        with engine.connect() as connection:
            trending_result = connection.execute(text(trending_query))
            trending_ids = {row.crypto_id for row in trending_result}

        # Then get main query - removed TOP 100
        query = """
        SELECT 
            m.id,
            m.name,
            m.symbol,
            m.image_url,
            d.current_price,
            d.price_change_24h,
            d.market_cap,
            d.total_volume,
            d.price_date as last_updated,
            m.market_cap_rank as rank
        FROM coingecko_crypto_master m
        JOIN coingecko_crypto_daily_data d ON m.id = d.crypto_id
        WHERE d.price_date = (
            SELECT MAX(price_date) 
            FROM coingecko_crypto_daily_data
        )
        ORDER BY m.market_cap_rank
        """
        
        with engine.connect() as connection:
            result = connection.execute(text(query))
            cryptocurrencies = [
                {
                    'rank': row.rank,
                    'id': row.id,
                    'name': row.name,
                    'symbol': row.symbol,
                    'image_url': f"https://lcw.nyc3.cdn.digitaloceanspaces.com/production/currencies/64/{row.symbol.lower()}.png",
                    'current_price': float(row.current_price) if row.current_price else 0,
                    'price_change_24h': float(row.price_change_24h) if row.price_change_24h else 0,
                    'market_cap': float(row.market_cap) if row.market_cap else 0,
                    'total_volume': float(row.total_volume) if row.total_volume else 0,
                    'last_updated': row.last_updated,
                    'is_trending': row.id in trending_ids
                } for row in result
            ]
            
        return render_template('index.html', cryptocurrencies=cryptocurrencies)
    except Exception as e:
        print(f"Error in index route: {str(e)}")
        import traceback
        print(traceback.format_exc())
        return str(e), 500

@app.route('/get_coin_details/<crypto_id>')
def get_coin_details(crypto_id):
    try:
        query = """
        SELECT TOP 1
            m.name,
            m.symbol,
            d.crypto_id,
            d.current_price,
            d.price_change_24h,
            d.market_cap,
            d.total_volume,
            COALESCE(s.sentiment_votes_up, 0) as sentiment_votes_up,
            COALESCE(s.sentiment_votes_down, 0) as sentiment_votes_down,
            COALESCE(s.public_interest_score, 0) as public_interest_score,
            COALESCE(s.twitter_sentiment, 0) as twitter_sentiment,
            COALESCE(s.reddit_sentiment, 0) as reddit_sentiment,
            COALESCE(s.news_sentiment, 0) as news_sentiment
        FROM coingecko_crypto_daily_data d
        JOIN coingecko_crypto_master m ON d.crypto_id = m.id
        LEFT JOIN coingecko_crypto_sentiment s 
            ON d.crypto_id = s.crypto_id 
            AND CAST(d.price_date AS DATE) = CAST(s.metric_date AS DATE)
        WHERE d.crypto_id = :crypto_id
        ORDER BY d.price_date DESC
        """
        
        with engine.connect() as connection:
            result = connection.execute(text(query), {"crypto_id": crypto_id}).fetchone()
            
            if result:
                image_url = f"https://lcw.nyc3.cdn.digitaloceanspaces.com/production/currencies/64/{result.symbol.lower()}.png"
                
                return jsonify({
                    'name': result.name,
                    'symbol': result.symbol,
                    'image_url': image_url,
                    'crypto_id': result.crypto_id,
                    'current_price': float(result.current_price),
                    'price_change_24h': float(result.price_change_24h) if result.price_change_24h else 0,
                    'market_cap': float(result.market_cap) if result.market_cap else 0,
                    'volume': float(result.total_volume) if result.total_volume else 0,
                    'sentiment_up': float(result.sentiment_votes_up),
                    'sentiment_down': float(result.sentiment_votes_down),
                    'interest_score': float(result.public_interest_score),
                    'twitter_sentiment': float(result.twitter_sentiment),
                    'reddit_sentiment': float(result.reddit_sentiment),
                    'news_sentiment': float(result.news_sentiment)
                })
            else:
                return jsonify({'error': 'Crypto not found'}), 404
                
    except Exception as e:
        print(f"Error fetching crypto detail: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/test')
def test_db():
    try:
        with engine.connect() as connection:
            result = connection.execute(text("SELECT TOP 1 * FROM coingecko_crypto_master"))
            row = result.fetchone()
            return str(dict(row))
    except Exception as e:
        return f"Database error: {str(e)}"

# Add trending route
@app.route('/trending')
def trending():
    try:
        # First get trending coins (top 20 by 24h change)
        query = """
        SELECT TOP 20
            m.id,
            m.name,
            m.symbol,
            m.market_cap_rank as rank,
            d.current_price,
            d.price_change_24h,
            d.market_cap,
            d.total_volume,
            d.price_date as last_updated
        FROM coingecko_crypto_master m
        JOIN coingecko_crypto_daily_data d ON m.id = d.crypto_id
        WHERE d.price_date = (
            SELECT MAX(price_date) 
            FROM coingecko_crypto_daily_data
        )
        ORDER BY d.price_change_24h DESC
        """
        
        with engine.connect() as connection:
            result = connection.execute(text(query))
            cryptocurrencies = [
                {
                    'rank': row.rank,
                    'id': row.id,
                    'name': row.name,
                    'symbol': row.symbol,
                    'image_url': f"https://lcw.nyc3.cdn.digitaloceanspaces.com/production/currencies/64/{row.symbol.lower()}.png",
                    'current_price': float(row.current_price) if row.current_price else 0,
                    'price_change_24h': float(row.price_change_24h) if row.price_change_24h else 0,
                    'market_cap': float(row.market_cap) if row.market_cap else 0,
                    'total_volume': float(row.total_volume) if row.total_volume else 0,
                    'last_updated': row.last_updated
                } for row in result
            ]
            
        return render_template('trending.html', cryptocurrencies=cryptocurrencies)
    except Exception as e:
        print(f"Error in trending route: {str(e)}")
        import traceback
        print(traceback.format_exc())
        return str(e), 500

@app.route('/predictions')
def predictions():
    try:
        query = """
        SELECT TOP 100
            m.id,
            m.name,
            m.symbol,
            m.market_cap_rank as rank,
            d.current_price,
            d.price_date as last_updated
        FROM coingecko_crypto_master m
        JOIN coingecko_crypto_daily_data d ON m.id = d.crypto_id
        WHERE d.price_date = (
            SELECT MAX(price_date) 
            FROM coingecko_crypto_daily_data
        )
        ORDER BY m.market_cap_rank
        """
        
        with engine.connect() as connection:
            result = connection.execute(text(query))
            predictions = [
                {
                    'id': row.id,
                    'name': row.name,
                    'symbol': row.symbol,
                    'rank': row.rank,
                    'image_url': f"https://lcw.nyc3.cdn.digitaloceanspaces.com/production/currencies/64/{row.symbol.lower()}.png",
                    'current_price': float(row.current_price),
                    # Placeholder predictions with increasing percentages
                    'pred_24h': float(row.current_price) * 1.01,
                    'pred_48h': float(row.current_price) * 1.02,
                    'pred_3d': float(row.current_price) * 1.03,
                    'pred_7d': float(row.current_price) * 1.05,
                    # Placeholder changes
                    'pred_24h_change': 1.0,
                    'pred_48h_change': 2.0,
                    'pred_3d_change': 3.0,
                    'pred_7d_change': 5.0,
                    # Placeholder confidence scores (decreasing with time horizon)
                    'confidence_24h': 85.0,
                    'confidence_48h': 80.0,
                    'confidence_3d': 75.0,
                    'confidence_7d': 70.0,
                    'last_updated': row.last_updated
                } for row in result
            ]
            
        return render_template('predictions.html', predictions=predictions)
    except Exception as e:
        print(f"Error in predictions route: {str(e)}")
        import traceback
        print(traceback.format_exc())
        return str(e), 500

if __name__ == '__main__':
    app.run(debug=True) 