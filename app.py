from config import DB_CONNECTION_STRING
from flask import Flask, render_template, jsonify
import pyodbc
from datetime import datetime

app = Flask(__name__)

def get_db_connection():
    """Create database connection"""
    return pyodbc.connect(DB_CONNECTION_STRING)

@app.route('/get_coin_details/<coin_id>')
def get_coin_details(coin_id):
    """Get detailed coin information from database only"""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                WITH LatestData AS (
                    SELECT 
                        crypto_id,
                        current_price,
                        market_cap,
                        market_cap_rank,
                        price_change_24h,
                        total_volume,
                        price_date,
                        is_trending,
                        CONVERT(varchar, price_date, 120) as datetime_str,
                        ROW_NUMBER() OVER (PARTITION BY crypto_id ORDER BY price_date DESC) as rn
                    FROM coingecko_crypto_daily_data
                    WHERE crypto_id = ?
                ),
                LatestSentiment AS (
                    SELECT 
                        crypto_id,
                        sentiment_votes_up,
                        sentiment_votes_down,
                        public_interest_score,
                        metric_date,
                        CONVERT(varchar, metric_date, 120) as sentiment_datetime,
                        ROW_NUMBER() OVER (PARTITION BY crypto_id ORDER BY metric_date DESC) as rn
                    FROM coingecko_crypto_sentiment
                    WHERE crypto_id = ?
                )
                SELECT 
                    m.*,
                    d.current_price,
                    d.market_cap,
                    d.market_cap_rank,
                    d.price_change_24h,
                    d.total_volume,
                    d.datetime_str as price_datetime,
                    d.is_trending,
                    s.sentiment_votes_up,
                    s.sentiment_votes_down,
                    s.public_interest_score,
                    s.sentiment_datetime
                FROM coingecko_crypto_master m
                LEFT JOIN LatestData d ON m.id = d.crypto_id AND d.rn = 1
                LEFT JOIN LatestSentiment s ON m.id = s.crypto_id AND s.rn = 1
                WHERE m.id = ?
            """, coin_id, coin_id, coin_id)
            
            columns = [column[0] for column in cursor.description]
            row = cursor.fetchone()
            
            if not row:
                return jsonify({'error': 'Coin not found'})
                
            result = dict(zip(columns, row))
            
            return jsonify({
                'id': result['id'],
                'name': result['name'],
                'symbol': result['symbol'],
                'market_data': {
                    'current_price': result['current_price'],
                    'market_cap': result['market_cap'],
                    'total_volume': result['total_volume'],
                    'price_change_24h': result['price_change_24h']
                },
                'community_data': {
                    'sentiment_votes_up': result['sentiment_votes_up'],
                    'sentiment_votes_down': result['sentiment_votes_down'],
                    'public_interest_score': result['public_interest_score']
                },
                'is_trending': result['is_trending'],
                'price_datetime': result['price_datetime'],
                'sentiment_datetime': result['sentiment_datetime']
            })
            
    except Exception as e:
        print(f"Error getting coin details: {str(e)}")
        return jsonify({'error': str(e)})

@app.route('/')
def index():
    crypto_data = get_top_100_crypto()
    return render_template('index.html', crypto_data=crypto_data)

def get_top_100_crypto():
    """Get top 100 cryptocurrencies from database"""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                WITH LatestData AS (
                    SELECT 
                        crypto_id,
                        current_price,
                        market_cap,
                        market_cap_rank,
                        price_change_24h,
                        total_volume,
                        is_trending,
                        price_date,
                        ROW_NUMBER() OVER (PARTITION BY crypto_id ORDER BY price_date DESC) as rn
                    FROM coingecko_crypto_daily_data
                )
                SELECT 
                    m.id,
                    m.name,
                    m.symbol,
                    m.image_id,
                    m.image_filename,
                    d.current_price,
                    d.market_cap,
                    d.market_cap_rank,
                    d.price_change_24h,
                    d.total_volume as volume_24h,
                    d.is_trending,
                    d.price_date as last_updated
                FROM coingecko_crypto_master m
                JOIN LatestData d ON m.id = d.crypto_id AND d.rn = 1
                ORDER BY d.market_cap_rank
                OFFSET 0 ROWS FETCH NEXT 100 ROWS ONLY
            """)
            
            columns = [column[0] for column in cursor.description]
            results = []
            
            for row in cursor.fetchall():
                crypto = dict(zip(columns, row))
                # Construct image URL from stored ID and filename
                crypto['image'] = f"https://assets.coingecko.com/coins/images/{crypto['image_id']}/thumb/{crypto['image_filename']}"
                results.append(crypto)
            
            return {'data': results, 'error': None}
            
    except Exception as e:
        print(f"Database error: {str(e)}")
        return {'error': str(e)}

@app.route('/trending')
def trending():
    """Get trending cryptocurrencies from database"""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                WITH LatestData AS (
                    SELECT 
                        crypto_id,
                        current_price,
                        market_cap,
                        market_cap_rank,
                        price_change_24h,
                        total_volume,
                        is_trending,
                        price_date,
                        ROW_NUMBER() OVER (PARTITION BY crypto_id ORDER BY price_date DESC) as rn
                    FROM coingecko_crypto_daily_data
                )
                SELECT 
                    m.id,
                    m.name,
                    m.symbol,
                    m.image_id,
                    m.image_filename,
                    d.current_price,
                    d.market_cap,
                    d.market_cap_rank,
                    d.price_change_24h,
                    d.total_volume as volume_24h,
                    d.is_trending,
                    d.price_date as last_updated
                FROM coingecko_crypto_master m
                JOIN LatestData d ON m.id = d.crypto_id AND d.rn = 1
                WHERE d.is_trending = 1
                ORDER BY d.market_cap_rank
            """)
            
            columns = [column[0] for column in cursor.description]
            results = []
            
            for row in cursor.fetchall():
                crypto = dict(zip(columns, row))
                crypto['image'] = f"https://assets.coingecko.com/coins/images/{crypto['image_id']}/thumb/{crypto['image_filename']}"
                results.append(crypto)
            
            return render_template('trending.html', crypto_data={'data': results, 'error': None})
            
    except Exception as e:
        print(f"Database error: {str(e)}")
        return render_template('trending.html', crypto_data={'error': str(e)})

if __name__ == '__main__':
    app.run(debug=True) 