from config import DB_CONNECTION_STRING
from flask import Flask, render_template, jsonify
import pyodbc
from datetime import datetime, timedelta

app = Flask(__name__)

def get_db_connection():
    """Create database connection"""
    return pyodbc.connect(DB_CONNECTION_STRING)

def get_crypto_data():
    conn = pyodbc.connect(DB_CONNECTION_STRING)
    cursor = conn.cursor()
    
    try:
        # Modified query to calculate 24h price change correctly
        query = """
        WITH PricePoints AS (
            SELECT 
                m.id,
                m.name,
                m.symbol,
                m.market_cap_rank,
                m.image_id,
                m.image_filename,
                d.current_price,
                d.market_cap,
                d.total_volume,
                d.price_date,
                LAG(d.current_price) OVER (PARTITION BY m.id ORDER BY d.price_date) as prev_price
            FROM coingecko_crypto_master m
            JOIN coingecko_crypto_daily_data d ON m.id = d.crypto_id
            WHERE d.price_date >= DATEADD(hour, -25, GETDATE())
        )
        SELECT 
            id,
            name,
            symbol,
            market_cap_rank,
            image_id,
            image_filename,
            current_price as latest_price,
            market_cap,
            total_volume,
            price_date,
            CASE 
                WHEN prev_price IS NOT NULL AND prev_price != 0 
                THEN ((current_price - prev_price) / prev_price) * 100 
                ELSE 0 
            END as price_change_24h
        FROM PricePoints
        WHERE price_date = (SELECT MAX(price_date) FROM PricePoints)
        ORDER BY market_cap_rank ASC
        """
        
        cursor.execute(query)
        columns = [column[0] for column in cursor.description]
        results = []
        
        for row in cursor.fetchall():
            crypto = dict(zip(columns, row))
            # Add image URL
            crypto['image_url'] = f"https://assets.coingecko.com/coins/images/{crypto['image_id']}/thumb/{crypto['image_filename']}"
            # Ensure price_change_24h is 0 if None
            crypto['price_change_24h'] = crypto['price_change_24h'] or 0
            results.append(crypto)
            
        return results
        
    except Exception as e:
        print(f"Database error: {str(e)}")
        return []
        
    finally:
        cursor.close()
        conn.close()

@app.route('/')
def index():
    crypto_data = get_crypto_data()
    print(f"Retrieved {len(crypto_data)} cryptocurrencies")  # Debug line
    return render_template('index.html', crypto_data=crypto_data)

@app.route('/get_coin_details/<coin_id>')
def get_coin_details(coin_id):
    conn = pyodbc.connect(DB_CONNECTION_STRING)
    cursor = conn.cursor()
    
    try:
        # Get basic crypto data
        cursor.execute("""
            SELECT TOP 1 
                m.id,
                m.name,
                m.symbol,
                m.image_id,
                m.image_filename,
                d.current_price as latest_price,
                d.market_cap,
                d.total_volume,
                d.price_change_24h,
                d.price_date
            FROM coingecko_crypto_master m
            JOIN coingecko_crypto_daily_data d ON m.id = d.crypto_id
            WHERE m.id = ?
            ORDER BY d.price_date DESC
        """, coin_id)
        
        row = cursor.fetchone()
        if not row:
            return jsonify({'error': 'Crypto not found'}), 404
            
        # Construct image URL
        image_url = f"https://assets.coingecko.com/coins/images/{row.image_id}/thumb/{row.image_filename}"
            
        # Get predictions - updated to match your table structure
        cursor.execute("""
            SELECT TOP 1 
                price_24h,
                price_7d,
                confidence_score
            FROM coingecko_crypto_predictions
            WHERE crypto_id = ?
            ORDER BY prediction_date DESC
        """, coin_id)
        
        pred_row = cursor.fetchone()
        current_price = float(row.latest_price)
        
        # Use actual predictions if available, otherwise use dummy predictions
        pred_24h = float(pred_row.price_24h) if pred_row and pred_row.price_24h else current_price * 1.01
        pred_7d = float(pred_row.price_7d) if pred_row and pred_row.price_7d else current_price * 1.05
        
        return jsonify({
            'id': row.id,
            'name': row.name,
            'symbol': row.symbol,
            'image_url': image_url,
            'latest_price': current_price,
            'market_cap': float(row.market_cap),
            'total_volume': float(row.total_volume),
            'price_change_24h': float(row.price_change_24h or 0),
            'price_date': row.price_date.strftime('%Y-%m-%d %H:%M:%S'),
            'pred_24h': pred_24h,
            'pred_7d': pred_7d,
            'confidence_score': float(pred_row.confidence_score) if pred_row else 50.0
        })
        
    except Exception as e:
        print(f"Error fetching crypto detail: {str(e)}")
        return jsonify({'error': str(e)}), 500
        
    finally:
        cursor.close()
        conn.close()

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

@app.route('/crypto/<crypto_id>')
def crypto_detail(crypto_id):
    conn = pyodbc.connect(DB_CONNECTION_STRING)
    cursor = conn.cursor()
    
    try:
        # Get basic crypto data
        cursor.execute("""
            SELECT TOP 1 
                m.id,
                m.name,
                m.symbol,
                m.image_id,
                m.image_filename,
                d.current_price as latest_price,
                d.market_cap,
                d.total_volume,
                d.price_change_24h
            FROM coingecko_crypto_master m
            JOIN coingecko_crypto_daily_data d ON m.id = d.crypto_id
            WHERE m.id = ?
            ORDER BY d.price_date DESC
        """, crypto_id)
        
        row = cursor.fetchone()
        if not row:
            return jsonify({'error': 'Crypto not found'}), 404
            
        # Construct image URL
        image_url = f"https://assets.coingecko.com/coins/images/{row.image_id}/thumb/{row.image_filename}"
            
        # Get predictions
        cursor.execute("""
            SELECT TOP 1 
                pred_24h,
                pred_7d
            FROM coingecko_crypto_predictions
            WHERE crypto_id = ?
            ORDER BY prediction_date DESC
        """, crypto_id)
        
        pred_row = cursor.fetchone()
        predictions = {
            'pred_24h': pred_row.pred_24h if pred_row else None,
            'pred_7d': pred_row.pred_7d if pred_row else None
        } if pred_row else {'pred_24h': None, 'pred_7d': None}
        
        return jsonify({
            'id': row.id,
            'name': row.name,
            'symbol': row.symbol,
            'image_url': image_url,
            'latest_price': float(row.latest_price),
            'market_cap': float(row.market_cap),
            'total_volume': float(row.total_volume),
            'price_change_24h': float(row.price_change_24h or 0),
            'pred_24h': float(predictions['pred_24h'] or row.latest_price),
            'pred_7d': float(predictions['pred_7d'] or row.latest_price)
        })
        
    except Exception as e:
        print(f"Error fetching crypto detail: {str(e)}")
        return jsonify({'error': 'Internal server error'}), 500
        
    finally:
        cursor.close()
        conn.close()

if __name__ == '__main__':
    app.run(debug=True) 