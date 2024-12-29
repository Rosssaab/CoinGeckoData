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
        # Get basic crypto data and sentiment metrics
        cursor.execute("""
            WITH LatestData AS (
                SELECT TOP 1 
                    m.id,
                    m.name,
                    m.symbol,
                    m.image_id,
                    m.image_filename,
                    d.current_price,
                    d.market_cap,
                    d.total_volume,
                    d.price_change_24h,
                    d.price_date,
                    s.sentiment_votes_up,
                    s.sentiment_votes_down,
                    s.public_interest_score,
                    s.price_change_7d,
                    s.price_change_14d,
                    s.price_change_30d,
                    s.market_cap_change_24h
                FROM coingecko_crypto_master m
                JOIN coingecko_crypto_daily_data d ON m.id = d.crypto_id
                LEFT JOIN coingecko_crypto_sentiment s ON m.id = s.crypto_id 
                    AND CAST(d.price_date AS DATE) = CAST(s.metric_date AS DATE)
                WHERE m.id = ?
                ORDER BY d.price_date DESC
            )
            SELECT * FROM LatestData
        """, coin_id)
        
        row = cursor.fetchone()
        if not row:
            return jsonify({'error': 'Crypto not found'}), 404
            
        # Get predictions and model performance
        cursor.execute("""
            SELECT TOP 1 
                p.price_24h,
                p.price_48h,
                p.price_3d,
                p.price_7d,
                p.confidence_score,
                p.model_version,
                mp.mae_24h,
                mp.rmse_24h,
                mp.mae_7d,
                mp.rmse_7d
            FROM coingecko_crypto_predictions p
            LEFT JOIN coingecko_model_performance mp 
                ON p.model_version = mp.model_version
            WHERE p.crypto_id = ?
            ORDER BY p.prediction_date DESC
        """, coin_id)
        
        pred_row = cursor.fetchone()
        
        # Get feature importance for latest model
        cursor.execute("""
            SELECT TOP 3
                feature_name,
                importance_score
            FROM coingecko_feature_importance
            WHERE model_version = ?
            ORDER BY importance_score DESC
        """, pred_row.model_version if pred_row else '')
        
        features = cursor.fetchall()
        
        # Calculate sentiment score
        total_votes = (row.sentiment_votes_up or 0) + (row.sentiment_votes_down or 0)
        sentiment = ((row.sentiment_votes_up or 0) / total_votes * 100) if total_votes > 0 else 50
        
        return jsonify({
            'id': row.id,
            'name': row.name,
            'symbol': row.symbol,
            'image_url': f"https://assets.coingecko.com/coins/images/{row.image_id}/thumb/{row.image_filename}",
            'latest_price': float(row.current_price),
            'market_cap': float(row.market_cap),
            'total_volume': float(row.total_volume),
            'price_change_24h': float(row.price_change_24h or 0),
            
            # Price Changes
            'price_change_7d': float(row.price_change_7d or 0),
            'price_change_14d': float(row.price_change_14d or 0),
            'price_change_30d': float(row.price_change_30d or 0),
            'market_cap_change_24h': float(row.market_cap_change_24h or 0),
            
            # Social/Sentiment Metrics
            'sentiment_score': f"{sentiment:.1f}%",
            'public_interest_score': float(row.public_interest_score or 0),
            'total_votes': total_votes,
            
            # Predictions
            'pred_24h': float(pred_row.price_24h) if pred_row else None,
            'pred_48h': float(pred_row.price_48h) if pred_row else None,
            'pred_3d': float(pred_row.price_3d) if pred_row else None,
            'pred_7d': float(pred_row.price_7d) if pred_row else None,
            'confidence_score': float(pred_row.confidence_score) if pred_row else None,
            
            # Model Performance
            'model_version': pred_row.model_version if pred_row else 'N/A',
            'model_accuracy_24h': calculate_accuracy(pred_row.mae_24h, row.current_price) if pred_row else None,
            'model_accuracy_7d': calculate_accuracy(pred_row.mae_7d, row.current_price) if pred_row else None,
            
            # Top Features
            'top_features': [
                {'name': f.feature_name, 'importance': float(f.importance_score)}
                for f in features
            ] if features else []
        })
        
    except Exception as e:
        print(f"Error fetching crypto detail: {str(e)}")
        return jsonify({'error': str(e)}), 500
        
    finally:
        cursor.close()
        conn.close()

def calculate_accuracy(mae, current_price):
    """Calculate accuracy percentage based on MAE and current price"""
    if mae and current_price:
        error_percentage = (float(mae) / float(current_price)) * 100
        return max(0, 100 - error_percentage)
    return None

@app.route('/trending')
def trending():
    """Get trending cryptocurrencies from database"""
    conn = pyodbc.connect(DB_CONNECTION_STRING)
    cursor = conn.cursor()
    
    try:
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
                WHERE price_date >= DATEADD(hour, -24, GETDATE())
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
            crypto['image_url'] = f"https://assets.coingecko.com/coins/images/{crypto['image_id']}/thumb/{crypto['image_filename']}"
            results.append(crypto)
        
        return render_template('trending.html', crypto_data=results)
        
    except Exception as e:
        print(f"Database error: {str(e)}")
        return render_template('trending.html', crypto_data=[])
        
    finally:
        cursor.close()
        conn.close()

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