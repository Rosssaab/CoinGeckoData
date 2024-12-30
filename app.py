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
        SELECT m.id, m.name, m.symbol,
               d.current_price, d.price_change_24h, 
               d.market_cap, d.total_volume
        FROM coingecko_crypto_master m
        JOIN coingecko_crypto_daily_data d ON m.id = d.crypto_id
        WHERE m.id = :crypto_id
        AND d.price_date = (
            SELECT MAX(price_date) 
            FROM coingecko_crypto_daily_data
        )
        """
        
        with engine.connect() as connection:
            result = connection.execute(text(query), {"crypto_id": crypto_id}).fetchone()
            
            if not result:
                return jsonify({'error': 'Coin not found'}), 404
                
            return jsonify({
                'id': result.id,
                'name': result.name,
                'symbol': result.symbol,
                'current_price': float(result.current_price) if result.current_price else 0,
                'price_change_24h': float(result.price_change_24h) if result.price_change_24h else 0,
                'market_cap': float(result.market_cap) if result.market_cap else 0,
                'volume': float(result.total_volume) if result.total_volume else 0,
                'image_url': f"https://lcw.nyc3.cdn.digitaloceanspaces.com/production/currencies/64/{result.symbol.lower()}.png",
                # Add dummy values for now
                'sentiment_up': 0,
                'sentiment_down': 0,
                'interest_score': 0,
                'twitter_sentiment': 0,
                'reddit_sentiment': 0,
                'news_sentiment': 0
            })
            
    except Exception as e:
        print(f"Error getting coin details: {str(e)}")
        return jsonify({'error': 'Failed to load coin details'}), 500

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

@app.route('/past_predictions')
def past_predictions():
    try:
        query = """
        SELECT 
            m.id,
            m.name,
            m.symbol,
            m.market_cap_rank as rank,
            p.prediction_date,
            p.price_24h as predicted_price_24h,
            p.price_48h as predicted_price_48h,
            p.price_3d as predicted_price_3d,
            p.price_7d as predicted_price_7d,
            d.current_price as actual_price,
            d.price_date
        FROM coingecko_crypto_master m
        JOIN coingecko_crypto_predictions p ON m.id = p.crypto_id
        LEFT JOIN coingecko_crypto_daily_data d ON m.id = d.crypto_id 
            AND d.price_date > p.prediction_date  -- Get any actual price after prediction
        ORDER BY p.prediction_date DESC, m.market_cap_rank
        """
        
        with engine.connect() as connection:
            result = connection.execute(text(query))
            past_predictions = []
            
            for row in result:
                # Calculate accuracy for each timeframe
                actual_price = float(row.actual_price) if row.actual_price else None
                if actual_price:
                    predictions = {
                        '24h': {
                            'predicted': float(row.predicted_price_24h),
                            'actual': actual_price,
                            'timeframe': '24 hours'
                        },
                        '48h': {
                            'predicted': float(row.predicted_price_48h),
                            'actual': actual_price,
                            'timeframe': '48 hours'
                        },
                        '3d': {
                            'predicted': float(row.predicted_price_3d),
                            'actual': actual_price,
                            'timeframe': '3 days'
                        },
                        '7d': {
                            'predicted': float(row.predicted_price_7d),
                            'actual': actual_price,
                            'timeframe': '7 days'
                        }
                    }
                    
                    for timeframe, data in predictions.items():
                        if data['predicted']:
                            difference = ((data['actual'] - data['predicted']) / data['predicted']) * 100
                            accuracy = 100 - min(abs(difference), 100)  # Cap accuracy at 0-100%
                            
                            past_predictions.append({
                                'id': row.id,
                                'name': row.name,
                                'symbol': row.symbol,
                                'rank': row.rank,
                                'image_url': f"https://lcw.nyc3.cdn.digitaloceanspaces.com/production/currencies/64/{row.symbol.lower()}.png",
                                'prediction_date': row.prediction_date,
                                'timeframe': data['timeframe'],
                                'predicted_price': data['predicted'],
                                'actual_price': data['actual'],
                                'difference': difference,
                                'accuracy': accuracy
                            })
            
        return render_template('past_predictions.html', past_predictions=past_predictions)
    except Exception as e:
        print(f"Error in past predictions route: {str(e)}")
        import traceback
        print(traceback.format_exc())
        return str(e), 500

@app.route('/api/coin_history/<crypto_id>')
def coin_history(crypto_id):
    try:
        # Remove the TOP 7 limit to get all history
        query = """
        SELECT price_date, current_price 
        FROM coingecko_crypto_daily_data 
        WHERE crypto_id = :crypto_id 
        ORDER BY price_date DESC
        """
        
        with engine.connect() as connection:
            result = connection.execute(text(query), {"crypto_id": crypto_id}).fetchall()
            
            if not result:
                return jsonify({'error': 'No data found'}), 404
                
            dates = [row[0].strftime('%d-%m-%Y %H:%M:%S') for row in result][::-1]
            prices = [float(row[1]) for row in result][::-1]
            
            return jsonify({
                'dates': dates,
                'prices': prices
            })
            
    except Exception as e:
        print(f"Error in coin_history: {str(e)}")
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True) 