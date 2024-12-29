import pandas as pd
import numpy as np
import pyodbc
from datetime import datetime, timedelta
from config import DB_CONNECTION_STRING

def generate_test_data():
    """Generate and insert test data for a sample cryptocurrency"""
    conn = pyodbc.connect(DB_CONNECTION_STRING)
    cursor = conn.cursor()
    
    try:
        # Insert test crypto into master table
        cursor.execute("""
        IF NOT EXISTS (SELECT 1 FROM coingecko_crypto_master WHERE id = 'test-bitcoin')
        INSERT INTO coingecko_crypto_master (id, symbol, name, market_cap_rank, image_id, image_filename)
        VALUES ('test-bitcoin', 'tbtc', 'Test Bitcoin', 1, 1, 'test.png')
        """)
        
        # Generate 30 days of test data
        base_price = 30000.0
        base_date = datetime.now() - timedelta(days=30)
        
        for i in range(30):
            current_date = base_date + timedelta(days=i)
            # Generate slightly random price movements
            price_change = base_price * (0.95 + 0.1 * np.random.random())
            market_cap = price_change * 19000000  # Approximate BTC circulation
            
            # Delete existing data for this date if any
            cursor.execute("""
            DELETE FROM coingecko_crypto_daily_data 
            WHERE crypto_id = 'test-bitcoin' AND CAST(price_date AS DATE) = CAST(? AS DATE)
            """, current_date)
            
            # Insert daily data
            cursor.execute("""
            INSERT INTO coingecko_crypto_daily_data 
            (crypto_id, price_date, current_price, market_cap, total_volume, price_change_24h, market_cap_rank)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                'test-bitcoin',
                current_date,
                price_change,
                market_cap,
                market_cap * 0.05,  # 5% of market cap as volume
                ((price_change - base_price) / base_price) * 100,
                1
            ))
            
            # Delete existing sentiment data if any
            cursor.execute("""
            DELETE FROM coingecko_crypto_sentiment 
            WHERE crypto_id = 'test-bitcoin' AND CAST(metric_date AS DATE) = CAST(? AS DATE)
            """, current_date)
            
            # Add sentiment data
            cursor.execute("""
            INSERT INTO coingecko_crypto_sentiment
            (crypto_id, metric_date, sentiment_votes_up, sentiment_votes_down, public_interest_score)
            VALUES (?, ?, ?, ?, ?)
            """, (
                'test-bitcoin',
                current_date,
                int(100 * np.random.random()),
                int(50 * np.random.random()),
                np.random.random() * 100
            ))
            
            base_price = price_change  # Use current price as base for next day
            
        conn.commit()
        print("Test data generated successfully")
        
    except Exception as e:
        print(f"Error generating test data: {str(e)}")
        conn.rollback()
        raise  # Re-raise the exception for debugging
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    print("Generating test data...")
    generate_test_data()
    
    print("\nRunning model trainer...")
    from crypto_model_trainer import CryptoModelTrainer
    trainer = CryptoModelTrainer()
    trainer.train_models()
    
    print("\nRunning predictor...")
    from crypto_predictor import CryptoPredictor
    predictor = CryptoPredictor()
    predictor.make_predictions() 