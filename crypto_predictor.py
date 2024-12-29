import pandas as pd
import numpy as np
from tensorflow.keras.models import load_model
from sklearn.preprocessing import MinMaxScaler
import pyodbc
from datetime import datetime, timedelta
import os
import glob
import logging
from config import DB_CONNECTION_STRING

class CryptoPredictor:
    def __init__(self):
        self.conn = pyodbc.connect(DB_CONNECTION_STRING)
        self.sequence_length = 10
        self.scaler = MinMaxScaler()  # Create a new scaler instance
        
        # Create directories if they don't exist
        os.makedirs('models', exist_ok=True)
        os.makedirs('logs', exist_ok=True)
        
        logging.basicConfig(
            filename=f'logs/predictor_{datetime.now().strftime("%Y%m%d")}.log',
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        
    def get_latest_data(self, crypto_id):
        """Get latest data for prediction"""
        query = """
            WITH LatestData AS (
                SELECT TOP 10
                    d.crypto_id,
                    d.price_date,
                    d.current_price,
                    d.market_cap,
                    d.total_volume,
                    d.price_change_24h,
                    s.sentiment_votes_up,
                    s.sentiment_votes_down,
                    s.public_interest_score
                FROM coingecko_crypto_daily_data d
                LEFT JOIN coingecko_crypto_sentiment s 
                    ON d.crypto_id = s.crypto_id 
                    AND CAST(d.price_date AS DATE) = CAST(s.metric_date AS DATE)
                WHERE d.crypto_id = ?
                ORDER BY d.price_date DESC
            )
            SELECT * FROM LatestData ORDER BY price_date ASC
        """
        return pd.read_sql(query, self.conn, params=[crypto_id])
        
    def prepare_data(self, data):
        """Prepare data for prediction"""
        # Fill missing values
        data = data.fillna(method='ffill').fillna(method='bfill')
        
        features = ['current_price', 'market_cap', 'total_volume', 'price_change_24h',
                   'sentiment_votes_up', 'sentiment_votes_down', 'public_interest_score']
                   
        X = data[features].values
        # Fit and transform in one go, no need for saved scaler
        X_scaled = self.scaler.fit_transform(X)
        return X_scaled.reshape(1, len(data), len(features))
        
    def save_prediction(self, crypto_id, predictions, confidence, model_version):
        """Save predictions to database"""
        cursor = self.conn.cursor()
        prediction_date = datetime.now()
        
        try:
            cursor.execute("""
                INSERT INTO coingecko_crypto_predictions (
                    crypto_id, prediction_date, prediction_created_at,
                    price_24h, price_48h, price_3d, price_7d,
                    confidence_score, model_version
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                crypto_id,
                prediction_date,
                datetime.now(),
                float(predictions[0]),  # 24h
                float(predictions[1]),  # 48h
                float(predictions[2]),  # 3d
                float(predictions[3]),  # 7d
                float(confidence),
                model_version
            ))
            
            self.conn.commit()
            logging.info(f"Saved predictions for {crypto_id}")
            
        except Exception as e:
            logging.error(f"Error saving predictions for {crypto_id}: {str(e)}")
            self.conn.rollback()
            
    def make_predictions(self):
        """Make predictions for all cryptocurrencies"""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT id 
            FROM coingecko_crypto_master 
            WHERE id IN (
                SELECT DISTINCT crypto_id 
                FROM coingecko_crypto_daily_data
            )
        """)
        crypto_ids = [row[0] for row in cursor.fetchall()]
        
        for crypto_id in crypto_ids:
            try:
                # Find latest model
                model_files = glob.glob(f'models/{crypto_id}_LSTM_v1_*.h5')
                if not model_files:
                    logging.warning(f"No model found for {crypto_id}")
                    continue
                    
                latest_model_file = max(model_files)
                model = load_model(latest_model_file)
                model_version = os.path.basename(latest_model_file).replace('.h5', '')
                
                # Get and prepare data
                data = self.get_latest_data(crypto_id)
                if len(data) < self.sequence_length:
                    logging.warning(f"Insufficient data for {crypto_id}")
                    continue
                    
                X = self.prepare_data(data)
                
                # Make predictions
                predictions = model.predict(X)[0]
                
                # Calculate confidence score based on recent prediction accuracy
                current_price = data['current_price'].iloc[-1]
                confidence = 100 * (1 - np.std(predictions) / current_price)
                confidence = max(min(confidence, 100), 0)  # Clip between 0 and 100
                
                # Scale predictions back to actual prices
                predictions = predictions * current_price
                
                # Save predictions
                self.save_prediction(crypto_id, predictions, confidence, model_version)
                logging.info(f"Successfully made predictions for {crypto_id}")
                
            except Exception as e:
                logging.error(f"Error making predictions for {crypto_id}: {str(e)}")
                continue

if __name__ == "__main__":
    predictor = CryptoPredictor()
    predictor.make_predictions() 