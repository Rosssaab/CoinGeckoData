import os
from datetime import datetime
from config import (
    DB_CONNECTION_STRING,
    DB_SERVER,
    DB_NAME,
    DB_USER,
    DB_PASSWORD
)
import logging
from sqlalchemy import create_engine, text
import pandas as pd
import numpy as np
from tensorflow.keras.models import load_model
from sklearn.preprocessing import MinMaxScaler
import glob
import tensorflow as tf
tf.get_logger().setLevel('ERROR')  # Suppress TF warnings

os.environ['TF_ENABLE_ONEDNN_OPTS'] = '0'  # Add at very top of file to disable oneDNN optimizations

class CryptoPredictor:
    def __init__(self):
        # Create SQLAlchemy engine using imported config
        self.connection_str = f'mssql+pyodbc://{DB_USER}:{DB_PASSWORD}@{DB_SERVER}/{DB_NAME}?driver=SQL+Server+Native+Client+11.0'
        self.engine = create_engine(self.connection_str)
        self.sequence_length = 10
        self.confidence_threshold = 0.7
        
        # Create directories if they don't exist
        os.makedirs('models', exist_ok=True)
        os.makedirs('logs', exist_ok=True)
        
        logging.basicConfig(
            filename=f'logs/predictor_{datetime.now().strftime("%Y%m%d")}.log',
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        
        # Add list of stablecoins to ignore
        self.stablecoin_identifiers = [
            'usdt', 'usdc', 'dai', 'usde', 'usd0', 'fdusd', 'usds',  # Major stablecoins
            'aammdai',  # Aave AMM stablecoins
            'aave-amm-unidaiusdc'  # Stablecoin pairs
        ]
        
    def get_latest_data(self, crypto_id):
        """Get latest data for prediction using SQLAlchemy"""
        query = text("""
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
                WHERE d.crypto_id = :crypto_id
                ORDER BY d.price_date DESC
            )
            SELECT * FROM LatestData ORDER BY price_date ASC
        """)
        
        with self.engine.connect() as connection:
            return pd.read_sql(query, connection, params={"crypto_id": crypto_id})
        
    def prepare_data(self, data):
        """Prepare data for prediction with enhanced accuracy"""
        try:
            # Use only the original 7 features that the model was trained on
            base_features = [
                'current_price',
                'market_cap',
                'total_volume',
                'price_change_24h',
                'sentiment_votes_up',
                'sentiment_votes_down',
                'public_interest_score'
            ]
            
            # Create a new dataframe with only required columns
            numeric_data = data[base_features].copy()
            
            # Store the last current price for prediction calculations
            self.last_current_price = numeric_data['current_price'].iloc[-1]
            
            # Convert to float and handle missing values
            for col in base_features:
                numeric_data[col] = pd.to_numeric(numeric_data[col], errors='coerce')
            
            # Log data shape and types before processing
            logging.info(f"Data shape before preparation: {numeric_data.shape}")
            logging.info(f"Data types: {numeric_data.dtypes}")
            
            # Fill missing values
            numeric_data = numeric_data.ffill().bfill()
            
            # Convert to numpy array
            X = numeric_data[base_features].values
            
            # Ensure no NaN values
            X = np.nan_to_num(X, nan=0)
            
            # Scale the data
            scaler = MinMaxScaler()
            X = scaler.fit_transform(X)
            
            # Log shape after preparation
            logging.info(f"X shape after preparation: {X.shape}")
            
            # Reshape for LSTM (samples, time steps, features)
            return X.reshape(1, len(numeric_data), len(base_features))
            
        except Exception as e:
            logging.error(f"Error in prepare_data: {str(e)}")
            logging.error(f"Data columns: {data.columns.tolist()}")
            logging.error(f"Data head: \n{data.head()}")
            raise
        
    def save_prediction(self, crypto_id, predictions, confidence, model_version):
        """Save predictions to database using SQLAlchemy"""
        try:
            with self.engine.connect() as connection:
                # Log the predictions
                logging.info(f"Predictions shape in save_prediction: {predictions.shape}")
                logging.info(f"Predictions content: {predictions}")
                
                # Get the current price
                current_price = self.last_current_price
                
                # Calculate predicted prices with diminishing returns
                predicted_change = float(predictions[0][0])
                price_24h = current_price * (1 + predicted_change)
                price_48h = current_price * (1 + predicted_change * 1.5)  # Not full 2x
                price_3d = current_price * (1 + predicted_change * 2.0)   # Not full 3x
                price_7d = current_price * (1 + predicted_change * 3.0)   # Not full 7x
                
                # Limit extreme predictions (e.g., max 100% increase in 7 days)
                max_7d_change = 2.0  # 100% increase
                if price_7d / current_price > max_7d_change:
                    adjustment_factor = max_7d_change / (price_7d / current_price)
                    price_24h = current_price + (price_24h - current_price) * adjustment_factor
                    price_48h = current_price + (price_48h - current_price) * adjustment_factor
                    price_3d = current_price + (price_3d - current_price) * adjustment_factor
                    price_7d = current_price * max_7d_change

                # Create insert query with SQLAlchemy text
                insert_query = text("""
                    INSERT INTO coingecko_crypto_predictions (
                        crypto_id, prediction_date, prediction_created_at,
                        price_24h, price_48h, price_3d, price_7d,
                        confidence_score, model_version
                    ) VALUES (
                        :crypto_id, :prediction_date, :prediction_created_at,
                        :price_24h, :price_48h, :price_3d, :price_7d,
                        :confidence_score, :model_version
                    )
                """)
                
                # Prepare parameters
                params = {
                    'crypto_id': crypto_id,
                    'prediction_date': datetime.now(),
                    'prediction_created_at': datetime.now(),
                    'price_24h': price_24h,
                    'price_48h': price_48h,
                    'price_3d': price_3d,
                    'price_7d': price_7d,
                    'confidence_score': float(confidence),
                    'model_version': model_version
                }
                
                # Execute insert
                connection.execute(insert_query, params)
                connection.commit()
                
                logging.info(f"Successfully saved predictions for {crypto_id}")
                
        except Exception as e:
            logging.error(f"Error saving predictions for {crypto_id}: {str(e)}")
            logging.error(f"Predictions shape: {predictions.shape}")
            logging.error(f"Predictions content: {predictions}")
            import traceback
            logging.error(f"Detailed error: {traceback.format_exc()}")
        
    def make_predictions(self):
        """Make predictions with enhanced accuracy checks"""
        try:
            # Create comma-separated string of stablecoin IDs
            stablecoin_list = "'" + "','".join(self.stablecoin_identifiers) + "'"
            
            # Create the query using SQLAlchemy text()
            query = text(f"""
                SELECT id, symbol, name, market_cap_rank 
                FROM coingecko_crypto_master 
                WHERE id NOT IN ({stablecoin_list})
                    AND market_cap_rank IS NOT NULL  -- Only get ranked cryptos
                ORDER BY market_cap_rank ASC
            """)
            
            with self.engine.connect() as connection:
                result = connection.execute(query)
                crypto_ids = result.fetchall()
                logging.info(f"Found {len(crypto_ids)} cryptocurrencies to process")
            
            for crypto_id, symbol, name, rank in crypto_ids:
                try:
                    logging.info(f"Processing {name} ({crypto_id})")
                    
                    model_files = glob.glob(f'models/{crypto_id}_LSTM_v1_*.keras')
                    if not model_files:
                        logging.warning(f"No model found for {name} ({crypto_id})")
                        continue
                        
                    latest_model_file = max(model_files)
                    logging.info(f"Loading model from {latest_model_file}")
                    
                    model = load_model(latest_model_file)
                    model_version = os.path.basename(latest_model_file).replace('.keras', '')
                    
                    data = self.get_latest_data(crypto_id)
                    logging.info(f"Retrieved {len(data)} rows of data for {name}")
                    
                    if len(data) < self.sequence_length:
                        logging.warning(f"Insufficient data for {name} ({crypto_id})")
                        continue
                        
                    X = self.prepare_data(data)
                    logging.info(f"Prepared data shape: {X.shape}")
                    
                    predictions = model.predict(X, verbose=0)
                    logging.info(f"Made predictions: {predictions.shape}")
                    
                    self.save_prediction(crypto_id, predictions, 0.8, model_version)
                    
                except Exception as e:
                    logging.error(f"Error processing {name}: {str(e)}")
                    import traceback
                    logging.error(f"Detailed error: {traceback.format_exc()}")
                    continue
                
        except Exception as e:
            logging.error(f"Error in make_predictions: {str(e)}")
            import traceback
            logging.error(f"Detailed error: {traceback.format_exc()}")

if __name__ == "__main__":
    try:
        predictor = CryptoPredictor()
        predictor.make_predictions()
    except Exception as e:
        logging.error(f"Main execution error: {str(e)}")
        import traceback
        logging.error(f"Detailed error: {traceback.format_exc()}")