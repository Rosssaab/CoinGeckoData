import os
# Set environment variables for reproducibility
os.environ['TF_ENABLE_ONEDNN_OPTS'] = '0'
os.environ['PYTHONHASHSEED'] = '0'

import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler
from tensorflow.keras.models import Sequential, load_model
from tensorflow.keras.layers import LSTM, Dense, Dropout
from tensorflow.keras.optimizers import Adam
from datetime import datetime, timedelta
import logging
import tensorflow as tf
import random
from sqlalchemy import create_engine
from sqlalchemy.types import String, DateTime, Float, Integer
import urllib
from config import DB_CONNECTION_STRING

# Set random seeds for reproducibility
np.random.seed(42)
random.seed(42)
tf.random.set_seed(42)

# Configure TensorFlow for deterministic operations
tf.config.threading.set_inter_op_parallelism_threads(1)
tf.config.threading.set_intra_op_parallelism_threads(1)

class CryptoModelTrainer:
    def __init__(self):
        """Initialize the trainer"""
        self.model_version = f"LSTM_v1_{datetime.now().strftime('%Y%m%d')}"
        self.scaler = MinMaxScaler()
        
        # Create SQLAlchemy engine using the existing connection string
        params = urllib.parse.quote_plus(DB_CONNECTION_STRING)
        self.engine = create_engine(
            f'mssql+pyodbc:///?odbc_connect={params}',
            fast_executemany=True
        )
        
        self.sequence_length = 3  # Changed from 10 to 3 to match available data
        
        # Setup logging
        os.makedirs('logs', exist_ok=True)
        os.makedirs('models', exist_ok=True)
        
        logging.basicConfig(
            filename=f'logs/model_trainer_{datetime.now().strftime("%Y%m%d")}.log',
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        
    def get_training_data(self, crypto_id):
        """Get historical data for training"""
        query = """
            SELECT 
                p.crypto_id,
                p.price_date as date,
                p.current_price,
                p.market_cap,
                p.total_volume,
                p.price_change_24h,
                s.sentiment_votes_up,
                s.sentiment_votes_down,
                s.public_interest_score
            FROM coingecko_crypto_daily_data p
            LEFT JOIN coingecko_crypto_sentiment s 
                ON p.crypto_id = s.crypto_id 
                AND CAST(p.price_date AS DATE) = CAST(s.metric_date AS DATE)
            WHERE p.crypto_id = ?
            ORDER BY p.price_date
        """
        
        try:
            # Execute query with parameters as a list
            df = pd.read_sql_query(
                sql=query,
                con=self.engine,
                params=(crypto_id,)  # Note the comma to make it a tuple
            )
            return df
        except Exception as e:
            logging.error(f"Database error for {crypto_id}: {str(e)}")
            return pd.DataFrame()  # Return empty DataFrame on error
            
    def prepare_sequences(self, data):
        """Prepare sequences for LSTM"""
        # Fill missing values with forward fill then backward fill
        data = data.ffill().bfill()
        
        features = ['current_price', 'market_cap', 'total_volume', 'price_change_24h',
                   'sentiment_votes_up', 'sentiment_votes_down', 'public_interest_score']
        
        X = []
        y_24h = []
        y_48h = []
        y_3d = []
        y_7d = []
        
        for i in range(len(data) - self.sequence_length - 7):
            X.append(data[features].iloc[i:i+self.sequence_length].values)
            
            future_prices = data['current_price'].iloc[i+self.sequence_length:]
            y_24h.append(future_prices.iloc[1] if len(future_prices) > 1 else None)
            y_48h.append(future_prices.iloc[2] if len(future_prices) > 2 else None)
            y_3d.append(future_prices.iloc[3] if len(future_prices) > 3 else None)
            y_7d.append(future_prices.iloc[7] if len(future_prices) > 7 else None)
            
        return np.array(X), np.array(y_24h), np.array(y_48h), np.array(y_3d), np.array(y_7d)
        
    def build_model(self, input_shape):
        """Build LSTM model"""
        model = Sequential([
            LSTM(100, return_sequences=True, input_shape=input_shape),
            Dropout(0.2),
            LSTM(50, return_sequences=False),
            Dropout(0.2),
            Dense(25),
            Dense(4)  # 4 outputs for different time horizons
        ])
        
        model.compile(optimizer=Adam(learning_rate=0.001),
                     loss='mse',
                     metrics=['mae'])
        return model
        
    def save_model_performance(self, crypto_id, mae_scores, rmse_scores, samples):
        """Save model performance metrics"""
        try:
            # Create a dictionary for the performance data
            performance_data = {
                'crypto_id': crypto_id,
                'model_version': self.model_version,
                'training_date': datetime.now(),
                'mae_24h': float(mae_scores[0]),
                'mae_48h': float(mae_scores[1]),
                'mae_3d': float(mae_scores[2]),
                'mae_7d': float(mae_scores[3]),
                'rmse_24h': float(rmse_scores[0]),
                'rmse_48h': float(rmse_scores[1]),
                'rmse_3d': float(rmse_scores[2]),
                'rmse_7d': float(rmse_scores[3]),
                'training_samples': int(samples),
                'notes': f"Training completed for {crypto_id}"
            }
            
            # Convert to DataFrame and save
            df = pd.DataFrame([performance_data])
            df.to_sql('coingecko_model_performance', 
                     self.engine, 
                     if_exists='append', 
                     index=False,
                     dtype={
                         'crypto_id': String,
                         'model_version': String,
                         'training_date': DateTime,
                         'mae_24h': Float,
                         'mae_48h': Float,
                         'mae_3d': Float,
                         'mae_7d': Float,
                         'rmse_24h': Float,
                         'rmse_48h': Float,
                         'rmse_3d': Float,
                         'rmse_7d': Float,
                         'training_samples': Integer,
                         'notes': String
                     })
            
            print(f"Saved performance metrics for {crypto_id}")
            logging.info(f"Saved performance metrics for {crypto_id}")
            
        except Exception as e:
            print(f"Error saving model performance for {crypto_id}: {str(e)}")
            logging.error(f"Error saving model performance for {crypto_id}: {str(e)}")

    def train_model(self, crypto_id):
        """Train model for a specific cryptocurrency"""
        try:
            logging.info(f"Starting training for {crypto_id}")
            print(f"Starting training for {crypto_id}")
            
            # Get and prepare data
            data = self.get_training_data(crypto_id)
            
            # Reduce minimum data requirement to match what we have
            if len(data) < 5:  # Changed to 5 since that's our maximum
                logging.warning(f"Insufficient data for {crypto_id} (only {len(data)} samples)")
                print(f"Skipping {crypto_id} - insufficient data ({len(data)} samples)")
                return False
                
            print(f"Got {len(data)} samples for {crypto_id}")
            
            X, y_24h, y_48h, y_3d, y_7d = self.prepare_sequences(data)
            print(f"Prepared {len(X)} sequences")
            
            # Scale features
            X_scaled = self.scaler.fit_transform(X.reshape(-1, X.shape[-1])).reshape(X.shape)
            y = np.column_stack([y_24h, y_48h, y_3d, y_7d])
            
            # Build and train model
            model = self.build_model((X.shape[1], X.shape[2]))
            print(f"Training model for {crypto_id}...")
            
            history = model.fit(
                X_scaled, y,
                epochs=50,
                batch_size=32,
                validation_split=0.2,
                verbose=1
            )
            
            # Save model with explicit error handling
            try:
                model_path = f'models/{crypto_id}_{self.model_version}.h5'
                print(f"Attempting to save model to {model_path}")  # Debug print
                model.save(model_path)
                print(f"Successfully saved model to {model_path}")
                
            except Exception as save_error:
                print(f"Error saving model for {crypto_id}: {str(save_error)}")
                logging.error(f"Error saving model for {crypto_id}: {str(save_error)}")
                return False
            
            # Calculate performance metrics
            mae_scores = [
                history.history['val_mae'][-1],
                history.history['val_mae'][-1],
                history.history['val_mae'][-1],
                history.history['val_mae'][-1]
            ]
            
            rmse_scores = [
                np.sqrt(history.history['val_loss'][-1]),
                np.sqrt(history.history['val_loss'][-1]),
                np.sqrt(history.history['val_loss'][-1]),
                np.sqrt(history.history['val_loss'][-1])
            ]
            
            # Save model and performance metrics
            model_path = f'models/{crypto_id}_{self.model_version}.h5'
            model.save(model_path)
            print(f"Saved model to {model_path}")  # Added console output
            
            self.save_model_performance(crypto_id, mae_scores, rmse_scores, len(X))
            print(f"Completed training for {crypto_id} with MAE: {mae_scores[0]:.4f}")  # Added console output
            
            logging.info(f"Successfully trained model for {crypto_id}")
            return True
            
        except Exception as e:
            logging.error(f"Error training model for {crypto_id}: {str(e)}")
            print(f"Error training model for {crypto_id}: {str(e)}")  # Added console output
            return False

    def train_all_models(self):
        """Train models for all cryptocurrencies"""
        df = pd.read_sql("SELECT id FROM coingecko_crypto_master", self.engine)
        crypto_ids = df['id'].tolist()
        
        print(f"Starting training for {len(crypto_ids)} cryptocurrencies")
        successful = 0
        
        for idx, crypto_id in enumerate(crypto_ids, 1):
            print(f"\nProgress: {idx}/{len(crypto_ids)} ({(idx/len(crypto_ids)*100):.1f}%)")
            if self.train_model(crypto_id):
                successful += 1
        
        print(f"\nTraining completed. Successfully trained {successful}/{len(crypto_ids)} models")

if __name__ == "__main__":
    trainer = CryptoModelTrainer()
    trainer.train_all_models() 