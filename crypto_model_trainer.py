import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense, Dropout
from tensorflow.keras.optimizers import Adam
import pyodbc
import os
from datetime import datetime
from config import DB_CONNECTION_STRING

class CryptoModelTrainer:
    def __init__(self):
        self.conn = pyodbc.connect(DB_CONNECTION_STRING)
        self.min_days_required = 90  # Minimum days of data required
        self.sequence_length = 10    # Number of time steps to look back
        self.models_dir = 'models'
        
        # Create models directory if it doesn't exist
        if not os.path.exists(self.models_dir):
            os.makedirs(self.models_dir)
            
    def log(self, message):
        """Log a message with timestamp"""
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}")

    def prepare_data(self, data, target_column):
        """Prepare sequences for LSTM"""
        scaler = MinMaxScaler()
        scaled_data = scaler.fit_transform(data[['current_price', 'market_cap', 'total_volume']])
        
        X, y = [], []
        for i in range(len(scaled_data) - self.sequence_length - target_column):
            X.append(scaled_data[i:(i + self.sequence_length)])
            y.append(scaled_data[i + self.sequence_length + target_column - 1, 0])  # Price is first column
            
        return np.array(X), np.array(y), scaler

    def create_model(self, input_shape):
        """Create LSTM model"""
        model = Sequential([
            LSTM(50, return_sequences=True, input_shape=input_shape),
            Dropout(0.2),
            LSTM(50),
            Dropout(0.2),
            Dense(1)
        ])
        model.compile(optimizer=Adam(learning_rate=0.001), loss='mse')
        return model

    def train_models(self):
        """Train models for all suitable cryptocurrencies"""
        try:
            # Get list of cryptocurrencies with sufficient data
            query = """
            SELECT m.id, m.name, m.symbol, COUNT(d.id) as day_count
            FROM coingecko_crypto_master m
            JOIN coingecko_crypto_daily_data d ON m.id = d.crypto_id
            GROUP BY m.id, m.name, m.symbol
            HAVING COUNT(d.id) >= ?
            ORDER BY m.market_cap_rank
            """
            
            df = pd.read_sql(query, self.conn, params=(self.min_days_required,))
            total_coins = len(df)
            self.log(f"Found {total_coins} coins with sufficient data")

            for idx, (coin_id, name, symbol, day_count) in enumerate(df.values, 1):
                self.log(f"\nProcessing {name} ({idx}/{total_coins})")
                
                try:
                    # Get historical data
                    data = pd.read_sql(f"""
                        SELECT price_date, current_price, market_cap, total_volume
                        FROM coingecko_crypto_daily_data
                        WHERE crypto_id = '{coin_id}'
                        ORDER BY price_date ASC
                    """, self.conn)
                    
                    # Train models for different prediction horizons
                    for days in [1, 2, 3, 7]:  # 24h, 48h, 3d, 7d predictions
                        model_name = f"{symbol.lower()}_{days}d"
                        self.log(f"Training {model_name} model...")
                        
                        # Prepare data
                        X, y, scaler = self.prepare_data(data, days)
                        
                        if len(X) < 50:  # Skip if not enough sequences
                            self.log(f"Insufficient sequences for {model_name}")
                            continue
                            
                        # Create and train model
                        model = self.create_model((self.sequence_length, 3))
                        model.fit(X, y, epochs=50, batch_size=32, validation_split=0.2, verbose=0)
                        
                        # Save model and scaler
                        model_path = os.path.join(self.models_dir, f"{model_name}.h5")
                        scaler_path = os.path.join(self.models_dir, f"{model_name}_scaler.pkl")
                        
                        model.save(model_path)
                        pd.to_pickle(scaler, scaler_path)
                        
                        self.log(f"Saved {model_name} model and scaler")
                        
                except Exception as e:
                    self.log(f"Error processing {name}: {str(e)}")
                    continue
                    
        except Exception as e:
            self.log(f"Error in train_models: {str(e)}")
        finally:
            self.conn.close()

if __name__ == "__main__":
    trainer = CryptoModelTrainer()
    trainer.train_models() 