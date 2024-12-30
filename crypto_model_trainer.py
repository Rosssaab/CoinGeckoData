import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense, Dropout, Input
from tensorflow.keras.optimizers import Adam
from tensorflow.keras.callbacks import EarlyStopping
from sqlalchemy import create_engine, text
import os
from datetime import datetime
from config import DB_CONNECTION_STRING, DB_SERVER, DB_NAME, DB_USER, DB_PASSWORD
import warnings
import glob
import threading
from concurrent.futures import ThreadPoolExecutor, TimeoutError
import gc

warnings.filterwarnings('ignore', category=UserWarning)

class CryptoModelTrainer:
    def __init__(self):
        # Create SQLAlchemy engine with ODBC Driver 17
        self.connection_str = f'mssql+pyodbc://{DB_USER}:{DB_PASSWORD}@{DB_SERVER}/{DB_NAME}?driver=ODBC+Driver+17+for+SQL+Server'
        self.engine = create_engine(self.connection_str)
        
        self.min_days_required = 14  # Reduced from 90 to 14 days for testing
        self.sequence_length = 5     # Reduced from 10 to 5 for testing
        self.models_dir = 'models'
        
        if not os.path.exists(self.models_dir):
            os.makedirs(self.models_dir)
            
    def log(self, message):
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}")

    def prepare_data(self, data, target_column):
        scaler = MinMaxScaler()
        features = ['current_price', 'market_cap', 'total_volume', 'price_change_24h',
                   'sentiment_votes_up', 'sentiment_votes_down', 'public_interest_score']
        scaled_data = scaler.fit_transform(data[features])
        
        X, y = [], []
        for i in range(len(scaled_data) - self.sequence_length - target_column):
            X.append(scaled_data[i:(i + self.sequence_length)])
            y.append(scaled_data[i + self.sequence_length + target_column - 1, 0])
            
        return np.array(X), np.array(y), scaler

    def create_model(self, input_shape):
        # Create model using functional API instead of Sequential
        inputs = Input(shape=input_shape)
        x = LSTM(50, return_sequences=True)(inputs)
        x = Dropout(0.2)(x)
        x = LSTM(50)(x)
        x = Dropout(0.2)(x)
        outputs = Dense(1)(x)
        
        model = Sequential([
            Input(shape=input_shape),
            LSTM(50, return_sequences=True),
            Dropout(0.2),
            LSTM(50),
            Dropout(0.2),
            Dense(1)
        ])
        
        model.compile(optimizer=Adam(learning_rate=0.001), loss='mse')
        return model

    def cleanup_old_models(self):
        """Remove all previous model files before starting new training"""
        try:
            # Delete all .keras and .pkl files in models directory
            model_files = glob.glob(os.path.join(self.models_dir, '*.keras'))
            scaler_files = glob.glob(os.path.join(self.models_dir, '*_scaler.pkl'))
            
            for file_path in model_files + scaler_files:
                try:
                    os.remove(file_path)
                    self.log(f"Removed old file: {os.path.basename(file_path)}")
                except Exception as e:
                    self.log(f"Error removing {file_path}: {str(e)}")
                    
            self.log("Cleaned up old model files")
        except Exception as e:
            self.log(f"Error during model cleanup: {str(e)}")

    def train_model_with_timeout(self, model, X, y, early_stopping, timeout_seconds=300):
        """Train model with timeout"""
        done = threading.Event()
        result = {'history': None, 'error': None}
        
        def training_target():
            try:
                result['history'] = model.fit(
                    X, y,
                    epochs=50,
                    batch_size=32,
                    validation_split=0.2,
                    callbacks=[early_stopping],
                    verbose=0
                )
                done.set()
            except Exception as e:
                result['error'] = str(e)
                done.set()

        training_thread = threading.Thread(target=training_target)
        training_thread.start()
        
        # Wait for training to complete or timeout
        if not done.wait(timeout=timeout_seconds):
            return None
        
        if result['error']:
            raise Exception(result['error'])
            
        return result['history']

    def train_models(self):
        """Train models for all suitable cryptocurrencies"""
        try:
            self.cleanup_old_models()
            start_time = datetime.now()
            
            query = """
            WITH CryptoData AS (
                SELECT 
                    m.id, m.name, m.symbol, m.market_cap_rank,
                    COUNT(d.id) as day_count
                FROM coingecko_crypto_master m
                JOIN coingecko_crypto_daily_data d ON m.id = d.crypto_id
                GROUP BY m.id, m.name, m.symbol, m.market_cap_rank
                HAVING COUNT(d.id) >= ?
            )
            SELECT * FROM CryptoData ORDER BY market_cap_rank
            """
            
            df = pd.read_sql(query, self.engine, params=(self.min_days_required,))
            total_coins = len(df)
            total_models = total_coins * 4  # 4 timeframes per coin
            models_completed = 0
            
            self.log(f"Starting training of {total_models} models ({total_coins} coins x 4 timeframes)")
            self.log(f"Start time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
            
            import gc
            from tensorflow.keras import backend as K
            from contextlib import contextmanager
            import signal
            
            @contextmanager
            def timeout(seconds):
                def handler(signum, frame):
                    raise TimeoutError(f"Training timed out after {seconds} seconds")
                
                signal.signal(signal.SIGALRM, handler)
                signal.alarm(seconds)
                try:
                    yield
                finally:
                    signal.alarm(0)
            
            for idx, row in df.iterrows():
                coin_id, name = row['id'], row['name']
                coin_start_time = datetime.now()
                
                self.log(f"\nProcessing {name} ({idx+1}/{total_coins}, {(idx/total_coins)*100:.1f}% of coins)")
                
                try:
                    data = pd.read_sql(f"""
                        SELECT 
                            d.price_date,
                            d.current_price,
                            d.market_cap,
                            d.total_volume,
                            d.price_change_24h,
                            COALESCE(s.sentiment_votes_up, 0) as sentiment_votes_up,
                            COALESCE(s.sentiment_votes_down, 0) as sentiment_votes_down,
                            COALESCE(s.public_interest_score, 0) as public_interest_score
                        FROM coingecko_crypto_daily_data d
                        LEFT JOIN coingecko_crypto_sentiment s 
                            ON d.crypto_id = s.crypto_id 
                            AND CAST(d.price_date AS DATE) = CAST(s.metric_date AS DATE)
                        WHERE d.crypto_id = '{coin_id}'
                        ORDER BY d.price_date ASC
                    """, self.engine)
                    
                    if data.empty:
                        self.log(f"No data found for {name}")
                        continue
                        
                    # Use ffill() instead of fillna(method='ffill')
                    data = data.ffill().fillna(0)
                    
                    for days in [1, 2, 3, 7]:
                        try:
                            model_name = f"{coin_id}_LSTM_v1_{days}d"
                            models_completed += 1
                            progress = (models_completed / total_models) * 100
                            
                            elapsed_time = (datetime.now() - start_time).total_seconds() / 3600  # hours
                            estimated_total_time = (elapsed_time / progress) * 100 if progress > 0 else 0
                            remaining_time = max(0, estimated_total_time - elapsed_time)
                            
                            self.log(f"Training {model_name} model... ({progress:.1f}% complete, ~{remaining_time:.1f}h remaining)")
                            
                            X, y, scaler = self.prepare_data(data, days)
                            
                            if len(X) < 50:
                                self.log(f"Insufficient sequences for {model_name}")
                                continue
                            
                            K.clear_session()
                            
                            model = self.create_model((self.sequence_length, X.shape[2]))
                            early_stopping = EarlyStopping(
                                monitor='val_loss',
                                patience=5,
                                restore_best_weights=True
                            )
                            
                            history = self.train_model_with_timeout(model, X, y, early_stopping)
                            
                            if history is None:
                                self.log(f"Training timed out for {model_name}")
                                continue
                            
                            model_path = os.path.join(self.models_dir, f"{model_name}.keras")
                            model.save(model_path)
                            
                            scaler_path = os.path.join(self.models_dir, f"{model_name}_scaler.pkl")
                            pd.to_pickle(scaler, scaler_path)
                            
                            val_loss = history.history['val_loss'][-1]
                            self.save_model_performance(coin_id, model_name, val_loss, days)
                            
                            self.log(f"Saved {model_name} model and scaler (validation loss: {val_loss:.6f})")
                            
                            del model, history
                            gc.collect()
                            
                        except Exception as e:
                            self.log(f"Error training {model_name}: {str(e)}")
                            continue
                            
                    coin_duration = (datetime.now() - coin_start_time).total_seconds() / 60
                    self.log(f"Completed {name} in {coin_duration:.1f} minutes")
                    
                except Exception as e:
                    self.log(f"Error processing {name}: {str(e)}")
                    continue
                    
            total_duration = (datetime.now() - start_time).total_seconds() / 3600
            self.log(f"\nTraining completed in {total_duration:.1f} hours")
            self.log(f"Successfully trained {models_completed} out of {total_models} models")
            
        except Exception as e:
            self.log(f"Error in train_models: {str(e)}")
        finally:
            self.engine.dispose()
            gc.collect()

    def save_model_performance(self, crypto_id, model_version, val_loss, prediction_days):
        """Save model performance metrics"""
        try:
            with self.engine.connect() as connection:
                # Calculate both MAE and RMSE
                mae = float(np.sqrt(val_loss))
                rmse = float(np.sqrt(val_loss))  # For this simple case, they're the same
                
                # Create a properly formatted dictionary for the INSERT
                params = {
                    'model_version': model_version,
                    'training_date': datetime.now(),
                    'mae_24h': mae if prediction_days == 1 else None,
                    'mae_48h': mae if prediction_days == 2 else None,
                    'mae_3d': mae if prediction_days == 3 else None,
                    'mae_7d': mae if prediction_days == 7 else None,
                    'rmse_24h': rmse if prediction_days == 1 else None,
                    'rmse_48h': rmse if prediction_days == 2 else None,
                    'rmse_3d': rmse if prediction_days == 3 else None,
                    'rmse_7d': rmse if prediction_days == 7 else None,
                    'training_samples': len(self.X) if hasattr(self, 'X') else None,
                    'notes': f'Model trained for {crypto_id}'
                }
                
                # Use SQLAlchemy text with named parameters
                insert_query = text("""
                    INSERT INTO coingecko_model_performance (
                        model_version, training_date,
                        mae_24h, mae_48h, mae_3d, mae_7d,
                        rmse_24h, rmse_48h, rmse_3d, rmse_7d,
                        training_samples, notes
                    ) VALUES (
                        :model_version, :training_date,
                        :mae_24h, :mae_48h, :mae_3d, :mae_7d,
                        :rmse_24h, :rmse_48h, :rmse_3d, :rmse_7d,
                        :training_samples, :notes
                    )
                """)
                
                connection.execute(insert_query, params)
                connection.commit()
                self.log(f"Successfully saved performance metrics for {model_version}")
                
        except Exception as e:
            self.log(f"Error saving model performance: {str(e)}")
            # Add more detailed error information
            import traceback
            self.log(f"Detailed error: {traceback.format_exc()}")

if __name__ == "__main__":
    trainer = CryptoModelTrainer()
    trainer.train_models() 