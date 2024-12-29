from datetime import datetime, timedelta, date
import logging
from sqlalchemy import create_engine, text
import requests
import time

class CryptoCompareHistoricalLoader:
    def __init__(self):
        # Hardcoded configuration
        DB_SERVER = 'MICROBOX\\SQLEXPRESS'
        DB_NAME = 'CryptoAiDb'
        DB_USER = 'CryptoAdm'
        DB_PASSWORD = 'oracle69'
        CRYPTOCOMPARE_API_KEY = 'e50f480237f72014cc79a1141b4be8750d32c9f714fdfdc7a751183843404b92'
        
        self.connection_str = f'mssql+pyodbc://{DB_USER}:{DB_PASSWORD}@{DB_SERVER}/{DB_NAME}?driver=SQL+Server+Native+Client+11.0'
        self.engine = create_engine(self.connection_str)
        self.api_key = CRYPTOCOMPARE_API_KEY
        
    def log(self, message):
        """Log a message with timestamp"""
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}")

    def fetch_historical_data(self, symbol, start_date, end_date):
        """Fetch historical price data from CryptoCompare"""
        try:
            start_ts = int(datetime.combine(start_date, datetime.min.time()).timestamp())
            end_ts = int(datetime.combine(end_date, datetime.min.time()).timestamp())
            
            url = "https://min-api.cryptocompare.com/data/v2/histoday"
            params = {
                "fsym": symbol,
                "tsym": "USD",
                "toTs": end_ts,
                "limit": 2000,
                "api_key": self.api_key
            }
            
            response = requests.get(url, params=params)
            if response.status_code == 429:  # Rate limit
                self.log("Rate limit hit - waiting 30 seconds...")
                time.sleep(30)
                return self.fetch_historical_data(symbol, start_date, end_date)
                
            data = response.json()
            
            if data['Response'] == 'Success':
                return data['Data']['Data']
            else:
                self.log(f"API Error for {symbol}: {data.get('Message', 'Unknown error')}")
                return None
                
        except Exception as e:
            self.log(f"Error fetching data for {symbol}: {str(e)}")
            return None

    def load_historical_data(self, days=730):  # 2 years of data
        """Load historical data for all coins"""
        try:
            # Get coins that need historical data
            query = text("""
                SELECT id, symbol, name, market_cap_rank 
                FROM coingecko_crypto_master 
                WHERE market_cap_rank <= 100
                ORDER BY market_cap_rank ASC
            """)
            
            with self.engine.connect() as conn:
                coins = conn.execute(query).fetchall()
                
            target_end = datetime.now().date()
            target_start = (datetime.now() - timedelta(days=days)).date()
            
            self.log(f"Loading historical data from {target_start} to {target_end}")
            
            for coin_id, symbol, name, rank in coins:
                try:
                    # Check existing data
                    query = text("""
                        SELECT MIN(CAST(price_date AS DATE)) as earliest_date,
                               MAX(CAST(price_date AS DATE)) as latest_date
                        FROM coingecko_crypto_daily_data
                        WHERE crypto_id = :coin_id
                    """)
                    
                    with self.engine.connect() as conn:
                        result = conn.execute(query, {"coin_id": coin_id}).first()
                        earliest = result[0] if result[0] else None
                        latest = result[1] if result[1] else None

                    if earliest and latest:
                        self.log(f"{name} existing data: {earliest} to {latest}")
                        
                        # Fill gaps in data
                        if earliest > target_start:
                            self.log(f"Fetching earlier data for {name}: {target_start} to {earliest}")
                            historical_data = self.fetch_historical_data(symbol.upper(), target_start, earliest)
                            if historical_data:
                                self._insert_historical_data(coin_id, historical_data, rank)
                        
                        if latest < target_end:
                            self.log(f"Fetching newer data for {name}: {latest} to {target_end}")
                            historical_data = self.fetch_historical_data(symbol.upper(), latest, target_end)
                            if historical_data:
                                self._insert_historical_data(coin_id, historical_data, rank)
                    else:
                        # No existing data, fetch full range
                        self.log(f"Fetching full range for {name}: {target_start} to {target_end}")
                        historical_data = self.fetch_historical_data(symbol.upper(), target_start, target_end)
                        if historical_data:
                            self._insert_historical_data(coin_id, historical_data, rank)
                    
                    time.sleep(1)  # Rate limiting
                    
                except Exception as e:
                    self.log(f"Error processing {name}: {str(e)}")
                    continue
                    
        except Exception as e:
            self.log(f"Error in load_historical_data: {str(e)}")

    def _insert_historical_data(self, coin_id, historical_data, rank):
        """Insert historical data points"""
        records_added = 0
        
        for data_point in historical_data:
            try:
                price_date = datetime.fromtimestamp(data_point['time'])
                
                # Calculate price change
                price_change_24h = data_point['close'] - data_point['open']
                
                query = text("""
                    INSERT INTO coingecko_crypto_daily_data (
                        crypto_id, price_date, current_price, 
                        market_cap, total_volume, price_change_24h,
                        market_cap_rank, created_at
                    )
                    SELECT 
                        :coin_id, :price_date, :current_price,
                        :market_cap, :total_volume, :price_change_24h,
                        :rank, GETDATE()
                    WHERE NOT EXISTS (
                        SELECT 1 
                        FROM coingecko_crypto_daily_data 
                        WHERE crypto_id = :coin_id 
                        AND CAST(price_date AS DATE) = CAST(:price_date AS DATE)
                    )
                """)
                
                with self.engine.connect() as conn:
                    result = conn.execute(query, {
                        "coin_id": coin_id,
                        "price_date": price_date,
                        "current_price": data_point['close'],
                        "market_cap": data_point.get('market_cap', 0),
                        "total_volume": data_point['volumeto'],
                        "price_change_24h": price_change_24h,
                        "rank": rank
                    })
                    conn.commit()
                    
                    if result.rowcount > 0:
                        records_added += 1
                        
            except Exception as e:
                self.log(f"Error inserting data point: {str(e)}")
                continue
                
        self.log(f"Added {records_added} historical records for {coin_id}")

if __name__ == "__main__":
    loader = CryptoCompareHistoricalLoader()
    loader.load_historical_data()  # Will fetch 2 years of data