import requests
from datetime import datetime
import time
from sqlalchemy import create_engine, text
from config import (
    DB_CONNECTION_STRING,
    DB_SERVER,
    DB_NAME,
    DB_USER,
    DB_PASSWORD
)

class CoinGeckoHistoricalLoader:
    def __init__(self):
        self.base_url = "https://api.coingecko.com/api/v3"
        self.connection_str = f'mssql+pyodbc://{DB_USER}:{DB_PASSWORD}@{DB_SERVER}/{DB_NAME}?driver=SQL+Server+Native+Client+11.0'
        self.engine = create_engine(self.connection_str)
        
    def log(self, message):
        """Log a message with timestamp"""
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}")
        
    def make_request(self, url, params=None):
        """Make API request with rate limit handling and verbose logging"""
        try:
            response = requests.get(url, params=params, timeout=10)
            self.log(f"API Response Status: {response.status_code}")
            
            if response.status_code == 429:  # Rate limit hit
                self.log("Rate limit hit - waiting 65 seconds...")
                time.sleep(65)
                return self.make_request(url, params)
            elif response.status_code != 200:
                self.log(f"API Error: {response.text}")
                time.sleep(2)  # Wait before retry
                return self.make_request(url, params)
            
            return response
        except Exception as e:
            self.log(f"Request error: {str(e)}")
            time.sleep(2)  # Wait before retry
            return None

    def get_earliest_date(self, coin_id):
        """Get earliest date for a coin in our database"""
        query = text("""
            SELECT MIN(CAST(price_date AS DATE))
            FROM coingecko_crypto_daily_data
            WHERE crypto_id = :coin_id
        """)
        with self.engine.connect() as conn:
            result = conn.execute(query, {"coin_id": coin_id}).scalar()
        return result

    def load_historical_data(self, days=1825):  # Default to 5 years
        """Load historical data for all coins"""
        try:
            # Get top 100 coins
            query = text("""
                SELECT id, name, market_cap_rank 
                FROM coingecko_crypto_master 
                WHERE market_cap_rank <= 100
                ORDER BY market_cap_rank ASC
            """)
            
            with self.engine.connect() as conn:
                coins = conn.execute(query).fetchall()
                
            total_coins = len(coins)
            self.log(f"Starting historical data collection for {total_coins} coins")
            start_time = time.time()
            
            for idx, (coin_id, name, rank) in enumerate(coins, 1):
                earliest_date = self.get_earliest_date(coin_id)
                if earliest_date:
                    self.log(f"Earliest data for {name}: {earliest_date}")
                
                elapsed = int(time.time() - start_time)
                eta = int((elapsed / idx) * (total_coins - idx)) if idx > 0 else 0
                
                self.log(f"\nProcessing {name} ({idx}/{total_coins}) - ETA: {eta//60}m {eta%60}s")
                
                try:
                    # Get historical market data
                    response = self.make_request(
                        f"{self.base_url}/coins/{coin_id}/market_chart",
                        params={
                            'vs_currency': 'usd',
                            'days': days,
                            'interval': 'daily'
                        }
                    )
                    
                    if response and response.status_code == 200:
                        data = response.json()
                        records_added = 0
                        
                        # Process each day's data
                        for i in range(len(data['prices'])):
                            try:
                                timestamp = datetime.fromtimestamp(data['prices'][i][0] / 1000)
                                
                                # Calculate price change
                                price_change_24h = None
                                if i > 0:
                                    prev_price = data['prices'][i-1][1]
                                    curr_price = data['prices'][i][1]
                                    price_change_24h = curr_price - prev_price
                                
                                # Insert only if we don't have this date
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
                                        "price_date": timestamp,
                                        "current_price": data['prices'][i][1],
                                        "market_cap": data['market_caps'][i][1],
                                        "total_volume": data['total_volumes'][i][1],
                                        "price_change_24h": price_change_24h,
                                        "rank": rank
                                    })
                                    conn.commit()
                                    
                                    if result.rowcount > 0:
                                        records_added += 1
                                    
                            except Exception as e:
                                self.log(f"Error processing data point for {name}: {str(e)}")
                                continue
                        
                        self.log(f"Added {records_added} historical records for {name}")
                        
                    # Sleep to respect rate limits
                    time.sleep(1.5)
                    
                except Exception as e:
                    self.log(f"Error processing {name}: {str(e)}")
                    continue
                    
        except Exception as e:
            self.log(f"Error in load_historical_data: {str(e)}")

if __name__ == "__main__":
    loader = CoinGeckoHistoricalLoader()
    loader.load_historical_data()  # Will fetch 5 years of data 