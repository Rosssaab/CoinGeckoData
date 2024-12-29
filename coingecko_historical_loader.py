import requests
import pyodbc
from datetime import datetime
import time
from config import DB_CONNECTION_STRING

class CoinGeckoHistoricalLoader:
    def __init__(self):
        self.base_url = "https://api.coingecko.com/api/v3"
        self.conn = pyodbc.connect(DB_CONNECTION_STRING)
        self.cursor = self.conn.cursor()
        
    def log(self, message):
        """Log a message with timestamp"""
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}")
        
    def make_request(self, url, params=None):
        """Make API request with rate limit handling"""
        try:
            # Add timeout of 10 seconds
            response = requests.get(url, params=params, timeout=10)
            if response.status_code == 429:  # Rate limit hit
                self.log("Rate limit hit - waiting 65 seconds...")
                time.sleep(65)  # Wait slightly longer than 1 minute
                return self.make_request(url, params)
            return response
        except Exception as e:
            self.log(f"Request error: {str(e)}")
            return None

    def load_historical_data(self):
        """Load historical data for all coins"""
        try:
            # Get only top 100 coins from master table
            self.cursor.execute("""
                SELECT id, name, market_cap_rank 
                FROM coingecko_crypto_master 
                WHERE market_cap_rank <= 100
                ORDER BY market_cap_rank ASC
            """)
            coins = self.cursor.fetchall()
            total_coins = len(coins)
            
            self.log(f"Starting historical data collection for {total_coins} coins")
            start_time = time.time()
            
            for idx, (coin_id, name, rank) in enumerate(coins, 1):
                elapsed = int(time.time() - start_time)
                eta = int((elapsed / idx) * (total_coins - idx)) if idx > 0 else 0
                
                self.log(f"\nProcessing {name} ({idx}/{total_coins}) - ETA: {eta//60}m {eta%60}s")
                
                try:
                    # Get historical market data
                    response = self.make_request(
                        f"{self.base_url}/coins/{coin_id}/market_chart",
                        params={
                            'vs_currency': 'usd',
                            'days': '365',
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
                                
                                # Skip if we already have data for this date
                                self.cursor.execute("""
                                    SELECT 1 FROM coingecko_crypto_daily_data 
                                    WHERE crypto_id = ? AND CAST(price_date AS DATE) = CAST(? AS DATE)
                                """, coin_id, timestamp)
                                
                                if not self.cursor.fetchone():
                                    self.cursor.execute("""
                                        INSERT INTO coingecko_crypto_daily_data (
                                            crypto_id, 
                                            price_date,
                                            current_price, 
                                            market_cap,
                                            total_volume,
                                            market_cap_rank,
                                            created_at
                                        ) VALUES (?, ?, ?, ?, ?, ?, GETDATE())
                                    """,
                                    coin_id,
                                    timestamp,
                                    data['prices'][i][1],
                                    data['market_caps'][i][1],
                                    data['total_volumes'][i][1],
                                    rank,
                                    )
                                    records_added += 1
                                    
                            except Exception as e:
                                self.log(f"Error processing data point for {name}: {str(e)}")
                                continue
                        
                        self.conn.commit()
                        self.log(f"Added {records_added} historical records for {name}")
                        
                    # Sleep to respect rate limits
                    time.sleep(1.5)  # CoinGecko allows ~50 calls per minute
                    
                except Exception as e:
                    self.log(f"Error processing {name}: {str(e)}")
                    continue
                    
        except Exception as e:
            self.log(f"Error in load_historical_data: {str(e)}")
        finally:
            self.cursor.close()
            self.conn.close()

if __name__ == "__main__":
    loader = CoinGeckoHistoricalLoader()
    loader.load_historical_data() 