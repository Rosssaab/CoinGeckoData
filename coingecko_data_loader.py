import requests
import argparse
import pyodbc
from datetime import datetime
import time

class CoinGeckoDataLoader:
    def __init__(self, args):
        self.base_url = "https://api.coingecko.com/api/v3"
        self.args = args
        self.stats = {}
        
        # Fix the escape sequence in connection string
        self.conn = pyodbc.connect(r'Driver={SQL Server};'
                                 r'Server=.\SQLEXPRESS;'
                                 r'Database=CryptoAiDb;'
                                 r'Trusted_Connection=yes;')
        self.cursor = self.conn.cursor()
        
    def log(self, message):
        """Log a message with timestamp"""
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}")
        
    def check_if_update_needed(self):
        """Check if we need to update data today"""
        try:
            self.log("\n=== Checking Data Status ===")
            
            # Check latest data dates
            self.cursor.execute("""
                SELECT MAX(CAST(price_date AS DATE)) 
                FROM coingecko_crypto_daily_data
            """)
            latest_price_date = self.cursor.fetchone()[0]
            
            self.cursor.execute("""
                SELECT MAX(CAST(metric_date AS DATE)) 
                FROM coingecko_crypto_sentiment
            """)
            latest_sentiment_date = self.cursor.fetchone()[0]
            
            # Count today's records
            today = datetime.now().date()
            self.cursor.execute("""
                SELECT COUNT(*) FROM coingecko_crypto_daily_data 
                WHERE CAST(price_date AS DATE) = CAST(GETDATE() AS DATE)
            """)
            todays_price_records = self.cursor.fetchone()[0]
            
            self.cursor.execute("""
                SELECT COUNT(*) FROM coingecko_crypto_sentiment 
                WHERE CAST(metric_date AS DATE) = CAST(GETDATE() AS DATE)
            """)
            todays_sentiment_records = self.cursor.fetchone()[0]
            
            needs_price_update = latest_price_date != today or todays_price_records < 100
            needs_sentiment_update = latest_sentiment_date != today or todays_sentiment_records < 100
            
            self.log(f"Current date: {today}")
            self.log(f"Price data status: {'COMPLETE' if not needs_price_update else 'NEEDS UPDATE'}")
            self.log(f"  - Latest date: {latest_price_date}")
            self.log(f"  - Today's records: {todays_price_records}")
            
            self.log(f"Sentiment data status: {'COMPLETE' if not needs_sentiment_update else 'NEEDS UPDATE'}")
            self.log(f"  - Latest date: {latest_sentiment_date}")
            self.log(f"  - Today's records: {todays_sentiment_records}")
            
            self.log("===========================\n")
            
            return needs_price_update or needs_sentiment_update
            
        except Exception as e:
            self.log(f"Error checking update status: {str(e)}")
            return True  # If error, assume update needed

    def run(self):
        """Main run method"""
        try:
            self.log("\n=== Starting Data Collection Process ===")
            self.log(f"Time: {datetime.now()}")
            
            if self.check_if_update_needed():
                if self.args.master:
                    self.log("\n--- Updating Master Data ---")
                    self.update_master_data()
                    
                if self.args.daily:
                    self.log("\n--- Updating Daily Price Data ---")
                    self.update_daily_data()
                    
                if self.args.sentiment:
                    self.log("\n--- Updating Sentiment Data ---")
                    self.update_sentiment_data()
                    
                self.log("\n=== Data Collection Complete ===")
                
            else:
                self.log("\nNO UPDATES NEEDED - All data is current for today")
                self.log("Daily Price Data: 100 records for today")
                self.log("Sentiment Data: 100 records for today")
                self.log("\nSkipping data collection to avoid unnecessary API calls")
                
        except Exception as e:
            self.log(f"\nERROR in main run: {str(e)}")
            raise

    def __del__(self):
        """Cleanup database connections"""
        if hasattr(self, 'cursor') and self.cursor:
            self.cursor.close()
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()

    def make_request(self, url, params=None, wait_time=60):
        """Make API request with rate limit handling"""
        try:
            response = requests.get(url, params=params)
            
            if response.status_code == 429:  # Rate limit hit
                self.log(f"Rate limit hit - waiting {wait_time} seconds...")
                self.countdown(wait_time)
                # Exponential backoff - double the wait time for next attempt
                return self.make_request(url, params, min(wait_time * 2, 300))  # Cap at 5 minutes
                
            return response
            
        except Exception as e:
            self.log(f"Request error: {str(e)}")
            return None

    def update_master_data(self):
        """Update master list of coins"""
        try:
            self.log("Fetching master list from CoinGecko...")
            
            response = self.make_request(f"{self.base_url}/coins/markets", params={
                'vs_currency': 'usd',
                'order': 'market_cap_desc',
                'per_page': 100,
                'page': 1,
                'sparkline': False
            })
            
            if response and response.status_code == 200:
                coins = response.json()
                updated_count = 0
                
                for coin in coins:
                    try:
                        # Extract image info
                        image_id = None
                        image_filename = None
                        if 'image' in coin:
                            url_parts = coin['image'].split('/')
                            for i, part in enumerate(url_parts):
                                if part == 'images' and i + 1 < len(url_parts):
                                    try:
                                        image_id = int(url_parts[i + 1])
                                        image_filename = url_parts[-1].split('?')[0]
                                    except ValueError:
                                        continue
                        
                        self.cursor.execute("""
                            MERGE coingecko_crypto_master AS target
                            USING (SELECT ? as id) AS source
                            ON target.id = source.id
                            WHEN MATCHED THEN
                                UPDATE SET 
                                    symbol = ?,
                                    name = ?,
                                    image_id = ?,
                                    image_filename = ?,
                                    market_cap_rank = ?,
                                    created_at = GETDATE()
                            WHEN NOT MATCHED THEN
                                INSERT (id, symbol, name, image_id, image_filename, market_cap_rank, created_at)
                                VALUES (?, ?, ?, ?, ?, ?, GETDATE());
                        """,
                        coin['id'], 
                        coin['symbol'], 
                        coin['name'],
                        image_id,
                        image_filename,
                        coin.get('market_cap_rank'),
                        coin['id'],
                        coin['symbol'],
                        coin['name'],
                        image_id,
                        image_filename,
                        coin.get('market_cap_rank'))
                        
                        if self.cursor.rowcount > 0:
                            updated_count += 1
                            
                        # Small delay between records
                        time.sleep(0.1)
                            
                    except Exception as e:
                        self.log(f"Error updating master data for {coin['id']}: {str(e)}")
                        continue
                        
                self.conn.commit()
                self.log(f"Updated master data for {updated_count} coins")
                
            else:
                self.log(f"Error fetching master list: Status {response.status_code if response else 'No response'}")
                
        except Exception as e:
            self.log(f"Error in update_master_data: {str(e)}")

    def update_daily_data(self):
        """Update daily price data for coins"""
        try:
            self.log("Fetching daily price data from CoinGecko...")
            response = requests.get(f"{self.base_url}/coins/markets", params={
                'vs_currency': 'usd',
                'order': 'market_cap_desc',
                'per_page': 100,
                'page': 1,
                'sparkline': False
            })
            
            if response.status_code == 429:
                self.log("Rate limit hit - waiting 60 seconds...")
                self.countdown(60)
                response = requests.get(f"{self.base_url}/coins/markets", params={
                    'vs_currency': 'usd',
                    'order': 'market_cap_desc',
                    'per_page': 100,
                    'page': 1,
                    'sparkline': False
                })
                
            if response.status_code == 200:
                coins = response.json()
                updated_count = 0
                current_time = datetime.now()
                
                for coin in coins:
                    try:
                        self.cursor.execute("""
                            INSERT INTO coingecko_crypto_daily_data (
                                crypto_id, 
                                price_date,  -- This will now store both date and time
                                current_price, 
                                market_cap,
                                total_volume, 
                                price_change_24h, 
                                market_cap_rank,
                                is_trending, 
                                created_at
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        coin['id'],
                        current_time,  # Use the same timestamp for all coins in this batch
                        coin.get('current_price'),
                        coin.get('market_cap'),
                        coin.get('total_volume'),
                        coin.get('price_change_percentage_24h'),  # Note: changed to get correct field
                        coin.get('market_cap_rank'),
                        coin.get('is_trending', False),
                        current_time)
                        
                        if self.cursor.rowcount > 0:
                            updated_count += 1
                            
                    except Exception as e:
                        self.log(f"Error updating daily data for {coin['id']}: {str(e)}")
                        continue
                        
                self.conn.commit()
                self.log(f"Updated daily price data for {updated_count} coins")
                
            else:
                self.log(f"Error fetching daily price data: Status {response.status_code}")
                
        except Exception as e:
            self.log(f"Error in update_daily_data: {str(e)}")

    def update_sentiment_data(self):
        """Update sentiment data for coins"""
        try:
            self.log("Fetching sentiment data from CoinGecko...")
            response = requests.get(f"{self.base_url}/coins/markets", params={
                'vs_currency': 'usd',
                'order': 'market_cap_desc',
                'per_page': 100,
                'page': 1,
                'sparkline': False
            })
            
            if response.status_code == 429:
                self.log("Rate limit hit - waiting 60 seconds...")
                self.countdown(60)
                response = requests.get(f"{self.base_url}/coins/markets", params={
                    'vs_currency': 'usd',
                    'order': 'market_cap_desc',
                    'per_page': 100,
                    'page': 1,
                    'sparkline': False
                })
                
            if response.status_code == 200:
                coins = response.json()
                updated_count = 0
                current_time = datetime.now()
                
                for coin in coins:
                    try:
                        response = requests.get(f"{self.base_url}/coins/{coin['id']}")
                        if response.status_code == 200:
                            data = response.json()
                            
                            self.cursor.execute("""
                                INSERT INTO coingecko_crypto_sentiment (
                                    crypto_id,
                                    metric_date,  -- This will now store both date and time
                                    sentiment_votes_up,
                                    sentiment_votes_down,
                                    public_interest_score,
                                    created_at
                                ) VALUES (?, ?, ?, ?, ?, ?)
                            """,
                            coin['id'],
                            current_time,
                            data.get('sentiment_votes_up_percentage'),
                            data.get('sentiment_votes_down_percentage'),
                            data.get('public_interest_score'),
                            current_time)
                            
                            if self.cursor.rowcount > 0:
                                updated_count += 1
                                
                    except Exception as e:
                        self.log(f"Error updating sentiment for {coin['id']}: {str(e)}")
                        continue
                        
                self.conn.commit()
                self.log(f"Updated sentiment data for {updated_count} coins")
                
            else:
                self.log(f"Error fetching coin list: Status {response.status_code}")
                
        except Exception as e:
            self.log(f"Error in update_sentiment_data: {str(e)}")

    def countdown(self, seconds):
        """Display countdown timer on same line"""
        start_time = time.time()
        for remaining in range(seconds, 0, -1):
            elapsed = int(time.time() - start_time)
            print(f"\rRate limit cooldown: {remaining}s remaining (elapsed: {elapsed}s)", end='', flush=True)
            time.sleep(1)
        print("\nResuming data collection...")  # New line only at the end

def main():
    parser = argparse.ArgumentParser(description='Update CoinGecko data')
    parser.add_argument('--master', action='store_true', help='Update master list')
    parser.add_argument('--daily', action='store_true', help='Update daily data')
    parser.add_argument('--sentiment', action='store_true', help='Update sentiment data')
    
    args = parser.parse_args()
    loader = CoinGeckoDataLoader(args)
    loader.run()

if __name__ == "__main__":
    main() 