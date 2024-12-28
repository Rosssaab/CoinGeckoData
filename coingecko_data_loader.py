import requests
import pyodbc
from datetime import datetime, timedelta
import time
from config import DB_CONNECTION_STRING, EMAIL_CONFIG
import argparse
import sys
from tqdm import tqdm
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import pandas as pd
from decimal import Decimal

class CoinGeckoLoader:
    def __init__(self):
        self.conn = pyodbc.connect(DB_CONNECTION_STRING)
        self.cursor = self.conn.cursor()
        self.base_url = "https://api.coingecko.com/api/v3"
        self.stats = {
            'master_updates': 0,
            'daily_updates': 0,
            'social_updates': 0,
            'errors': [],
            'api_calls': 0,
            'last_call_time': 0
        }
        self.rate_limit = {
            'minimum_interval': 1  # Assuming a default minimum interval
        }

    def log(self, message):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{timestamp}] {message}")

    def handle_rate_limit(self, response):
        self.stats['api_calls'] += 1
        current_time = time.time()
        elapsed = current_time - self.stats['last_call_time']
        
        if response.status_code == 429:
            retry_after = int(response.headers.get('Retry-After', 60))
            self.log(f"Rate limit hit. Waiting {retry_after} seconds...")
            
            # Add a progress indicator during the wait
            for i in range(retry_after):
                sys.stdout.write(f"\rWaiting... {retry_after - i} seconds remaining")
                sys.stdout.flush()
                time.sleep(1)
            print("\nResuming...")
            return True
        elif response.status_code == 403:
            self.log("Daily API limit exceeded. Please try again tomorrow.")
            sys.exit(1)
        
        # Normal rate limiting
        if elapsed < self.rate_limit['minimum_interval']:
            wait_time = self.rate_limit['minimum_interval'] - elapsed
            self.log(f"Rate limiting: Waiting {wait_time:.2f} seconds...")
            time.sleep(wait_time)
        
        self.stats['last_call_time'] = time.time()
        self.log(f"API call completed. Status: {response.status_code}")
        return False

    def get_coin_list(self):
        """Get list of all coins from CoinGecko"""
        try:
            self.log("Requesting coin list from CoinGecko...")
            response = requests.get(f"{self.base_url}/coins/list")
            
            if self.handle_rate_limit(response):
                self.log("Retrying coin list request...")
                return self.get_coin_list()
            
            if response.status_code != 200:
                self.log(f"Error: Status code {response.status_code}")
                self.log(f"Response: {response.text}")
                return []
            
            coins = response.json()
            self.log(f"Successfully retrieved {len(coins)} coins")
            return coins
            
        except Exception as e:
            self.log(f"Error getting coin list: {str(e)}")
            return []

    def update_master_data(self):
        """Update master list of coins with image IDs and filenames"""
        try:
            # First ensure the column exists
            self.cursor.execute("""
            IF NOT EXISTS (
                SELECT * FROM sys.columns 
                WHERE object_id = OBJECT_ID(N'coingecko_crypto_master') 
                AND name = 'image_filename'
            )
            BEGIN
                ALTER TABLE coingecko_crypto_master
                ADD image_filename NVARCHAR(255)
            END
            """)
            self.conn.commit()
            
            url = f"{self.base_url}/coins/markets"
            params = {
                'vs_currency': 'usd',
                'order': 'market_cap_desc',
                'per_page': 100,
                'page': 1,
                'sparkline': False
            }
            
            self.log("Fetching top 100 coins with image data...")
            response = requests.get(url, params=params)
            
            if response.status_code == 200:
                coins = response.json()
                for coin in coins:
                    try:
                        # Extract both image ID and filename from thumb URL
                        thumb_url = coin.get('image', '')
                        if '/images/' in thumb_url:
                            parts = thumb_url.split('/images/')[1].split('/')
                            image_id = parts[0]
                            filename = parts[2] if len(parts) > 2 else f"{coin['symbol'].lower()}.png"
                            
                            self.cursor.execute("""
                                UPDATE coingecko_crypto_master 
                                SET image_id = ?,
                                    image_filename = ?
                                WHERE id = ?
                            """, image_id, filename, coin['id'])
                            self.conn.commit()
                            self.log(f"Updated {coin['id']} with image_id: {image_id}, filename: {filename}")
                    
                    except Exception as e:
                        self.log(f"Error updating {coin['id']}: {str(e)}")
                        
                self.log(f"Updated image data for {len(coins)} coins")
                
        except Exception as e:
            self.log(f"Error in update_master_data: {str(e)}")

    def get_market_data(self, page=1):
        """Get current market data for all coins"""
        try:
            params = {
                'vs_currency': 'usd',
                'order': 'market_cap_desc',
                'per_page': 250,
                'page': page,
                'sparkline': False
            }
            
            response = requests.get(f"{self.base_url}/coins/markets", params=params)
            if self.handle_rate_limit(response):
                return self.get_market_data(page)
            
            return response.json()
        except Exception as e:
            self.log(f"Error getting market data page {page}: {str(e)}")
            return []

    def get_trending_coins(self):
        """Get list of trending coins"""
        try:
            response = requests.get(f"{self.base_url}/search/trending")
            if response.status_code == 200:
                data = response.json()
                return {coin['item']['id'] for coin in data['coins']}
        except Exception as e:
            self.log(f"Error getting trending coins: {str(e)}")
        return set()

    def update_daily_data(self):
        """Update daily price and market data for top 100 coins"""
        self.log("Updating daily market data for top 100 coins...")
        
        today = datetime.now().date()
        self.cursor.execute("""
            SELECT crypto_id 
            FROM coingecko_crypto_daily_data 
            WHERE price_date = ?
        """, today)
        existing_data = {row[0] for row in self.cursor.fetchall()}
        
        params = {
            'vs_currency': 'usd',
            'order': 'market_cap_desc',
            'per_page': 100,
            'page': 1,
            'sparkline': False
        }
        
        response = requests.get(f"{self.base_url}/coins/markets", params=params)
        if self.handle_rate_limit(response):
            return self.update_daily_data()
        
        coins = response.json()
        trending_coins = self.get_trending_coins()
        total_updated = 0
        
        for coin in coins:
            if coin['id'] not in existing_data:
                try:
                    # Explicitly convert all values to their proper SQL types
                    self.cursor.execute("""
                        INSERT INTO coingecko_crypto_daily_data 
                        (crypto_id, price_date, current_price, market_cap, total_volume, 
                         price_change_24h, market_cap_rank, is_trending)
                        VALUES 
                        (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        str(coin['id']),                    # varchar
                        today,                              # date
                        Decimal(str(coin['current_price'])) if coin['current_price'] is not None else None,  # decimal
                        Decimal(str(coin['market_cap'])) if coin['market_cap'] is not None else None,        # decimal
                        Decimal(str(coin['total_volume'])) if coin['total_volume'] is not None else None,    # decimal
                        Decimal(str(coin['price_change_percentage_24h'])) if coin.get('price_change_percentage_24h') is not None else None,  # decimal
                        int(coin['market_cap_rank']) if coin.get('market_cap_rank') is not None else None,   # int
                        1 if coin['id'] in trending_coins else 0                                             # bit
                    ))
                    total_updated += 1
                    
                except Exception as e:
                    self.log(f"Error updating daily data for {coin['id']}: {str(e)}")
                    self.log(f"Raw data: {coin}")
                    continue

        self.conn.commit()
        self.log(f"Updated daily data for {total_updated} coins")
        self.stats['daily_updates'] = total_updated

    def send_email_report(self):
        """Send email report of the data loading process"""
        try:
            msg = MIMEMultipart()
            msg['From'] = EMAIL_CONFIG['from']
            msg['To'] = EMAIL_CONFIG['to']
            msg['Subject'] = f"CoinGecko Data Load Report - {datetime.now().strftime('%Y-%m-%d')}"

            body = f"""
            Data Loading Report:
            
            Master Updates: {self.stats['master_updates']}
            Daily Updates: {self.stats['daily_updates']}
            Social Updates: {self.stats['social_updates']}
            
            Errors ({len(self.stats['errors'])}):
            {chr(10).join(self.stats['errors'][:10])}  # Show first 10 errors
            """

            msg.attach(MIMEText(body, 'plain'))

            with smtplib.SMTP(EMAIL_CONFIG['smtp_server'], EMAIL_CONFIG['smtp_port']) as server:
                server.starttls()
                server.login(EMAIL_CONFIG['username'], EMAIL_CONFIG['password'])
                server.send_message(msg)

        except Exception as e:
            self.log(f"Error sending email report: {str(e)}")

    def validate_data(self, data_type, data):
        """Validate data before insertion"""
        if data_type == 'market':
            required_fields = ['id', 'current_price', 'market_cap', 'total_volume']
            for field in required_fields:
                if field not in data or data[field] is None:
                    return False
            
            # Validate numeric fields
            try:
                float(data['current_price'])
                float(data['market_cap'])
                float(data['total_volume'])
                return True
            except (ValueError, TypeError):
                return False

        elif data_type == 'social':
            # Validate social data
            if not isinstance(data.get('community_data'), dict):
                return False
            return True

        return True

    def get_social_data(self, coin_id):
        """Get social data and sentiment for a specific coin"""
        try:
            # Basic social metrics
            url = f"{self.base_url}/coins/{coin_id}?localization=false&tickers=false&market_data=false&community_data=true&developer_data=true&sparkline=false"
            self.log(f"Fetching social data for {coin_id}")
            response = requests.get(url)
            
            if self.handle_rate_limit(response):
                return self.get_social_data(coin_id)
            
            if response.status_code != 200:
                self.log(f"Error getting social data for {coin_id}: Status {response.status_code}")
                return None
            
            data = response.json()
            
            # Get sentiment data
            sentiment_url = f"{self.base_url}/coins/{coin_id}/sentiment"
            sentiment_response = requests.get(sentiment_url)
            sentiment_data = sentiment_response.json() if sentiment_response.status_code == 200 else {}
            
            community_data = data.get('community_data', {})
            
            social_data = {
                'twitter_followers': community_data.get('twitter_followers', 0),
                'reddit_subscribers': community_data.get('reddit_subscribers', 0),
                'reddit_active_users': community_data.get('reddit_average_active_users_48h', 0),
                'telegram_users': community_data.get('telegram_channel_user_count', 0),
                'github_stars': data.get('developer_data', {}).get('stars', 0),
                'sentiment_votes_up_percentage': data.get('sentiment_votes_up_percentage', 0),
                'sentiment_votes_down_percentage': data.get('sentiment_votes_down_percentage', 0),
                'public_interest_score': data.get('public_interest_score', 0)
            }
            
            self.log(f"Retrieved social and sentiment data for {coin_id}")
            return social_data
            
        except Exception as e:
            self.log(f"Error fetching social/sentiment data for {coin_id}: {str(e)}")
            return None

    def create_sentiment_columns(self):
        """Add sentiment columns if they don't exist"""
        try:
            self.cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sys.columns 
                          WHERE object_id = OBJECT_ID('coingecko_crypto_social_data') 
                          AND name = 'sentiment_votes_up_percentage')
            BEGIN
                ALTER TABLE coingecko_crypto_social_data
                ADD sentiment_votes_up_percentage DECIMAL(5,2),
                    sentiment_votes_down_percentage DECIMAL(5,2),
                    public_interest_score DECIMAL(10,2)
            END
            """)
            self.conn.commit()
            self.log("Added sentiment columns to social data table")
        except Exception as e:
            self.log(f"Error adding sentiment columns: {str(e)}")

    def update_social_data(self):
        """Update social media data and sentiment for top 100 coins"""
        self.log("Updating social media and sentiment data for top 100 coins...")
        
        # First ensure we have the sentiment columns
        self.create_sentiment_columns()
        
        today = datetime.now().date()
        
        # Get top 100 coins by market cap
        params = {
            'vs_currency': 'usd',
            'order': 'market_cap_desc',
            'per_page': 100,
            'page': 1,
            'sparkline': False
        }
        
        response = requests.get(f"{self.base_url}/coins/markets", params=params)
        if self.handle_rate_limit(response):
            return self.update_social_data()
        
        top_coins = response.json()
        self.log(f"Retrieved {len(top_coins)} coins to check for social/sentiment data")
        
        total_updated = 0
        for coin in top_coins:
            coin_id = coin['id']
            try:
                social_data = self.get_social_data(coin_id)
                if social_data:
                    self.cursor.execute("""
                        INSERT INTO coingecko_crypto_social_data 
                        (crypto_id, metric_date, twitter_followers, reddit_subscribers, 
                         reddit_active_users_48h, telegram_users, github_stars,
                         sentiment_votes_up_percentage, sentiment_votes_down_percentage,
                         public_interest_score)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    coin_id,
                    today,
                    social_data['twitter_followers'],
                    social_data['reddit_subscribers'],
                    social_data['reddit_active_users'],
                    social_data['telegram_users'],
                    social_data['github_stars'],
                    social_data['sentiment_votes_up_percentage'],
                    social_data['sentiment_votes_down_percentage'],
                    social_data['public_interest_score']
                    )
                    total_updated += 1
                    self.log(f"Updated social/sentiment data for {coin_id}")
                    
                    # Commit after each successful insert
                    self.conn.commit()
                    
                    # Respect rate limits
                    time.sleep(self.rate_limit['minimum_interval'])
                    
            except Exception as e:
                self.log(f"Error updating social/sentiment data for {coin_id}: {str(e)}")
                continue
        
        self.log(f"Updated social and sentiment data for {total_updated} coins")
        self.stats['social_updates'] = total_updated

    def backfill_historical_data(self, days=30):
        """Backfill historical price data"""
        self.log(f"Backfilling {days} days of historical data...")
        
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=days)
        
        # Get top 100 coins
        self.cursor.execute("SELECT id FROM coingecko_crypto_master WHERE id IN (SELECT TOP 100 crypto_id FROM coingecko_crypto_daily_data WHERE price_date = CAST(GETDATE() AS DATE) ORDER BY market_cap DESC)")
        coins = [row[0] for row in self.cursor.fetchall()]
        
        with tqdm(total=len(coins), desc="Backfilling historical data") as pbar:
            for coin_id in coins:
                try:
                    # Get historical data
                    response = requests.get(
                        f"{self.base_url}/coins/{coin_id}/market_chart",
                        params={
                            'vs_currency': 'usd',
                            'from': int(datetime.combine(start_date, datetime.min.time()).timestamp()),
                            'to': int(datetime.combine(end_date, datetime.min.time()).timestamp()),
                            'interval': 'daily'
                        }
                    )
                    
                    if self.handle_rate_limit(response):
                        continue
                        
                    data = response.json()
                    
                    # Process daily data
                    prices = data.get('prices', [])
                    market_caps = data.get('market_caps', [])
                    volumes = data.get('total_volumes', [])
                    
                    for i in range(len(prices)):
                        try:
                            date = datetime.fromtimestamp(prices[i][0]/1000).date()
                            price = prices[i][1]
                            market_cap = market_caps[i][1]
                            volume = volumes[i][1]
                            
                            self.cursor.execute("""
                                IF NOT EXISTS (
                                    SELECT 1 FROM coingecko_crypto_daily_data 
                                    WHERE crypto_id = ? AND price_date = ?
                                )
                                INSERT INTO coingecko_crypto_daily_data 
                                (crypto_id, price_date, current_price, market_cap, total_volume)
                                VALUES (?, ?, ?, ?, ?)
                            """,
                            coin_id, date, coin_id, date, price, market_cap, volume)
                            
                        except Exception as e:
                            self.stats['errors'].append(f"Backfill error for {coin_id} on {date}: {str(e)}")
                            continue
                            
                    self.conn.commit()
                    
                except Exception as e:
                    self.stats['errors'].append(f"Backfill error for {coin_id}: {str(e)}")
                    continue
                
                finally:
                    pbar.update(1)
                    time.sleep(1.2)  # Respect rate limits

    def close(self):
        self.cursor.close()
        self.conn.close()

    def get_social_sentiment_data(self, coin_id):
        """Get detailed sentiment and market data for a coin"""
        try:
            url = f"{self.base_url}/coins/{coin_id}"
            self.log(f"Fetching sentiment data for {coin_id}")
            
            params = {
                'localization': 'false',
                'tickers': 'false',
                'market_data': 'true',
                'community_data': 'true',
                'developer_data': 'false',
                'sparkline': 'false'
            }
            
            response = requests.get(url, params=params)
            
            if self.handle_rate_limit(response):
                return self.get_social_sentiment_data(coin_id)
            
            if response.status_code != 200:
                self.log(f"Error getting data for {coin_id}: Status {response.status_code}")
                return None
            
            data = response.json()
            market_data = data.get('market_data', {})
            
            sentiment_data = {
                'price_change_24h': market_data.get('price_change_percentage_24h', 0),
                'price_change_7d': market_data.get('price_change_percentage_7d', 0),
                'price_change_14d': market_data.get('price_change_percentage_14d', 0),
                'price_change_30d': market_data.get('price_change_percentage_30d', 0),
                'market_cap_change_24h': market_data.get('market_cap_change_percentage_24h', 0),
                'volume_24h': market_data.get('total_volume', {}).get('usd', 0),
                'sentiment_votes_up': data.get('sentiment_votes_up_percentage', 0),
                'sentiment_votes_down': data.get('sentiment_votes_down_percentage', 0),
                'public_interest_score': data.get('public_interest_score', 0),
                'market_cap_rank': market_data.get('market_cap_rank', 0)
            }
            
            self.log(f"Retrieved sentiment data for {coin_id}")
            return sentiment_data
            
        except Exception as e:
            self.log(f"Error fetching sentiment data for {coin_id}: {str(e)}")
            return None

    def create_sentiment_table(self):
        """Create a new table for sentiment data"""
        try:
            self.cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[coingecko_crypto_sentiment]') AND type in (N'U'))
            BEGIN
                CREATE TABLE coingecko_crypto_sentiment (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    crypto_id VARCHAR(100),
                    metric_date DATE,
                    price_change_24h DECIMAL(10,2),
                    price_change_7d DECIMAL(10,2),
                    price_change_14d DECIMAL(10,2),
                    price_change_30d DECIMAL(10,2),
                    market_cap_change_24h DECIMAL(10,2),
                    volume_24h DECIMAL(18,2),
                    sentiment_votes_up DECIMAL(5,2),
                    sentiment_votes_down DECIMAL(5,2),
                    public_interest_score DECIMAL(10,2),
                    market_cap_rank INT,
                    created_at DATETIME DEFAULT GETDATE(),
                    CONSTRAINT FK_sentiment_master FOREIGN KEY (crypto_id) REFERENCES coingecko_crypto_master(id)
                )
            END
            """)
            self.conn.commit()
            self.log("Created sentiment table")
        except Exception as e:
            self.log(f"Error creating sentiment table: {str(e)}")

    def update_sentiment_data(self):
        """Update sentiment and market data for top 100 coins"""
        self.log("Updating sentiment data for top 100 coins...")
        
        today = datetime.now().date()
        
        # Get existing sentiment data for today to avoid duplicates
        self.cursor.execute("""
            SELECT crypto_id 
            FROM coingecko_crypto_sentiment 
            WHERE metric_date = ?
        """, today)
        existing_data = {row[0] for row in self.cursor.fetchall()}
        
        # Fixed query: Get top 100 coins by market cap rank
        self.cursor.execute("""
            SELECT TOP 100 m.id 
            FROM coingecko_crypto_master m
            INNER JOIN coingecko_crypto_daily_data d 
            ON m.id = d.crypto_id 
            WHERE d.price_date = ? 
            ORDER BY d.market_cap_rank
        """, today)
        
        coins = [row[0] for row in self.cursor.fetchall()]
        self.log(f"Found {len(coins)} coins to process")
        
        total_updated = 0
        for coin_id in coins:
            if coin_id not in existing_data:
                try:
                    sentiment_data = self.get_social_sentiment_data(coin_id)
                    if sentiment_data:
                        self.cursor.execute("""
                            INSERT INTO coingecko_crypto_sentiment
                            (crypto_id, metric_date, price_change_24h, price_change_7d, 
                             price_change_14d, price_change_30d, market_cap_change_24h,
                             volume_24h, sentiment_votes_up, sentiment_votes_down,
                             public_interest_score, market_cap_rank)
                            VALUES
                            (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            coin_id, 
                            today,
                            sentiment_data['price_change_24h'],
                            sentiment_data['price_change_7d'],
                            sentiment_data['price_change_14d'],
                            sentiment_data['price_change_30d'],
                            sentiment_data['market_cap_change_24h'],
                            sentiment_data['volume_24h'],
                            sentiment_data['sentiment_votes_up'],
                            sentiment_data['sentiment_votes_down'],
                            sentiment_data['public_interest_score'],
                            sentiment_data['market_cap_rank']
                        ))
                        
                        self.conn.commit()
                        total_updated += 1
                        self.log(f"Updated sentiment data for {coin_id}")
                        
                        # Respect rate limits
                        time.sleep(2)  # 2-second delay between requests
                        
                except Exception as e:
                    self.log(f"Error updating sentiment for {coin_id}: {str(e)}")
                    self.log(f"Attempted data: {sentiment_data if 'sentiment_data' in locals() else 'No data'}")
                    continue
            
        self.log(f"Updated sentiment data for {total_updated} coins")
        self.stats['sentiment_updates'] = total_updated

    def create_tables(self):
        """Create or update tables"""
        try:
            # Add image_id column if it doesn't exist
            self.cursor.execute("""
            IF NOT EXISTS (
                SELECT * FROM sys.columns 
                WHERE object_id = OBJECT_ID(N'coingecko_crypto_master') 
                AND name = 'image_id'
            )
            BEGIN
                ALTER TABLE coingecko_crypto_master
                ADD image_id INT NULL
            END
            """)
            self.conn.commit()
            self.log("Added image_id column if it didn't exist")
            
        except Exception as e:
            self.log(f"Error updating table structure: {str(e)}")

def main():
    parser = argparse.ArgumentParser(description='CoinGecko Data Loader')
    parser.add_argument('--master', action='store_true', help='Update master coin list')
    parser.add_argument('--daily', action='store_true', help='Update daily market data')
    parser.add_argument('--social', action='store_true', help='Update social media data')
    parser.add_argument('--sentiment', action='store_true', help='Update sentiment data')
    parser.add_argument('--backfill', type=int, help='Backfill historical data for N days')
    parser.add_argument('--all', action='store_true', help='Update all data')
    parser.add_argument('--email-report', action='store_true', help='Send email report after completion')
    parser.add_argument('--validate', action='store_true', help='Validate data before insertion')

    args = parser.parse_args()
    loader = CoinGeckoLoader()

    try:
        if args.master:
            loader.update_master_data()
        if args.daily:
            loader.update_daily_data()
        if args.sentiment:
            loader.update_sentiment_data()
        if args.backfill:
            loader.backfill_historical_data(args.backfill)
    finally:
        loader.close()

if __name__ == "__main__":
    main() 