import requests
import argparse
import pyodbc
from datetime import datetime
import time
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from tweepy import Client as TwitterClient
from config import (
    # Database configs
    DB_CONNECTION_STRING,
    DB_SERVER,
    DB_NAME,
    DB_USER,
    DB_PASSWORD,
    
    # API configs
    TWITTER_BEARER_TOKEN,
    NEWS_API_KEY,
    NEWS_API_URL,
    CRYPTOPANIC_API_KEY,
    CRYPTOPANIC_BASE_URL
)
from sqlalchemy import create_engine, text

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
        
        # Add SQLAlchemy engine initialization
        self.connection_str = f'mssql+pyodbc://{DB_USER}:{DB_PASSWORD}@{DB_SERVER}/{DB_NAME}?driver=SQL+Server+Native+Client+11.0'
        self.engine = create_engine(self.connection_str)
        
        # Add new API initializations
        self.twitter = TwitterClient(bearer_token=TWITTER_BEARER_TOKEN)
        self.analyzer = SentimentIntensityAnalyzer()
        self.reddit_headers = {
            'User-Agent': 'CryptoSentimentBot/1.0'
        }
        
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
            # First get trending coins
            trending_response = requests.get(f"{self.base_url}/search/trending")
            trending_coins = set()
            if trending_response.status_code == 200:
                trending_data = trending_response.json()
                trending_coins = {coin['item']['id'] for coin in trending_data.get('coins', [])}

            # Then get regular market data
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
                        # Set is_trending based on whether coin is in trending list
                        is_trending = 1 if coin['id'] in trending_coins else 0
                        
                        self.cursor.execute("""
                            INSERT INTO coingecko_crypto_daily_data (
                                crypto_id, 
                                price_date,
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
                        current_time,
                        coin.get('current_price'),
                        coin.get('market_cap'),
                        coin.get('total_volume'),
                        coin.get('price_change_percentage_24h'),
                        coin.get('market_cap_rank'),
                        is_trending,  # Use our trending flag
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
        """Enhanced sentiment collection including social media"""
        try:
            self.log("Collecting enhanced sentiment data...")
            coins = self.get_top_coins()
            self.log(f"Processing {len(coins)} coins...")
            
            for coin in coins:
                try:
                    self.log(f"\nProcessing {coin['id']}...")
                    
                    # Collect from multiple sources
                    twitter_sentiment = self.collect_twitter_mentions(coin)
                    self.log(f"Twitter mentions: {len(twitter_sentiment)}")
                    
                    reddit_sentiment = self.collect_reddit_mentions(coin)
                    self.log(f"Reddit mentions: {len(reddit_sentiment)}")
                    
                    news_sentiment = self.collect_news_mentions(coin)
                    self.log(f"News mentions: {len(news_sentiment)}")
                    
                    # Aggregate sentiment scores
                    scores = self.aggregate_sentiment_scores(
                        twitter_sentiment,
                        reddit_sentiment,
                        news_sentiment
                    )
                    
                    self.log(f"Aggregated scores for {coin['id']}:")
                    self.log(f"- Positive: {scores['positive']:.2f}%")
                    self.log(f"- Negative: {scores['negative']:.2f}%")
                    self.log(f"- Interest: {scores['interest_score']}")
                    
                    # Fixed SQL query with correct number of ? placeholders
                    query = """
                    INSERT INTO coingecko_crypto_sentiment (
                        crypto_id,
                        metric_date,
                        sentiment_votes_up,
                        sentiment_votes_down,
                        public_interest_score,
                        twitter_sentiment,
                        reddit_sentiment,
                        news_sentiment,
                        created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """
                    
                    current_time = datetime.now()
                    
                    # Make sure we have exactly 9 parameters to match the 9 ? placeholders
                    params = [
                        coin['id'],                    # crypto_id
                        current_time,                  # metric_date
                        scores['positive'],            # sentiment_votes_up
                        scores['negative'],            # sentiment_votes_down
                        scores['interest_score'],      # public_interest_score
                        scores['twitter_score'],       # twitter_sentiment
                        scores['reddit_score'],        # reddit_sentiment
                        scores['news_score'],          # news_sentiment
                        current_time                   # created_at
                    ]
                    
                    self.cursor.execute(query, params)
                    self.conn.commit()
                    self.log(f"Saved sentiment data for {coin['id']}")
                    
                    time.sleep(1)  # Rate limiting
                    
                except Exception as e:
                    self.log(f"Error processing {coin['id']}: {str(e)}")
                    import traceback
                    self.log(f"Detailed error: {traceback.format_exc()}")
                    continue
                
        except Exception as e:
            self.log(f"Error in sentiment update: {str(e)}")

    def collect_twitter_mentions(self, coin):
        """Collect Twitter mentions with rate limit handling"""
        try:
            self.log(f"Collecting Twitter mentions for {coin['id']}...")
            
            # Check if we've hit rate limit
            if hasattr(self, 'twitter_reset_time'):
                now = datetime.now()
                if now < self.twitter_reset_time:
                    wait_time = (self.twitter_reset_time - now).seconds
                    self.log(f"Twitter rate limit hit. Waiting {wait_time} seconds...")
                    time.sleep(wait_time)
            
            query = f"#{coin['symbol']} OR #{coin['name']} crypto"
            response = self.twitter.search_recent_tweets(query=query, max_results=10)
            
            if hasattr(response, 'errors') and response.errors:
                if any(error['code'] == 88 for error in response.errors):  # Rate limit error
                    reset_time = datetime.fromtimestamp(int(response.meta['reset']))
                    self.twitter_reset_time = reset_time
                    self.log(f"Twitter rate limit hit. Reset at {reset_time}")
                    return []
                    
            tweets = response.data or []
            self.log(f"Found {len(tweets)} tweets for {coin['id']}")
            
            sentiments = []
            for tweet in tweets:
                sentiment = self.analyzer.polarity_scores(tweet.text)['compound']
                sentiments.append(sentiment)
                
            return sentiments
            
        except Exception as e:
            self.log(f"Twitter error for {coin['id']}: {str(e)}")
            return []

    def collect_reddit_mentions(self, coin):
        """Collect Reddit sentiment - copied from CollectChat"""
        mentions = []
        subreddits = ['cryptocurrency', 'CryptoMarkets']
        
        for subreddit in subreddits:
            try:
                url = f"https://www.reddit.com/r/{subreddit}/search.json"
                params = {
                    'q': f"{coin['symbol']} OR {coin['name']}",
                    't': 'day',
                    'limit': 100
                }
                response = requests.get(url, headers=self.reddit_headers, params=params)
                
                if response.status_code == 200:
                    posts = response.json().get('data', {}).get('children', [])
                    for post in posts:
                        content = f"{post['data']['title']} {post['data'].get('selftext', '')}"
                        sentiment_score = self.analyzer.polarity_scores(content)['compound']
                        mentions.append(sentiment_score)
                        
            except Exception as e:
                self.log(f"Reddit error: {str(e)}")
                
        return mentions

    def aggregate_sentiment_scores(self, twitter_scores, reddit_scores, news_scores):
        """Aggregate sentiment scores from different sources"""
        def calculate_sentiment_ratio(scores):
            if not scores:
                return 0, 0
            positive = len([s for s in scores if s > 0])
            negative = len([s for s in scores if s < 0])
            total = len(scores)
            return (positive/total * 100) if total > 0 else 0, (negative/total * 100) if total > 0 else 0

        twitter_pos, twitter_neg = calculate_sentiment_ratio(twitter_scores)
        reddit_pos, reddit_neg = calculate_sentiment_ratio(reddit_scores)
        news_pos, news_neg = calculate_sentiment_ratio(news_scores)

        return {
            'positive': (twitter_pos + reddit_pos + news_pos) / 3,
            'negative': (twitter_neg + reddit_neg + news_neg) / 3,
            'interest_score': len(twitter_scores) + len(reddit_scores) + len(news_scores),
            'twitter_score': sum(twitter_scores) / len(twitter_scores) if twitter_scores else 0,
            'reddit_score': sum(reddit_scores) / len(reddit_scores) if reddit_scores else 0,
            'news_score': sum(news_scores) / len(news_scores) if news_scores else 0
        }

    def countdown(self, seconds):
        """Display countdown timer on same line"""
        start_time = time.time()
        for remaining in range(seconds, 0, -1):
            elapsed = int(time.time() - start_time)
            print(f"\rRate limit cooldown: {remaining}s remaining (elapsed: {elapsed}s)", end='', flush=True)
            time.sleep(1)
        print("\nResuming data collection...")  # New line only at the end

    def get_top_coins(self, limit=None):  # Remove limit parameter since we want all coins
        """Get all active coins from daily data table"""
        try:
            query = """
            SELECT DISTINCT 
                m.id,
                m.symbol,
                m.name,
                m.market_cap_rank
            FROM coingecko_crypto_master m
            INNER JOIN coingecko_crypto_daily_data d 
                ON m.id = d.crypto_id
            WHERE d.price_date >= DATEADD(day, -7, GETDATE())  -- Only get active coins from last 7 days
            ORDER BY m.market_cap_rank
            """
            
            with self.engine.connect() as connection:
                result = connection.execute(text(query))
                coins = [
                    {
                        'id': row[0],
                        'symbol': row[1],
                        'name': row[2],
                        'market_cap_rank': row[3]
                    }
                    for row in result
                ]
                self.log(f"Found {len(coins)} active coins")
                return coins
        except Exception as e:
            self.log(f"Error getting coins: {str(e)}")
            return []

    def collect_news_mentions(self, coin):
        """Collect news mentions and sentiment for a coin"""
        try:
            self.log(f"Collecting news for {coin['id']}...")
            
            # Query NewsAPI
            query = f"{coin['name']} OR {coin['symbol']} crypto"
            url = f"{NEWS_API_URL}"
            params = {
                'q': query,
                'apiKey': NEWS_API_KEY,
                'language': 'en',
                'sortBy': 'publishedAt',
                'pageSize': 10
            }
            
            response = requests.get(url, params=params)
            if response.status_code != 200:
                self.log(f"NewsAPI error for {coin['id']}: {response.status_code}")
                return []
            
            articles = response.json().get('articles', [])
            self.log(f"Found {len(articles)} news articles for {coin['id']}")
            
            # Analyze sentiment
            sentiments = []
            for article in articles:
                content = f"{article['title']} {article['description'] or ''}"
                sentiment = self.analyzer.polarity_scores(content)['compound']
                sentiments.append(sentiment)
            
            return sentiments
            
        except Exception as e:
            self.log(f"Error collecting news for {coin['id']}: {str(e)}")
            return []

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