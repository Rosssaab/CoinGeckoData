from config import ALPHA_VANTAGE_API_KEY, THE_TIE_API_KEY, DB_CONNECTION_STRING
from flask import Flask, render_template, jsonify
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd
from datetime import datetime, timezone
import os
import time
from functools import lru_cache
import pyodbc
import json

# Setup retry strategy
retry_strategy = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[500, 502, 503, 504]
)
adapter = HTTPAdapter(max_retries=retry_strategy)

app = Flask(__name__)

CACHE_DURATION = 300  # 5 minutes cache

# Add this to store coin data in memory
coin_cache = {}

@lru_cache(maxsize=100)
def get_cached_coin_details(coin_id, timestamp):
    """Cached version of coin details"""
    return get_coin_details(coin_id)

def get_alpha_vantage_data():
    """Get top cryptocurrency data from Alpha Vantage"""
    base_url = "https://www.alphavantage.co/query"
    params = {
        'function': 'CRYPTO_RATING',
        'symbol': 'BTC',  # This endpoint will return multiple cryptos regardless
        'apikey': ALPHA_VANTAGE_API_KEY
    }
    
    try:
        session = requests.Session()
        session.mount("https://", adapter)
        response = session.get(base_url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        # Check for error messages
        if "Error Message" in data:
            return {'error': f"Alpha Vantage API Error: {data['Error Message']}"}
            
        if 'Crypto Rating (FCAS)' not in data:
            return {'error': "Invalid response format from Alpha Vantage"}
            
        # Process all cryptocurrencies
        crypto_list = []
        for crypto in data['Crypto Rating (FCAS)']:
            try:
                crypto_list.append({
                    'name': crypto['name'],
                    'symbol': crypto['symbol'],
                    'fcas_rating': crypto['fcas_rating'],
                    'fcas_score': float(crypto['fcas_score']),
                    'market_maturity_score': float(crypto['market_maturity_score']),
                    'utility_score': float(crypto['utility_score']),
                    'last_refreshed': crypto['last_refreshed'],
                    'timezone': crypto['timezone']
                })
            except (KeyError, ValueError) as e:
                continue  # Skip cryptos with missing/invalid data
                
        return {
            'data': crypto_list,
            'error': None
        }
    except requests.exceptions.RequestException as e:
        return {'error': f"Network error: {str(e)}"}
    except Exception as e:
        return {'error': f"Unexpected error: {str(e)}"}

def get_lunarcrush_data():
    """Get Bitcoin data from LunarCrush (requires paid API)"""
    return {'error': "LunarCrush API requires a paid subscription"}

def get_the_tie_data():
    """Get Bitcoin data from The Tie"""
    base_url = "https://api.thetie.io/v1/cryptocurrency/quotes"
    headers = {
        'Authorization': f'Bearer {THE_TIE_API_KEY}'
    }
    params = {
        'tickers': 'BTC',
        'include_price': 'true',
        'include_volume': 'true'
    }
    
    try:
        session = requests.Session()
        session.mount("https://", adapter)
        response = session.get(base_url, headers=headers, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        if 'data' not in data or not data['data']:
            return {'error': "No data available from The Tie"}
            
        btc_data = data['data'][0]
        
        return {
            'name': 'Bitcoin',
            'symbol': 'BTC',
            'price': float(btc_data['price']),
            'volume_24h': float(btc_data['volume_24h']),
            'market_cap': float(btc_data['market_cap']),
            'last_updated': btc_data['last_updated'],
            'error': None
        }
    except requests.exceptions.RequestException as e:
        return {'error': f"Network error: {str(e)}"}
    except (KeyError, ValueError) as e:
        return {'error': f"Data parsing error: {str(e)}"}
    except Exception as e:
        return {'error': f"Unexpected error: {str(e)}"}

def calculate_24h_change(data, latest_date):
    """Calculate 24h price change percentage"""
    time_series = data['Time Series (Digital Currency Daily)']
    dates = list(time_series.keys())
    if len(dates) >= 2:
        current_price = float(time_series[dates[0]]['4a. close (USD)'])
        prev_price = float(time_series[dates[1]]['4a. close (USD)'])
        return ((current_price - prev_price) / prev_price) * 100
    return 0.0

def get_trending_coins():
    """Get trending coins from CoinGecko"""
    try:
        response = requests.get('https://api.coingecko.com/api/v3/search/trending')
        response.raise_for_status()
        data = response.json()
        return {coin['item']['id'] for coin in data['coins']}
    except:
        return set()

def get_db_connection():
    """Create database connection"""
    return pyodbc.connect(DB_CONNECTION_STRING)

def create_crypto_tables():
    """Create tables if they don't exist"""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        # Create main crypto data table
        cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='crypto_data' AND xtype='U')
            CREATE TABLE crypto_data (
                id VARCHAR(100) PRIMARY KEY,
                name NVARCHAR(255),
                symbol VARCHAR(20),
                current_price DECIMAL(18, 8),
                market_cap DECIMAL(18, 2),
                market_cap_rank INT,
                price_change_24h DECIMAL(8, 2),
                volume_24h DECIMAL(18, 2),
                image_url NVARCHAR(500),
                is_trending BIT,
                last_updated DATETIME,
                last_fetched DATETIME
            )
        """)
        
        # Create table for detailed coin data
        cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='crypto_details' AND xtype='U')
            CREATE TABLE crypto_details (
                id VARCHAR(100) PRIMARY KEY,
                description NVARCHAR(MAX),
                links_json NVARCHAR(MAX),
                community_data_json NVARCHAR(MAX),
                developer_data_json NVARCHAR(MAX),
                last_updated DATETIME
            )
        """)
        
        conn.commit()

def update_crypto_data(crypto_list):
    """Update crypto data in database"""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        for crypto in crypto_list:
            cursor.execute("""
                MERGE crypto_data AS target
                USING (SELECT ? as id) AS source
                ON target.id = source.id
                WHEN MATCHED THEN
                    UPDATE SET 
                        name = ?,
                        symbol = ?,
                        current_price = ?,
                        market_cap = ?,
                        market_cap_rank = ?,
                        price_change_24h = ?,
                        volume_24h = ?,
                        image_url = ?,
                        is_trending = ?,
                        last_updated = ?,
                        last_fetched = GETDATE()
                WHEN NOT MATCHED THEN
                    INSERT (id, name, symbol, current_price, market_cap, market_cap_rank, 
                            price_change_24h, volume_24h, image_url, is_trending, last_updated, last_fetched)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE());
                """,
                crypto['id'], crypto['name'], crypto['symbol'], crypto['current_price'],
                crypto['market_cap'], crypto['market_cap_rank'], crypto['price_change_24h'],
                crypto['volume_24h'], crypto['image'], crypto['is_trending'], crypto['last_updated'],
                crypto['id'], crypto['name'], crypto['symbol'], crypto['current_price'],
                crypto['market_cap'], crypto['market_cap_rank'], crypto['price_change_24h'],
                crypto['volume_24h'], crypto['image'], crypto['is_trending'], crypto['last_updated']
            )
        
        conn.commit()

def get_crypto_from_db():
    """Get crypto data from database"""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, name, symbol, current_price, market_cap, market_cap_rank,
                   price_change_24h, volume_24h, image_url, is_trending, last_updated
            FROM crypto_data
            ORDER BY market_cap_rank
        """)
        
        columns = [column[0] for column in cursor.description]
        results = []
        
        for row in cursor.fetchall():
            results.append(dict(zip(columns, row)))
        
        return {'data': results, 'error': None}

def get_top_100_crypto():
    """Get top 100 cryptocurrencies from our database"""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT 
                    m.id,
                    m.name,
                    m.symbol,
                    m.image_id,
                    m.image_filename,
                    d.current_price,
                    d.market_cap,
                    d.market_cap_rank,
                    d.price_change_24h,
                    d.total_volume as volume_24h,
                    s.sentiment_votes_up,
                    s.sentiment_votes_down,
                    s.public_interest_score,
                    d.is_trending,
                    d.price_date as last_updated
                FROM coingecko_crypto_master m
                JOIN coingecko_crypto_daily_data d ON m.id = d.crypto_id
                LEFT JOIN coingecko_crypto_sentiment s ON m.id = s.crypto_id 
                    AND s.metric_date = d.price_date
                WHERE d.price_date = (
                    SELECT MAX(price_date) 
                    FROM coingecko_crypto_daily_data
                )
                ORDER BY d.market_cap_rank
                OFFSET 0 ROWS FETCH NEXT 100 ROWS ONLY
            """)
            
            columns = [column[0] for column in cursor.description]
            results = []
            
            for row in cursor.fetchall():
                crypto = dict(zip(columns, row))
                # Use both image_id and image_filename
                crypto['image'] = f"https://assets.coingecko.com/coins/images/{crypto['image_id']}/thumb/{crypto['image_filename']}"
                results.append(crypto)
            
            return {'data': results, 'error': None}
            
    except Exception as e:
        print(f"Database error: {str(e)}")
        return {'error': str(e)}

def get_coin_social_data(coin_id):
    """Get social media data for a specific coin"""
    try:
        # Add delay to respect rate limits
        time.sleep(1.2)  # Wait 1.2 seconds between requests
        
        print(f"Requesting social data for {coin_id}")
        response = requests.get(
            f'https://api.coingecko.com/api/v3/coins/{coin_id}',
            params={
                'localization': 'false',
                'tickers': 'false',
                'market_data': 'false',
                'community_data': 'true',
                'developer_data': 'false',
                'sparkline': 'false'
            },
            timeout=10
        )
        response.raise_for_status()
        
        if response.status_code == 429:  # Too Many Requests
            print("Rate limit hit, waiting...")
            time.sleep(60)  # Wait 60 seconds if rate limited
            return get_coin_social_data(coin_id)  # Retry
            
        data = response.json()
        
        community_data = data.get('community_data', {})
        
        # Get all social metrics
        twitter = int(community_data.get('twitter_followers', 0))
        reddit_subs = int(community_data.get('reddit_subscribers', 0))
        reddit_active = int(community_data.get('reddit_accounts_active_48h', 0))
        telegram = int(community_data.get('telegram_channel_user_count', 0))
        
        # Calculate weighted social score
        social_score = (
            twitter * 1 +
            reddit_subs * 1.2 +
            reddit_active * 2 +
            telegram * 0.8
        )
        
        return {
            'twitter_followers': twitter,
            'reddit_subscribers': reddit_subs,
            'reddit_active_48h': reddit_active,
            'telegram_channel_user_count': telegram,
            'social_score': social_score
        }
    except Exception as e:
        print(f"Error getting social data for {coin_id}: {str(e)}")
        return {
            'twitter_followers': 0,
            'reddit_subscribers': 0,
            'reddit_active_48h': 0,
            'telegram_channel_user_count': 0,
            'social_score': 0
        }

def get_social_activity(coin_id):
    """Get 7-day social media activity for a coin"""
    try:
        # Get Twitter data
        twitter_url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/twitter"
        twitter_response = requests.get(twitter_url)
        twitter_data = twitter_response.json() if twitter_response.status_code == 200 else []

        # Get Reddit data
        reddit_url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/reddit"
        reddit_response = requests.get(reddit_url)
        reddit_data = reddit_response.json() if reddit_response.status_code == 200 else {}

        # Process the last 7 days of data
        from datetime import datetime, timedelta
        today = datetime.now()
        dates = [(today - timedelta(days=i)).strftime('%Y-%m-%d') for i in range(7)]
        
        social_activity = {
            'dates': dates,
            'twitter_posts': [0] * 7,  # Initialize with zeros
            'reddit_posts': [0] * 7,
            'reddit_comments': [0] * 7
        }

        # Process Twitter data
        for tweet in twitter_data:
            tweet_date = tweet.get('created_at', '').split('T')[0]
            if tweet_date in dates:
                idx = dates.index(tweet_date)
                social_activity['twitter_posts'][idx] += 1

        # Process Reddit data
        for date in dates:
            idx = dates.index(date)
            social_activity['reddit_posts'][idx] = reddit_data.get(f'posts_{date}', 0)
            social_activity['reddit_comments'][idx] = reddit_data.get(f'comments_{date}', 0)

        return social_activity
    except Exception as e:
        print(f"Error getting social activity: {str(e)}")
        return None

@app.route('/get_social_data/<coin_id>')
def get_social_data(coin_id):
    """API endpoint to get social data for a specific coin"""
    try:
        print(f"Fetching social data for {coin_id}...")
        start_time = time.time()
        
        social_data = get_coin_social_data(coin_id)
        is_trending = coin_id in get_trending_coins()
        
        print(f"Social data fetched in {time.time() - start_time:.2f} seconds")
        
        return jsonify({
            'social_data': social_data,
            'is_trending': is_trending
        })
    except Exception as e:
        print(f"Error fetching social data: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/get_coin_details/<coin_id>')
def cached_coin_details(coin_id):
    # Use current timestamp rounded to CACHE_DURATION to ensure cache hits
    timestamp = int(time.time() / CACHE_DURATION) * CACHE_DURATION
    return get_cached_coin_details(coin_id, timestamp)

@app.route('/')
def index():
    crypto_data = get_top_100_crypto()
    return render_template('index.html', crypto_data=crypto_data)

def preload_top_coins():
    """Preload data for top 10 coins"""
    print("Starting preload of top coins...")
    coins = get_top_100_crypto()
    if not coins.get('error'):
        for coin in coins['data'][:10]:  # Only top 10
            coin_id = coin['id']
            try:
                print(f"Preloading data for {coin_id}...")
                time.sleep(6)  # Respect rate limits
                details = get_coin_details(coin_id)
                if not details.get('error'):
                    coin_cache[coin_id] = {
                        'data': details,
                        'timestamp': time.time()
                    }
                    print(f"Successfully cached {coin_id}")
                else:
                    print(f"Error getting details for {coin_id}: {details.get('error')}")
            except Exception as e:
                print(f"Error preloading {coin_id}: {str(e)}")
    else:
        print(f"Error getting top coins: {coins.get('error')}")

def get_coin_details(coin_id):
    """Get detailed coin information from our database"""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # Get latest sentiment and price data
            cursor.execute("""
                SELECT 
                    m.name,
                    m.symbol,
                    d.current_price,
                    d.market_cap,
                    d.market_cap_rank,
                    s.price_change_24h,
                    s.price_change_7d,
                    s.price_change_14d,
                    s.price_change_30d,
                    s.sentiment_votes_up,
                    s.sentiment_votes_down,
                    s.public_interest_score,
                    s.volume_24h,
                    d.is_trending
                FROM coingecko_crypto_master m
                JOIN coingecko_crypto_daily_data d ON m.id = d.crypto_id
                LEFT JOIN coingecko_crypto_sentiment s ON m.id = s.crypto_id 
                    AND s.metric_date = d.price_date
                WHERE m.id = ?
                    AND d.price_date = (
                        SELECT MAX(price_date) 
                        FROM coingecko_crypto_daily_data
                    )
            """, coin_id)
            
            result = cursor.fetchone()
            if not result:
                return {'error': 'Coin not found'}
                
            columns = [column[0] for column in cursor.description]
            data = dict(zip(columns, result))
            
            return {
                'name': data['name'],
                'symbol': data['symbol'],
                'market_data': {
                    'current_price': data['current_price'],
                    'market_cap': data['market_cap'],
                    'market_cap_rank': data['market_cap_rank'],
                    'price_change_24h': data['price_change_24h'],
                    'price_change_7d': data['price_change_7d'],
                    'price_change_14d': data['price_change_14d'],
                    'price_change_30d': data['price_change_30d'],
                    'volume_24h': data['volume_24h']
                },
                'sentiment_data': {
                    'votes_up': data['sentiment_votes_up'],
                    'votes_down': data['sentiment_votes_down'],
                    'public_interest_score': data['public_interest_score']
                },
                'is_trending': data['is_trending']
            }
            
    except Exception as e:
        print(f"Database error: {str(e)}")
        return {'error': str(e)}

if __name__ == '__main__':
    create_crypto_tables()  # Create tables if they don't exist
    app.run(debug=True) 