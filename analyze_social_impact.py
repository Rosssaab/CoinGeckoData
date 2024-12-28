import pandas as pd
from sqlalchemy import create_engine
from config import DB_SERVER, DB_NAME, DB_USER, DB_PASSWORD

def analyze_social_impact():
    # Create SQLAlchemy engine
    connection_str = f'mssql+pyodbc://{DB_USER}:{DB_PASSWORD}@{DB_SERVER}/{DB_NAME}?driver=SQL+Server+Native+Client+11.0'
    engine = create_engine(connection_str)
    
    query = """
    WITH PriceChanges AS (
        SELECT 
            d1.crypto_id,
            d1.price_date,
            d1.current_price,
            d1.price_change_24h,
            s.twitter_followers,
            s.reddit_subscribers,
            s.reddit_active_users_48h
        FROM coingecko_crypto_daily_data d1
        JOIN coingecko_crypto_social_data s 
            ON d1.crypto_id = s.crypto_id 
            AND d1.price_date = s.metric_date
        WHERE d1.price_date >= DATEADD(day, -30, GETDATE())
    )
    SELECT 
        crypto_id,
        AVG(price_change_24h) as avg_price_change,
        AVG(twitter_followers) as avg_twitter_followers,
        AVG(reddit_subscribers) as avg_reddit_subscribers,
        AVG(reddit_active_users_48h) as avg_reddit_active_users
    FROM PriceChanges
    GROUP BY crypto_id
    ORDER BY avg_price_change DESC
    """
    
    try:
        # Read data using SQLAlchemy engine
        df = pd.read_sql(query, engine)
        
        print("\nTop 10 coins by price change and their social metrics:")
        print(df.head(10))
        
        if not df.empty:
            # Calculate correlations
            correlations = df.corr()['avg_price_change'].sort_values(ascending=False)
            print("\nCorrelations with price change:")
            print(correlations)
        else:
            print("\nNo data found for analysis")
            
    except Exception as e:
        print(f"Error during analysis: {str(e)}")
    finally:
        engine.dispose()

if __name__ == "__main__":
    analyze_social_impact() 