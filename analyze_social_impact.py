import pandas as pd
from sqlalchemy import create_engine
from config import DB_SERVER, DB_NAME, DB_USER, DB_PASSWORD
import os
from datetime import datetime

def analyze_social_impact():
    # Create logs directory if it doesn't exist
    logs_dir = 'logs'
    if not os.path.exists(logs_dir):
        os.makedirs(logs_dir)
    
    # Create log filename with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = os.path.join(logs_dir, f'sentiment_analysis_results_{timestamp}.txt')
    
    # Create SQLAlchemy engine
    connection_str = f'mssql+pyodbc://{DB_USER}:{DB_PASSWORD}@{DB_SERVER}/{DB_NAME}?driver=SQL+Server+Native+Client+11.0'
    engine = create_engine(connection_str)
    
    query = """
    WITH SentimentData AS (
        SELECT 
            d1.crypto_id,
            d1.price_date,
            d1.current_price,
            d1.price_change_24h,
            COALESCE(s.sentiment_votes_up, 0) as sentiment_up,
            COALESCE(s.sentiment_votes_down, 0) as sentiment_down,
            COALESCE(s.public_interest_score, 0) as interest_score,
            COALESCE(s.twitter_sentiment, 0) as twitter_sentiment,
            COALESCE(s.reddit_sentiment, 0) as reddit_sentiment,
            COALESCE(s.news_sentiment, 0) as news_sentiment
        FROM coingecko_crypto_daily_data d1
        LEFT JOIN coingecko_crypto_sentiment s 
            ON d1.crypto_id = s.crypto_id 
            AND CAST(d1.price_date AS DATE) = CAST(s.metric_date AS DATE)
        WHERE d1.price_date >= DATEADD(day, -30, GETDATE())
    )
    SELECT 
        m.name,
        sd.crypto_id,
        AVG(sd.price_change_24h) as avg_price_change,
        AVG(sd.sentiment_up) as avg_sentiment_up,
        AVG(sd.sentiment_down) as avg_sentiment_down,
        AVG(sd.interest_score) as avg_interest_score,
        AVG(sd.twitter_sentiment) as avg_twitter_sentiment,
        AVG(sd.reddit_sentiment) as avg_reddit_sentiment,
        AVG(sd.news_sentiment) as avg_news_sentiment,
        COUNT(*) as data_points
    FROM SentimentData sd
    JOIN coingecko_crypto_master m ON sd.crypto_id = m.id
    GROUP BY sd.crypto_id, m.name
    HAVING COUNT(*) > 0
    ORDER BY avg_price_change DESC
    """
    
    try:
        # Read data using SQLAlchemy engine
        df = pd.read_sql(query, engine)
        
        if not df.empty:
            print("\nTop 10 cryptocurrencies by price change and their sentiment metrics:")
            print(df.head(10).to_string())
            
            # Calculate correlations with price change
            correlation_columns = [
                'avg_price_change', 'avg_sentiment_up', 'avg_sentiment_down',
                'avg_interest_score', 'avg_twitter_sentiment', 'avg_reddit_sentiment',
                'avg_news_sentiment'
            ]
            correlations = df[correlation_columns].corr()['avg_price_change'].sort_values(ascending=False)
            
            print("\nCorrelations with price change:")
            print(correlations.to_string())
            
            # Save results to file in logs directory
            with open(log_file, 'w') as f:
                f.write("Sentiment Analysis Results\n")
                f.write("=========================\n\n")
                f.write("Top 10 cryptocurrencies by price change:\n")
                f.write(df.head(10).to_string())
                f.write("\n\nCorrelations with price change:\n")
                f.write(correlations.to_string())
                
            print(f"\nResults have been saved to {log_file}")
        else:
            print("\nNo data found for analysis")
            
    except Exception as e:
        print(f"Error during analysis: {str(e)}")
    finally:
        engine.dispose()

if __name__ == "__main__":
    analyze_social_impact() 