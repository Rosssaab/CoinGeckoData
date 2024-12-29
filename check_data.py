import pandas as pd
from sqlalchemy import create_engine
import urllib
from config import DB_CONNECTION_STRING

# Create SQLAlchemy engine
params = urllib.parse.quote_plus(DB_CONNECTION_STRING)
engine = create_engine(f'mssql+pyodbc:///?odbc_connect={params}')

# Check all data counts, even small amounts
query = """
SELECT 
    p.crypto_id,
    COUNT(*) as sample_count
FROM coingecko_crypto_daily_data p
LEFT JOIN coingecko_crypto_sentiment s 
    ON p.crypto_id = s.crypto_id 
    AND CAST(p.price_date AS DATE) = CAST(s.metric_date AS DATE)
GROUP BY p.crypto_id
ORDER BY COUNT(*) DESC
"""

df = pd.read_sql(query, engine)
print("\nAll cryptocurrencies and their sample counts:")
print(df.head(10))
print(f"\nTotal cryptocurrencies: {len(df)}")
print(f"Maximum samples for any crypto: {df['sample_count'].max()}")
print(f"Average samples per crypto: {df['sample_count'].mean():.2f}") 