import pyodbc
from config import DB_CONNECTION_STRING
from datetime import datetime, timedelta

def check_tables():
    conn = pyodbc.connect(DB_CONNECTION_STRING)
    cursor = conn.cursor()
    
    print("Checking database tables...\n")
    
    # Check master table
    cursor.execute("SELECT COUNT(*) FROM coingecko_crypto_master")
    master_count = cursor.fetchone()[0]
    print(f"Master table count: {master_count} coins")
    
    if master_count > 0:
        cursor.execute("SELECT TOP 5 id, symbol, name FROM coingecko_crypto_master")
        print("\nSample master records:")
        for row in cursor.fetchall():
            print(f"  {row.id}: {row.symbol} - {row.name}")
    
    # Check daily data
    today = datetime.now().date()
    cursor.execute("SELECT COUNT(*) FROM coingecko_crypto_daily_data WHERE price_date = ?", today)
    daily_count = cursor.fetchone()[0]
    print(f"\nDaily data count for today: {daily_count} records")
    
    if daily_count > 0:
        cursor.execute("""
            SELECT TOP 5 crypto_id, current_price, market_cap, price_change_24h 
            FROM coingecko_crypto_daily_data 
            WHERE price_date = ?
        """, today)
        print("\nSample daily records:")
        for row in cursor.fetchall():
            print(f"  {row.crypto_id}: ${row.current_price:.2f}, MC: ${row.market_cap:,.2f}, Change: {row.price_change_24h}%")
    
    # Check social data
    cursor.execute("SELECT COUNT(*) FROM coingecko_crypto_social_data WHERE metric_date = ?", today)
    social_count = cursor.fetchone()[0]
    print(f"\nSocial data count for today: {social_count} records")
    
    if social_count > 0:
        cursor.execute("""
            SELECT TOP 5 crypto_id, twitter_followers, reddit_subscribers, reddit_active_users_48h 
            FROM coingecko_crypto_social_data 
            WHERE metric_date = ?
        """, today)
        print("\nSample social records:")
        for row in cursor.fetchall():
            print(f"  {row.crypto_id}: Twitter: {row.twitter_followers:,}, Reddit: {row.reddit_subscribers:,}, Active: {row.reddit_active_users_48h}")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    check_data() 