from coingecko_data_loader import CoinGeckoDataLoader
from argparse import Namespace

# Create args object with required parameters
args = Namespace(
    email_from='rosssaab@gmail.com',
    email_to='where.to.send@gmail.com',
    smtp_server='smtp.gmail.com',
    smtp_port=587,
    smtp_username='rosssaab@gmail.com',
    smtp_password='your_password_here'  # Replace with your actual password
)

if __name__ == "__main__":
    loader = CoinGeckoDataLoader(args)
    print("Starting master data update...")
    loader.update_master_data()
    print("Master data update completed!")
    
    print("Starting daily data update...")
    loader.update_daily_data()
    print("Daily data update completed!") 