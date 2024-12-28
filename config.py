LUNARCRUSH_API_KEY = "eywy4f02q7who3j4hgwavb4kchm587ky04u536du"
ALPHA_VANTAGE_API_KEY = "C8VU0NRY6Z6RNWTH"
THE_TIE_API_KEY = "YOUR_THE_TIE_API_KEY"


# Database Configuration
DB_SERVER = 'MICROBOX\\SQLEXPRESS'
DB_NAME = 'CryptoAiDb'
DB_USER = 'CryptoAdm'
DB_PASSWORD = 'oracle69'

# Create the full connection string using SQL Server Native Client
DB_CONNECTION_STRING = (
    'DRIVER={SQL Server Native Client 11.0};'  # Updated driver
    f'SERVER={DB_SERVER};'
    f'DATABASE={DB_NAME};'
    f'UID={DB_USER};'
    f'PWD={DB_PASSWORD}'
)

# Debug info for connection string
if __name__ == "__main__":
    print("Database Connection String (without password):")
    debug_string = DB_CONNECTION_STRING.replace(DB_PASSWORD, '***')
    print(debug_string)

# Email Configuration with debug info
EMAIL_CONFIG = {
    'from': 'rosssaab@gmail.com',
    'to': 'where.to.send@gmail.com',
    'smtp_server': 'smtp.gmail.com',
    'smtp_port': 587,
    'username': 'rosssaab@gmail.com',
    'password': 'dbevsjqnublhbjgv',  # App Password with no spaces
    'debug_level': True  # Enable debug output
}

# Add debug print statements
if EMAIL_CONFIG['debug_level']:
    print("Email Configuration:")
    print(f"From: {EMAIL_CONFIG['from']}")
    print(f"To: {EMAIL_CONFIG['to']}")
    print(f"SMTP Server: {EMAIL_CONFIG['smtp_server']}")
    print(f"SMTP Port: {EMAIL_CONFIG['smtp_port']}")
    print(f"Username: {EMAIL_CONFIG['username']}")
    print(f"Password length: {len(EMAIL_CONFIG['password'])} characters")  # Print length instead of actual password for security