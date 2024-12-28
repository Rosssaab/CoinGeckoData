import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from config import EMAIL_CONFIG

def test_email():
    print("Starting email test...")
    print(f"Using email: {EMAIL_CONFIG['username']}")
    print(f"SMTP Server: {EMAIL_CONFIG['smtp_server']}:{EMAIL_CONFIG['smtp_port']}")
    
    try:
        # Create message
        msg = MIMEMultipart()
        msg['From'] = EMAIL_CONFIG['from']
        msg['To'] = EMAIL_CONFIG['to']
        msg['Subject'] = 'Test Email from Crypto Data Loader'
        
        body = 'This is a test email to verify the email configuration is working correctly.'
        msg.attach(MIMEText(body, 'plain'))
        
        # Create SMTP session
        print("Connecting to SMTP server...")
        server = smtplib.SMTP(EMAIL_CONFIG['smtp_server'], EMAIL_CONFIG['smtp_port'])
        server.starttls()
        
        # Login
        print("Attempting login...")
        server.login(EMAIL_CONFIG['username'], EMAIL_CONFIG['password'])
        print("Login successful!")
        
        # Send email
        print("Sending test email...")
        text = msg.as_string()
        server.sendmail(EMAIL_CONFIG['from'], EMAIL_CONFIG['to'], text)
        print("Test email sent successfully!")
        
        # Close session
        server.quit()
        
    except Exception as e:
        print(f"Error sending email: {str(e)}")
        print("\nDebug information:")
        print(f"From: {EMAIL_CONFIG['from']}")
        print(f"To: {EMAIL_CONFIG['to']}")
        print(f"Username: {EMAIL_CONFIG['username']}")
        print(f"Password length: {len(EMAIL_CONFIG['password'])} characters")

if __name__ == "__main__":
    test_email() 