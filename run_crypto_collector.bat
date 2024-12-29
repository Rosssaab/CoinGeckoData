@echo off
cd /d "C:\CryptoAnalytics\DataSupplierTest" 
python coingecko_data_loader.py --master --daily --sentiment 
