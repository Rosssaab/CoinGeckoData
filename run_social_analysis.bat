@echo off
echo Starting Social Impact Analysis...
cd /d "C:\CryptoAnalytics\DataSupplierTest"
python analyze_social_impact.py >> social_analysis_log_%date:~-4,4%%date:~-10,2%%date:~-7,2%.txt 2>&1
echo Social Impact Analysis completed. 