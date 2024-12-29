@echo off
cd /d "C:\CryptoAnalytics\DataSupplierTest"

:: Suppress TensorFlow warnings
set TF_CPP_MIN_LOG_LEVEL=2
set TF_ENABLE_ONEDNN_OPTS=0

echo Running test pipeline...
python test_crypto_pipeline.py

echo Done!
pause 