@echo off
cd /d "C:\CryptoAnalytics\DataSupplierTest"

REM Run model training (once per day)
IF %TIME:~0,2% EQU 00 (
    echo Training models...
    python crypto_model_trainer.py
)

REM Run predictions (every 2 hours)
echo Making predictions...
python crypto_predictor.py 