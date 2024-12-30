@echo off
echo Starting Crypto Model Trainer...
cd C:\DataLoaders\AiTrainer
call venv\Scripts\activate.bat
python crypto_model_trainer.py >> training_log_%date:~-4,4%%date:~-10,2%%date:~-7,2%.txt 2>&1
call venv\Scripts\deactivate.bat
echo Training completed. 