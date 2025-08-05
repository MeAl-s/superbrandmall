cd "C:\Point Detection\app"
python main.py

# Basic test
curl http://localhost:8000/

# Database test
curl http://localhost:8000/test-db

# Get receipts
curl http://localhost:8000/receipts?limit=5