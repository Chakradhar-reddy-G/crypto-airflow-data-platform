import requests
import json
from datetime import datetime
from pathlib import Path

def extract_crypto_data():
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 10,
        "page": 1
    }

    response = requests.get(url, params=params)
    data = response.json()

    date = datetime.utcnow().strftime("%Y-%m-%d")
    raw_path = Path(f"/opt/airflow/data/raw/{date}")
    raw_path.mkdir(parents=True, exist_ok=True)

    with open(raw_path / "crypto.json", "w") as f:
        json.dump(data, f)
