import json
import pandas as pd
from datetime import datetime
from pathlib import Path

def transform_crypto_data():
    date = datetime.utcnow().strftime("%Y-%m-%d")
    raw_file = Path(f"/opt/airflow/data/raw/{date}/crypto.json")

    with open(raw_file) as f:
        data = json.load(f)

    df = pd.DataFrame(data)
    df = df[[
        "id", "symbol", "current_price",
        "market_cap", "total_volume"
    ]]

    df["timestamp"] = datetime.utcnow()

    output = Path(f"/opt/airflow/data/raw/{date}/crypto_transformed.csv")
    df.to_csv(output, index=False)
