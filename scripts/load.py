import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
from pathlib import Path

def load_to_postgres():
    date = datetime.utcnow().strftime("%Y-%m-%d")
    csv_path = Path(f"/opt/airflow/data/raw/{date}/crypto_transformed.csv")

    engine = create_engine(
        "postgresql+psycopg2://warehouse:warehouse@warehouse:5432/crypto_warehouse"
    )

    df = pd.read_csv(csv_path)
    df.to_sql(
        "fact_crypto_prices",
        engine,
        if_exists="append",
        index=False
    )
