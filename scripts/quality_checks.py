from sqlalchemy import create_engine
import pandas as pd

def run_quality_checks():
    engine = create_engine(
        "postgresql+psycopg2://warehouse:warehouse@warehouse:5432/crypto_warehouse"
    )

    df = pd.read_sql("SELECT * FROM fact_crypto_prices", engine)

    assert df.isnull().sum().sum() == 0, "Null values found!"
    assert (df["current_price"] > 0).all(), "Invalid prices!"
