import os
import pandas as pd
import logging

SILVER_PATH = "data/silver"
GOLD_PATH = "data/gold"

def read_latest_parquet(dataset_name: str) -> pd.DataFrame:
    matching = [f for f in os.listdir(SILVER_PATH) if dataset_name in f and f.endswith(".parquet")]
    if not matching:
        raise FileNotFoundError(f"No silver file found for {dataset_name}")
    matching.sort(reverse=True)
    latest = matching[0]
    return pd.read_parquet(os.path.join(SILVER_PATH, latest))

def save_gold(df: pd.DataFrame, name: str):
    os.makedirs(GOLD_PATH, exist_ok=True)
    path = os.path.join(GOLD_PATH, f"{name}.parquet")
    df.to_parquet(path, index=False)
    logging.info(f"Saved Gold dataset: {path}")
