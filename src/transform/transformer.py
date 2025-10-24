import os
import re
import pandas as pd
import logging
from src.transform.cleaner.deliveries_cleaner import clean_deliveries
from src.transform.cleaner.vehicles_cleaner import clean_vehicles
from src.transform.cleaner.carriers_cleaner import clean_carriers
from src.transform.cleaner.warehouses_cleaner import clean_warehouses

BRONZE_PATH = "data/bronze"
SILVER_PATH = "data/silver"

PRIMARY_KEYS = {
    "deliveries": "id",
    "vehicles": "id",
    "carriers": "id",
    "warehouses": "id"
}

FOREIGN_KEYS = {
    "deliveries": {
        "vehicle_id": "vehicles",
        "carrier_id": "carriers",
        "origin_id": "warehouses",
        "destination_id": "warehouses"
    },
    "vehicles": {
        "carrier_id": "carriers"
    }
}

def read_latest_parquet(dataset_name: str) -> pd.DataFrame:
    matching_files = [
        f for f in os.listdir(BRONZE_PATH)
        if dataset_name in f and f.endswith(".parquet")
    ]

    if not matching_files:
        raise FileNotFoundError(f"No parquet files found for dataset '{dataset_name}' in {BRONZE_PATH}")

    matching_files = sorted(
        matching_files,
        key=lambda x: os.path.getmtime(os.path.join(BRONZE_PATH, x)),
        reverse=True
    )

    latest_file = matching_files[0]
    full_path = os.path.join(BRONZE_PATH, latest_file)
    match = re.search(r"(\d{8}_\d{6})", latest_file)
    timestamp = match.group(1) if match else "unknown_timestamp"

    return pd.read_parquet(full_path), timestamp

def save_silver(df: pd.DataFrame, dataset_name: str, timestamp: str):
    os.makedirs(SILVER_PATH, exist_ok=True)
    path = os.path.join(SILVER_PATH, f"{dataset_name}_{timestamp}.parquet")
    df.to_parquet(path, index=False)
    logging.info(f"Saved cleaned data: {path}")

def validate_foreign_keys(df: pd.DataFrame, df_dict: dict, ds_name: str):
    anomalies = pd.DataFrame(columns=df.columns)
    valid = df.copy()

    fks = FOREIGN_KEYS.get(ds_name, {})
    for fk_col, ref_table in fks.items():
        if fk_col in valid.columns and ref_table in df_dict:
            ref_ids = set(df_dict[ref_table][PRIMARY_KEYS[ref_table]])
            mask_missing = ~valid[fk_col].isin(ref_ids)
            if mask_missing.any():
                anomalies = pd.concat([anomalies, valid[mask_missing]])
                valid = valid[~mask_missing]

    return valid, anomalies

def transform_data():
    logging.info("=== Starting Silver transformation ===")

    datasets = {
        "deliveries": clean_deliveries,
        "vehicles": clean_vehicles,
        "carriers": clean_carriers,
        "warehouses": clean_warehouses,
    }

    bronze_data = {}
    timestamps = {}
    for ds_name in datasets.keys():
        df, ts = read_latest_parquet(ds_name)
        bronze_data[ds_name] = df
        timestamps[ds_name] = ts

    all_silver = {}
    for ds_name, cleaner_fn in datasets.items():
        all_silver[ds_name] = cleaner_fn(bronze_data[ds_name])

    all_anomalies = {}
    for ds_name in datasets.keys():
        valid, anomalies = validate_foreign_keys(all_silver[ds_name], all_silver, ds_name)
        all_silver[ds_name] = valid
        all_anomalies[ds_name] = anomalies

        save_silver(valid, ds_name, timestamps[ds_name])
        if not anomalies.empty:
            save_silver(anomalies, f"anomalies_{ds_name}", timestamps[ds_name])

    logging.info("=== Silver transformation with validation completed ===")
