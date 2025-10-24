from importlib.metadata import files
import os
import pandas as pd
import logging
from datetime import datetime
from src.extract.api_client import fetch_data

DATA_DIR = "data/bronze"

def save_raw_data(data: list, dataset_name: str):
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    os.makedirs(DATA_DIR, exist_ok=True)

    file_path = os.path.join(DATA_DIR, f"{dataset_name}_{ts}.parquet")
    df = pd.DataFrame(data)
    df.to_parquet(file_path, index=False)

    logging.info(f"Extraction completed: {file_path}")
    return file_path

def extract_raw_data():
    logging.info("=== Starting Bronze extraction ===")

    datasets = ["deliveries", "vehicles", "carriers", "warehouses"]
    saved_files = []
    for ds in datasets:
        data = fetch_data(ds)
        if data != []:
            path = save_raw_data(data, ds)
            saved_files.append(path)
        else:
            logging.info(f"No data extracted for {ds}")
            
    return saved_files
