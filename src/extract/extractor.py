import os
import pandas as pd
from datetime import datetime
from src.extract.api_client import fetch_data

DATA_DIR = "data/bronze"

def save_raw_data(data: list, dataset_name: str):
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    os.makedirs(DATA_DIR, exist_ok=True)

    file_path = os.path.join(DATA_DIR, f"{dataset_name}_{ts}.parquet")
    df = pd.DataFrame(data)
    df.to_parquet(file_path, index=False)
    return file_path

def extract_all():
    datasets = ["deliveries", "vehicles", "carriers", "warehouses"]
    saved_files = []
    for ds in datasets:
        data = fetch_data(ds)
        if data != []:
            path = save_raw_data(data, ds)
            saved_files.append(path)
    return saved_files
