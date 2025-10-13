import requests
import logging

BASE_URL = "http://localhost:8000"

def fetch_data(endpoint: str):
    url = f"{BASE_URL}/{endpoint}"
    logging.info(f"Fetching data from {url}")
    resp = requests.get(url)
    resp.raise_for_status()
    return resp.json()
