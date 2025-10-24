import logging
from src.extract.extractor import extract_raw_data
from src.transform.transformer import transform_data
from src.calculate.calculator import calculate_global_metrics

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

if __name__ == "__main__":
    extract_raw_data()
    transform_data()
    calculate_global_metrics()
