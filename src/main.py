import logging
from src.extract.extractor import extract_all
from src.transform.transformer import transform_all

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

if __name__ == "__main__":
    extract_all()
    transform_all()
