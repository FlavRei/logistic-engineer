import logging
from src.extract.extractor import extract_all

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

if __name__ == "__main__":
    files = extract_all()
    if files:
        logging.info("Extraction completed:")
    else:
        logging.info("No data extracted.")
    for f in files:
        logging.info(f"{f}")
