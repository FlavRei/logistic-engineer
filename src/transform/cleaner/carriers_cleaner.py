import pandas as pd
import logging

def clean_carriers(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Cleaning carriers...")

    df = df.copy()
    df = df.drop_duplicates()

    # Remplacer les chaînes vides et valeurs "unknown" par null
    df.replace(["", "unknown"], None, inplace=True)

    # Convertir les types de données
    df["rating"] = pd.to_numeric(df["rating"], errors="coerce")

    # Valeurs manquantes
    df["id"] = df["id"].fillna(pd.util.hash_pandas_object(df).astype(str))
    df["name"] = df["name"].fillna("Unknown Carrier")
    df["country"] = df["country"].fillna("Unknown Country")

    return df
