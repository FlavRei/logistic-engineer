import pandas as pd
import logging

def clean_warehouses(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Cleaning warehouses...")

    df = df.copy()
    df = df.drop_duplicates()

    # Remplacer les chaînes vides et valeurs "unknown" par null
    df.replace(["", "unknown"], None, inplace=True)
    df = df[df["country"].notna() & df["city"].notna()]

    # Convertir les types de données
    df["capacity"] = pd.to_numeric(df["capacity"], errors="coerce")
    df["country"] = df["country"].astype(str).str.title()
    df["city"] = df["city"].astype(str).str.title()

    # Valeurs manquantes
    df["id"] = df["id"].fillna(pd.util.hash_pandas_object(df).astype(str))
    df["capacity"] = df["capacity"].fillna(100)

    # Filtrer les lignes incohérentes
    df = df[df["capacity"] >= 0]

    return df
