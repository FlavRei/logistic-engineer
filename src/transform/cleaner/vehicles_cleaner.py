import pandas as pd
import logging

def clean_vehicles(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Cleaning vehicles...")

    df = df.copy()
    df = df.drop_duplicates()
    df = df[df["registration_number"].notna()]

    # Remplacer les chaînes vides et valeurs "unknown" par null
    df.replace(["", "unknown"], None, inplace=True)

    # Convertir les types de données
    df["capacity_tons"] = pd.to_numeric(df["capacity_tons"], errors="coerce")
    df["type"] = df["type"].astype(str).str.title()
    df["registration_number"] = df["registration_number"].str.replace(r"[^A-Za-z0-9]", "", regex=True).str.upper()

    # Valeurs manquantes
    df["id"] = df["id"].fillna(pd.util.hash_pandas_object(df).astype(str))
    df["capacity_tons"] = df["capacity_tons"].fillna(2)
    df["type"] = df["type"].fillna("Unknown")

    # Filtrer les lignes incohérentes
    df = df[df["capacity_tons"] >= 0]

    # Nettoyer les IDs invalides
    df = df[df["carrier_id"].notna()]

    return df
