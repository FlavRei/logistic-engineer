import pandas as pd
import logging

def clean_deliveries(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Cleaning deliveries...")

    df = df.copy()
    df = df.drop_duplicates()

    # Remplacer les chaînes vides et valeurs "unknown" par null
    df.replace(["", "unknown", "404"], None, inplace=True)

    # Convertir les types de données
    df["quantity_tons"] = pd.to_numeric(df["quantity_tons"], errors="coerce")
    df["distance_km"] = pd.to_numeric(df["distance_km"], errors="coerce")
    df["delay_days"] = pd.to_numeric(df["delay_days"], errors="coerce")
    df["status"] = df["status"].astype(str).str.replace("_", " ").str.strip().str.title()
    df["departure_time"] = pd.to_datetime(df["departure_time"], errors="coerce")
    df["arrival_time"] = pd.to_datetime(df["arrival_time"], errors="coerce")
    df["departure_time"] = df["departure_time"].dt.strftime("%Y-%m-%dT%H:%M:%S.%f")
    df["arrival_time"] = df["arrival_time"].dt.strftime("%Y-%m-%dT%H:%M:%S.%f")

    # Valeurs manquantes
    df["id"] = df["id"].fillna(pd.util.hash_pandas_object(df).astype(str))
    df["departure_time"] = df["departure_time"].fillna(pd.Timestamp.now().strftime("%Y-%m-%dT%H:%M:%S.%f"))
    df["arrival_time"] = df["arrival_time"].fillna((pd.Timestamp.now() + pd.Timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%S.%f"))
    df["quantity_tons"] = df["quantity_tons"].fillna(df["quantity_tons"].mean()).round(2)
    df["distance_km"] = df["distance_km"].fillna(df["distance_km"].mean()).round(2)
    df["delay_days"] = df["delay_days"].fillna(0)
    df["status"] = df["status"].fillna("On Time")

    # Filtrer les lignes incohérentes
    df = df[df["quantity_tons"] >= 0]

    # Nettoyer les IDs invalides
    df = df[df["carrier_id"].notna() & df["origin_id"].notna() & df["destination_id"].notna()]

    return df
