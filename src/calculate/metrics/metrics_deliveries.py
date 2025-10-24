import pandas as pd

def compute_global_delivery_metrics(df_deliveries: pd.DataFrame) -> pd.DataFrame:
    metrics = {
        # Volume et répartition
        "total_deliveries": len(df_deliveries),
        "deliveries_by_status": df_deliveries["status"].value_counts().to_dict(),
        "deliveries_by_carrier": df_deliveries["carrier_id"].value_counts().to_dict(),
        "deliveries_by_destination": df_deliveries["destination_id"].value_counts().to_dict(),

        # Performance temporelle
        "avg_delivery_duration_hours": (
            (pd.to_datetime(df_deliveries["arrival_time"]) - pd.to_datetime(df_deliveries["departure_time"]))
            .dt.total_seconds()
            .mean() / 3600
        ),
        "avg_delay_days": df_deliveries["delay_days"].mean(),
        "on_time_rate": (df_deliveries["delay_days"] <= 0).mean(),

        # Efficacité et productivité
        "avg_quantity_per_delivery": df_deliveries["quantity_tons"].mean(),
        "avg_distance_km": df_deliveries["distance_km"].mean(),
    }

    return pd.DataFrame([metrics])
