import pandas as pd

def compute_global_carrier_metrics(df_carriers: pd.DataFrame, df_deliveries: pd.DataFrame) -> pd.DataFrame:
    metrics = {
        # Volume et activité
        "total_carriers": len(df_carriers),
        "deliveries_by_carrier": df_deliveries["carrier_id"].value_counts().to_dict(),
        "tonnage_by_carrier": df_deliveries.groupby("carrier_id")["quantity_tons"].sum().to_dict(),
        "avg_distance_by_carrier": df_deliveries.groupby("carrier_id")["distance_km"].mean().to_dict(),

        # Qualité et satisfaction
        "on_time_rate_by_carrier": df_deliveries.groupby("carrier_id").apply(lambda x: (x["delay_days"] <= 0).mean(), include_groups=False).to_dict(),
        "avg_delay_by_carrier": df_deliveries.groupby("carrier_id")["delay_days"].mean().to_dict(),
        "carrier_rating_avg": df_carriers["rating"].mean(),

        # Classement
        "carrier_performance_score": {
            carrier_id: (
                (1 - (df_deliveries[df_deliveries["carrier_id"] == carrier_id]["delay_days"] > 0).mean()) * 0.4 +
                (df_deliveries[df_deliveries["carrier_id"] == carrier_id]["quantity_tons"].sum() / df_deliveries["quantity_tons"].sum()) * 0.4 +
                (df_carriers[df_carriers["id"] == carrier_id]["rating"].values[0] / 5) * 0.2
            )
            for carrier_id in df_carriers["id"]
        },
    }

    return pd.DataFrame([metrics])
