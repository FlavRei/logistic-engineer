import pandas as pd

def compute_global_vehicle_metrics(df_vehicles: pd.DataFrame, df_deliveries: pd.DataFrame) -> pd.DataFrame:
    metrics = {
        # Activité
        "total_vehicles": len(df_vehicles),
        "deliveries_by_vehicle": df_deliveries["vehicle_id"].value_counts().to_dict(),
        "avg_load_factor": (df_deliveries.groupby("vehicle_id")["quantity_tons"].mean() / df_vehicles["capacity_tons"]).mean() * 100,
        "vehicle_utilization_rate": (df_deliveries["vehicle_id"].nunique() / len(df_vehicles)) * 100,

        # Performance
        "top_vehicles_by_distance": df_deliveries.groupby("vehicle_id")["distance_km"].sum().nlargest(10).to_dict(),
        "top_vehicles_by_tonnage": df_deliveries.groupby("vehicle_id")["quantity_tons"].sum().nlargest(10).to_dict(),
    }

    return pd.DataFrame([metrics])
