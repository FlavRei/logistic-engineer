import pandas as pd

def compute_global_warehouse_metrics(df_warehouses: pd.DataFrame, df_deliveries: pd.DataFrame) -> pd.DataFrame:
    metrics = {
        # Activité
        "total_warehouses": len(df_warehouses),
        "nb_departures_by_warehouse": df_deliveries["origin_id"].value_counts().to_dict(),
        "nb_arrivals_by_warehouse": df_deliveries["destination_id"].value_counts().to_dict(),
        "avg_quantity_in": df_deliveries.groupby("destination_id")["quantity_tons"].mean().to_dict(),
        "avg_quantity_out": df_deliveries.groupby("origin_id")["quantity_tons"].mean().to_dict(),
        "warehouse_utilization_rate": {
            warehouse_id: (
                df_deliveries[(df_deliveries["origin_id"] == warehouse_id) | (df_deliveries["destination_id"] == warehouse_id)]["quantity_tons"].sum() /
                df_warehouses[df_warehouses["id"] == warehouse_id]["capacity"].values[0]
            ) * 100
            for warehouse_id in df_warehouses["id"]
        },

        # Flux
        # "top_routes": df_deliveries.groupby(["origin_id", "destination_id"]).size().nlargest(10).to_dict(),
        # "avg_distance_between_warehouses": df_deliveries.groupby(["origin_id", "destination_id"])["distance_km"].mean().to_dict(),
    }

    return pd.DataFrame([metrics])
