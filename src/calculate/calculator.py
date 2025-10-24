import logging
from src.calculate.save_metrics import read_latest_parquet, save_gold
from src.calculate.metrics.metrics_carriers import compute_global_carrier_metrics
from src.calculate.metrics.metrics_deliveries import compute_global_delivery_metrics
from src.calculate.metrics.metrics_vehicles import compute_global_vehicle_metrics
from src.calculate.metrics.metrics_warehouses import compute_global_warehouse_metrics

def calculate_global_metrics():
    logging.info("=== Starting Gold layer computation ===")
    
    df_carriers = read_latest_parquet("carriers")
    df_deliveries = read_latest_parquet("deliveries")
    df_vehicles = read_latest_parquet("vehicles")
    df_warehouses = read_latest_parquet("warehouses")

    df_carriers_metrics = compute_global_carrier_metrics(df_carriers, df_deliveries)
    save_gold(df_carriers_metrics, "carriers_metrics")

    df_deliveries_metrics = compute_global_delivery_metrics(df_deliveries)
    save_gold(df_deliveries_metrics, "deliveries_metrics")

    df_vehicles_metrics = compute_global_vehicle_metrics(df_vehicles, df_deliveries)
    save_gold(df_vehicles_metrics, "vehicles_metrics")

    df_warehouses_metrics = compute_global_warehouse_metrics(df_warehouses, df_deliveries)
    save_gold(df_warehouses_metrics, "warehouses_metrics")

    logging.info("Gold layer computation completed successfully")
