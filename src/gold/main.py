import yaml
import logging

from dim.dim_date import DimDate
from dim.dim_payment_mode import DimPayment_mode
from dim.dim_order_status import DimOrder_status
from dim.dim_restaurants import DimRestaurants
from dim.dim_employees import DimEmployees
from dim.dim_customers import DimCustomers
from dim.dim_menu_items import DimMenu_items
from dim.dim_delivery_partners import DimDelivery_partners

from fact.fact_sales import FactSales

logging.basicConfig(
    level=logging.INFO,
    filename="log/gold.log",
    filemode="w",
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S"
)

DIM_MAP = {
    "dim_date": DimDate,
    "dim_payment_mode": DimPayment_mode,
    "dim_order_status": DimOrder_status,
    "dim_restaurants": DimRestaurants,
    "dim_employees": DimEmployees,
    "dim_customers": DimCustomers,
    "dim_menu_items": DimMenu_items,
    "dim_delivery_partners": DimDelivery_partners
}

FACT_MAP = {
    "fact_sales": FactSales
}

def run_domain(config_path, class_map):
    with open(config_path) as f:
        cfg = yaml.safe_load(f)

    for table, table_cfg in cfg["tables"].items():
        cls = class_map.get(table)
        if not cls:
            continue

        pipeline = cls({
            "domain": cfg["domain"],
            "table": table,
            **table_cfg
        })
        pipeline.run()

if __name__ == "__main__":
    run_domain("src/gold/config/dim.yaml", DIM_MAP)
    run_domain("src/gold/config/fact.yaml", FACT_MAP)
