import yaml
import logging

from transforms.dim.dim_date import DimDate
from transforms.dim.dim_delivery_partners import DimDelivery_Partners
from transforms.dim.dim_customers import DimCustomers
from transforms.dim.dim_restaurants import DimRestaurants

from transforms.fact.fact_delivery import FactDelivery

logging.basicConfig(
    level=logging.INFO,
    filename="log/logistics_mart.log",
    filemode="w",
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S"
)

DIM_MAP = {
    "dim_date": DimDate,
    "dim_delivery_partners": DimDelivery_Partners,
    "dim_customers": DimCustomers,
    "dim_restaurants": DimRestaurants
}

FACT_MAP = {
    "fact_delivery": FactDelivery
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
    run_domain("src/logistics_mart/config/dim.yaml", DIM_MAP)
    run_domain("src/logistics_mart/config/fact.yaml", FACT_MAP)
