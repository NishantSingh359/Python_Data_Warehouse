import yaml
import logging

from dim.dim_date import DimDate
from dim.dim_chef import DimChef
from dim.dim_items import DimItems
from dim.dim_restaurants import DimRestaurants

from fact.fact_kitchen import FactKitchen

logging.basicConfig(
    level=logging.INFO,
    filename="log/ops_mart.log",
    filemode="w",
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S"
)

DIM_MAP = {
    "dim_date": DimDate,
    "dim_chef": DimChef,
    "dim_items": DimItems,
    "dim_restaurants": DimRestaurants
}

FACT_MAP = {
    "fact_kitchen": FactKitchen
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
    run_domain("src/ops_mart/config/dim.yaml", DIM_MAP)
    run_domain("src/ops_mart/config/fact.yaml", FACT_MAP)
