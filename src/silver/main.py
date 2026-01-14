import yaml
import logging
from erp.suppliers import SuppliersSilver
from erp.ingredients import IngredientsSilver
from erp.supplier_ingredients import Supplier_IngredientsSilver
from erp.menu_items import Menu_itemsSilver
from erp.recipe import RecipeSilver
from erp.restaurants import RestaurantsSilver
from erp.inventory import InventorySilver
from erp.delivery_partners import Delivery_partnersSilver
from erp.employees import EmployeesSilver

from crm.customers import CustomersSilver
from crm.orders import OrdersSilver
from crm.customer_reviews import Customer_reviewsSilver
from crm.order_items import Order_itemsSilver
from crm.kitchen_logs import Kitchen_logsSilver

logging.basicConfig(
    level=logging.INFO,
    filename="log/silver.log",
    filemode="w",
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S"
)

ERP_PIPELINE_MAP = {
    "suppliers": SuppliersSilver,
    "ingredients": IngredientsSilver,
    "supplier_ingredients": Supplier_IngredientsSilver,
    "menu_items": Menu_itemsSilver,
    "recipe": RecipeSilver,
    "restaurants": RestaurantsSilver,
    "inventory": InventorySilver,
    "delivery_partners": Delivery_partnersSilver,
    "employees": EmployeesSilver
}

CRM_PIPELINE_MAP = {
    "customers": CustomersSilver,
    "orders": OrdersSilver,
    "customer_reviews": Customer_reviewsSilver,
    "order_items": Order_itemsSilver,
    "kitchen_logs": Kitchen_logsSilver
}

def run_domain(config_path, pipeline_map):
    with open(config_path) as f:
        cfg = yaml.safe_load(f)

    for table, table_cfg in cfg["tables"].items():
        cls = pipeline_map.get(table)
        if not cls:
            continue

        pipeline = cls({
            "domain": cfg["domain"],
            "table": table,
            **table_cfg
        })
        pipeline.run()

if __name__ == "__main__":
    run_domain("src/silver/config/erp.yaml", ERP_PIPELINE_MAP)
    run_domain("src/silver/config/crm.yaml", CRM_PIPELINE_MAP)



