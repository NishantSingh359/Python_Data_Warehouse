import yaml
import logging
from transforms.erp.restaurants import RestaurantsSilver
from transforms.erp.employees import EmployeesSilver
from transforms.erp.delivery_partners import Delivery_partnersSilver
from transforms.erp.menu_items import Menu_itemsSilver
from transforms.erp.kitchen_logs import Kitchen_logsSilver

from transforms.crm.customers import CustomersSilver
from transforms.crm.promotions import PromotionsSilver
from transforms.crm.orders import OrdersSilver
from transforms.crm.customer_reviews import Customer_reviewsSilver
from transforms.crm.order_items import Order_itemsSilver
from transforms.crm.delivery_logs import Delivery_logsSilver
from transforms.crm.refunds import RefundsSilver

logging.basicConfig(
    level=logging.INFO,
    filename="log/silver.log",
    filemode="w",
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

ERP_PIPELINE_MAP = {
    "restaurants": RestaurantsSilver,
    "employees": EmployeesSilver,
    "delivery_partners": Delivery_partnersSilver,
    "menu_items": Menu_itemsSilver,
}

ERP_PIPELINE_MAP1 = {
   "kitchen_logs": Kitchen_logsSilver,
}

CRM_PIPELINE_MAP = {
    "customers": CustomersSilver,
    'promotions': PromotionsSilver,
    "orders": OrdersSilver,
    "order_items": Order_itemsSilver,
    "customer_reviews": Customer_reviewsSilver,
    "delivery_logs": Delivery_logsSilver,
    "refunds": RefundsSilver
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


run_domain("src/silver_layer/config/erp.yaml", ERP_PIPELINE_MAP)
run_domain("src/silver_layer/config/crm.yaml", CRM_PIPELINE_MAP)
run_domain("src/silver_layer/config/erp.yaml", ERP_PIPELINE_MAP1)


