import yaml
import pandas as pd
import datetime
from base.base_gold_pipeline import BaseGoldPipeline

with open("src/gold/config/fact.yaml") as f:
    cfg = yaml.full_load(f)['tables']['fact_sales']['dim_path']

class FactSales(BaseGoldPipeline):

    def build(self) -> pd.DataFrame:

        kic = pd.read_parquet(self.silver_path['kitchen_logs'])
        ord_itm = pd.read_parquet(self.silver_path['order_items'])
        ordr = pd.read_parquet(self.silver_path['orders'])

        fact = (
            kic
            .merge(ord_itm, on="order_item_id", how="left")
            .merge(ordr, on="order_id", how="left")
        )

        # ---------- DIM joins ----------
        fact = fact.merge(
            pd.read_parquet(cfg['payment_mode']),
            on="payment_mode",
            how="left"
        )

        fact = fact.merge(
            pd.read_parquet(cfg['order_status']),
            on="order_status",
            how="left"
        )

        fact = fact.merge(
            pd.read_parquet(cfg['customers'])
            [["customer_id","customer_key"]],
            on="customer_id",
            how="left"
        )

        fact = fact.merge(
            pd.read_parquet(cfg['restaurants'])
            [["restaurant_id","restaurant_key"]],
            on="restaurant_id",
            how="left"
        )

        fact = fact.merge(
            pd.read_parquet(cfg['menu_items'])
            [["item_id","item_key"]],
            on="item_id",
            how="left"
        )

        fact["date_key"] = (
            fact["order_datetime"]
            .dt.strftime("%Y%m%d")
            .astype("Int64")
        )

        return fact[
            [
                "order_item_id",
                "order_id",
                "order_datetime",
                "date_key",
                "customer_key",
                "restaurant_key",
                "item_key",
                "quantity",
                "unit_price",
                "line_total",
                "is_delivery"
            ]
        ]
