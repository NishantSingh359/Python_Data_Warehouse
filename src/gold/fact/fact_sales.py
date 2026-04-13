import yaml
import pandas as pd
import datetime
from base.base_gold_pipeline import BaseGoldPipeline

with open("src/gold/config/fact.yaml") as f:
    cfg = yaml.full_load(f)['tables']['fact_sales']['dim_path']

class FactSales(BaseGoldPipeline):

    def build(self) -> pd.DataFrame:

        kitchen_logs = pd.read_parquet(self.silver_path['kitchen_logs'])
        order_items = pd.read_parquet(self.silver_path['order_items'])
        orders = pd.read_parquet(self.silver_path['orders'])
        delivery_logs = pd.read_parquet(self.silver_path['delivery_logs'])

        # JOIN TABLES
        fact = order_items.merge(orders, on = 'order_id', how= 'inner')

        fact = fact.merge(kitchen_logs[['order_item_id', 'prep_time_mins']], on = 'order_item_id', how='left')

        fact = fact.merge(delivery_logs[['order_id', 'total_delivery_mins']], on='order_id', how='left')

        # DERIVED COLUMNS
        fact['sales_amount'] = fact['quantity'] * fact['unit_price']
        fact['is_late'] = fact['total_delivery_mins'] > 45


        # DATE KEY
        fact['date_key'] = fact['order_datetime'].dt.strftime('%Y%m%d').astype('int64')

        customers =         pd.read_parquet(cfg['customers'])
        restaurants =       pd.read_parquet(cfg['restaurants'])
        menu_items =        pd.read_parquet(cfg['menu_items'])
        delivery_partners = pd.read_parquet(cfg['delivery_partners'])

        fact = fact.merge(customers[['customer_id', 'customer_key']], on='customer_id')
        fact = fact.merge(restaurants[['restaurant_id', 'restaurant_key']], on='restaurant_id')
        fact = fact.merge(menu_items[['item_id', 'item_key']], on='item_id')
        fact = fact.merge(delivery_partners[['delivery_partner_id', 'delivery_partner_key']], on='delivery_partner_id')

        #fact.drop(columns=['customer_id', 'restaurant_id', 'item_id', 'delivery_partner_id'], inplace= True)
        
        return fact[
            [   
                "order_id",
                "order_item_id",
                "customer_key",
                "restaurant_key",
                "item_key",
                "delivery_partner_key",

                "date_key",
                "order_datetime",
            
                "quantity",
                "unit_price",
                "sales_amount",

                "prep_time_mins",
                "total_delivery_mins",
                'is_late',

                "order_status"
            ]
        ]
    
