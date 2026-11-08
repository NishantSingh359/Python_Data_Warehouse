import yaml
import pandas as pd
import datetime
from base.base_gold_pipeline import BaseGoldPipeline

with open("src/sales_mart/config/fact.yaml") as f:
    cfg = yaml.full_load(f)['tables']['fact_sales']['dim_path']

class FactSales(BaseGoldPipeline):

    def build(self) -> pd.DataFrame:

        order_items = pd.read_parquet(self.silver_path['order_items'])
        orders =      pd.read_parquet(self.silver_path['orders'])
    
        fact = order_items.merge(orders, on = 'order_id', how= 'inner')

        customers =         pd.read_parquet(cfg['customers'])
        restaurants =       pd.read_parquet(cfg['restaurants'])
        menu_items =        pd.read_parquet(cfg['menu_items'])
        payment_mode =      pd.read_parquet(cfg['payment_mode'])
        order_status =      pd.read_parquet(cfg['order_status'])
        refund =            pd.read_parquet(cfg['refund'])

        fact = fact.merge(customers[['customer_id', 'customer_key']], on='customer_id')
        fact = fact.merge(restaurants[['restaurant_id', 'restaurant_key']], on='restaurant_id')
        fact = fact.merge(menu_items[['item_id', 'item_key']], on='item_id')
        fact = fact.merge(payment_mode[['payment_mode_key', 'payment_mode']], on='payment_mode')
        fact = fact.merge(order_status, on=['order_status', 'cancel_stage', 'cancel_reason'])
        fact = fact.merge(refund[['refund_key', 'order_id']], on='order_id')

        fact['order_date'] = pd.to_datetime(fact['order_datetime'].dt.strftime("%Y-%m-%d"))
        fact['sales_key'] = fact.index + 1

        return fact[[
            'sales_key',
            'order_id',

            'customer_key',
            'restaurant_key',
            'item_key',
            'order_status_key',
            'payment_mode_key',
            'refund_key',

            'order_date',

            'quantity',
            'unit_price',
            'line_total',

            'subtotal',
            'delivery_fee',
            'discount_amount',
            'grand_total'
        ]]
    
