import yaml
import pandas as pd
import datetime
from base.base_gold_pipeline import BaseGoldPipeline

with open("src/sales_mart/config/fact.yaml") as f:
    cfg = yaml.full_load(f)['tables']['fact_sales']['dim_path']

class FactSales(BaseGoldPipeline):

    def build(self) -> pd.DataFrame:

        order_items = pd.read_parquet(self.silver_path['order_items'])
        orders = pd.read_parquet(self.silver_path['orders'])
        refunds = pd.read_parquet(self.silver_path['refunds'])

        # JOIN TABLES
        fact = order_items.merge(orders, on = 'order_id', how= 'inner')
        fact = fact.merge(refunds[['order_id', 'refund_amount']], on='order_id', how='left')

        # DATE KEY
        fact['date_key'] = fact['order_datetime'].dt.strftime('%Y%m%d')

        customers =         pd.read_parquet(cfg['customers'])
        restaurants =       pd.read_parquet(cfg['restaurants'])
        menu_items =        pd.read_parquet(cfg['menu_items'])

        fact = fact.merge(customers[['customer_id', 'customer_key']], on='customer_id')
        fact = fact.merge(restaurants[['restaurant_id', 'restaurant_key']], on='restaurant_id')
        fact = fact.merge(menu_items[['item_id', 'item_key']], on='item_id')

        fact['refund_amount'] = fact['refund_amount'].fillna(0)
        fact['is_refunded'] = (fact['refund_amount'] > 0).astype(int)

        #fact.drop(columns=['customer_id', 'restaurant_id', 'item_id'], inplace= True)
        
        fact['sales_id'] = fact.index + 1

        return fact[[
            'sales_id',
            'order_id',
            'order_item_id',

            'customer_key',
            'restaurant_key',
            'item_key',
            'date_key',

            'quantity',
            'unit_price',
            'line_total',

            'item_total',
            'delivery_fee',
            'discount_amount',
            'order_total',

            'order_status',
            'payment_mode',
            
            'is_refunded',
            'refund_amount'
        ]]
    
