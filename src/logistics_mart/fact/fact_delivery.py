import yaml
import pandas as pd
import datetime
from base.base_gold_pipeline import BaseGoldPipeline

with open("src/logistics_mart/config/fact.yaml") as f:
    cfg = yaml.full_load(f)['tables']['fact_delivery']['dim_path']

class FactDelivery(BaseGoldPipeline):

    def build(self) -> pd.DataFrame:

        delivery_logs = pd.read_parquet(self.silver_path['delivery_logs'])
        orders = pd.read_parquet(self.silver_path['orders'])

        # JOIN TABLES
        fact = delivery_logs.merge(orders[['order_id', 'customer_id', 'restaurant_id', 'order_datetime']], on = 'order_id', how= 'left')

        # DATE KEY
        fact['date_key'] = fact['order_datetime'].dt.strftime('%Y%m%d')

        customers =         pd.read_parquet(cfg['customers'])
        restaurants =       pd.read_parquet(cfg['restaurants'])
        delivery_partner =  pd.read_parquet(cfg['delivery_partners'])

        fact = fact.merge(customers[['customer_id', 'customer_key']], on='customer_id')
        fact = fact.merge(restaurants[['restaurant_id', 'restaurant_key']], on='restaurant_id')
        fact = fact.merge(delivery_partner[['delivery_partner_id', 'delivery_partner_key']], on='delivery_partner_id')

        fact['is_delivered'] = fact['delivered_at'].notna().astype(int)
        fact['is_late'] =      (fact['total_delivery_mins'] > 30).astype(int)
        fact['delivery_id'] =  fact.index + 1

        return fact[[
            'delivery_id',
            'order_id',

            'customer_key',
            'restaurant_key',
            'delivery_partner_key',
            'date_key',

            'assign_to_pick_mins',
            'delivery_time_mins',
            'total_delivery_mins',
            
            'is_delivered',
            'is_late'
        ]]
    
