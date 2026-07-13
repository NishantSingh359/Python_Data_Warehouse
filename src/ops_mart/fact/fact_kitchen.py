import yaml
import pandas as pd
import datetime
from base.base_gold_pipeline import BaseGoldPipeline

with open("src/ops_mart/config/fact.yaml") as f:
    cfg = yaml.full_load(f)['tables']['fact_kitchen']['dim_path']

class FactKitchen(BaseGoldPipeline):

    def build(self) -> pd.DataFrame:

        kitchen_logs = pd.read_parquet(self.silver_path['kitchen_logs'])
        order_items =  pd.read_parquet(self.silver_path['order_items'])
        orders =       pd.read_parquet(self.silver_path['orders'])

        fact = kitchen_logs.merge(order_items[['order_item_id', 'order_id', 'item_id']], on = 'order_item_id', how= 'left')
        fact = fact.merge(orders[[ 'order_id', 'restaurant_id', 'order_datetime']], on = 'order_id', how= 'left')
        
        fact['date_key'] = fact['order_datetime'].dt.strftime('%Y%m%d')

        items =             pd.read_parquet(cfg['items'])
        restaurants =       pd.read_parquet(cfg['restaurants'])
        chef =              pd.read_parquet(cfg['chef'])

        fact = fact.merge(items[['item_id', 'item_key']], on='item_id', how='left')
        fact = fact.merge(restaurants[['restaurant_id', 'restaurant_key']], on='restaurant_id', how='left')
        fact = fact.merge(chef[['chef_id', 'chef_key']], on='chef_id', how='left')

        fact['assigned_at'] =   fact['order_datetime'].dt.strftime('%H:%M:%S')
        fact['is_wasted'] =     fact['status'] == 'Wasted'
        fact['is_prepared'] =   fact['status'] == 'Prepared'

        fact = fact.rename(columns={'started_at':'prep_start_at', 'completed_at': 'prep_end_at', 'status': 'kitchen_status'})
    
        fact['kitchen_id'] =  fact.index + 1

        return fact[[
            'kitchen_id',
            'order_id',
            'order_item_id',

            'date_key',
            'restaurant_key',
            'chef_key',
            'item_key',

            'assigned_at',
            'prep_start_at',
            'prep_end_at',
            
            'prep_time_mins',
            'kitchen_status',

            'is_wasted',
            'is_prepared'
        ]]
    
