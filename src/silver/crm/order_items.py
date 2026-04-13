import yaml
import numpy as np
import pandas as pd
from pathlib import Path
from common.common import clean_id
from base.base_silver_pipeline import BaseSilverPipeline

with open("src/silver/config/crm.yaml") as f:
    cfg = yaml.full_load(f)
    path = cfg['tables']['order_items']

class Order_itemsSilver(BaseSilverPipeline):

    def clean(self, df: pd.DataFrame) -> pd.DataFrame:

        order_item_id = clean_id(df['order_item_id'], 'OI', 8)

        orders =        pd.read_parquet(path['orders_path'])
        order_id =      clean_id(df['order_id'], 'O', 7)
        order_id =      order_id.where(order_id.isin(orders['order_id']))

        menu =          pd.read_parquet(path['menu_items_path'])
        item_id =       clean_id(df['item_id'], 'I', 4)
        item_id =       item_id.where(item_id.isin(menu['item_id']))

        join =          df.merge(menu, on = 'item_id', how='left')
        unit_price =    join['unit_price'].fillna(join['selling_price'])

        quantity =      df['quantity']
        line_total =    df['line_total']

        df = pd.DataFrame({
            'order_item_id':order_item_id,
            'order_id':order_id,
            'item_id':item_id,
            'quantity':quantity,
            'unit_price':unit_price,
            'line_total':line_total
        })

        df['quantity'] =      df.loc[df['quantity'].isna(), 'quantity'] = df['line_total']/df['unit_price']
        df['line_total'] =    df.loc[df['line_total'].isna(), 'line_total'] = df['unit_price'] * df['quantity']

        return (
            df
            .dropna(subset=['order_item_id','order_id', 'item_id'])
            .drop_duplicates(subset='order_item_id')
            .sort_values(by='order_item_id')
            .reset_index(drop=True)
        )