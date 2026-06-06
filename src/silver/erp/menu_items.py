import yaml
import numpy as np
import pandas as pd
from common.common import clean_id, clean_text
from base.base_silver_pipeline import BaseSilverPipeline

with open("src/silver/config/erp.yaml") as f:
    cfg = yaml.safe_load(f)
    path = cfg['tables']['menu_items']

class Menu_itemsSilver(BaseSilverPipeline):

    mapping = {1:'Veg', 0:'Non-Veg'}
    def clean(self, df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:

        item_id =         clean_id(df['item_id'], 'I', 4)

        item_name =       clean_text(df['item_name']).str.title()

        category =        clean_text(df['category']).str.title()
        category =        category.where(category.isin(['Main', 'Beverage', 'Dessert', 'Starter']))

        cuisine  =        clean_text(df['cuisine']).str.title()
        cuisine =         cuisine.where(cuisine.isin(['Chinese', 'Indian', 'Italian']))

        is_veg =          pd.to_numeric(df['is_veg'], errors='coerce')
        is_veg =          is_veg.where(is_veg.isin([0,1]))
        is_veg =          is_veg.map(self.mapping)

        selling_price =   pd.to_numeric(df['selling_price'], errors='coerce')
        selling_price =   selling_price.where((selling_price >= 50) & (selling_price <= 500))

        added_date =      pd.to_datetime(df['added_date'], format='%Y-%m-%d', errors='coerce')
        added_date =      added_date.where((added_date >= "2021-01-01") & (added_date <= "2025-12-31")) #type:ignore

        df1 = pd.DataFrame({
            'item_id':item_id,
            'item_name':item_name,
            'category':category,
            'cuisine':cuisine,
            'is_veg':is_veg,
            'selling_price':selling_price,
            'added_date':added_date
        })

        # FILLING NaN selling_price
        order_items =            pd.read_csv(path['order_items'])
        selling_price =          order_items.groupby('item_id')['unit_price'].median()
        merge =                  df1.merge(selling_price, how='left', on='item_id')
        df1['selling_price'] =   merge['selling_price'].fillna(merge['unit_price'])

        df2 = df1.dropna(subset='item_id')
        df3 = df2.drop_duplicates('item_id').sort_values('item_id').reset_index(drop=True)

        return df1, df2, df3
        
