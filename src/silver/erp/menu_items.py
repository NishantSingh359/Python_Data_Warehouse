import yaml
import numpy as np
import pandas as pd
from common.common import clean_id, clean_text
from base.base_silver_pipeline import BaseSilverPipeline

with open("src/silver/config/erp.yaml") as f:
    cfg = yaml.safe_load(f)
    path = cfg['tables']['menu_items']

class Menu_itemsSilver(BaseSilverPipeline):

    def clean(self, df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:

        item_id =      clean_id(df['item_id'], 'I', 4)

        item_name =    clean_text(df['item_name']).str.title()

        category =     clean_text(df['category']).str.title()
        category =     category.where(category.isin(['Main', 'Beverage', 'Dessert', 'Starter']))

        cuisine  =     clean_text(df['cuisine']).str.title()
        cuisine =      cuisine.where(cuisine.isin(['Chinese', 'Indian', 'Italian']))

        is_veg =       pd.to_numeric(df['is_veg'], errors='coerce')
        is_veg =       is_veg.where(is_veg.isin([0,1]))

        sell_price =   pd.to_numeric(df['selling_price'], errors='coerce')
        sell_price =   sell_price.where((sell_price >= 50) & (sell_price <= 500))


        df1 = pd.DataFrame({
            'item_id':item_id,
            'item_name':item_name,
            'category':category,
            'cuisine':cuisine,
            'is_veg':is_veg,
            'selling_price':sell_price
        })

        ord_itm = pd.read_csv(path['order_items_path'])
        group = ord_itm[['item_id', 'unit_price']].groupby('item_id').median().reset_index()
        merge = df.merge(group, on = 'item_id', how='left')

        merge.loc[merge['selling_price'].isna(), 'selling_price'] = merge['unit_price']
        merge.drop('unit_price', axis=1, inplace=True)


        df2 = df1.dropna(subset='item_id')
        df3 = df2.drop_duplicates('item_id').sort_values('item_id').reset_index(drop=True)

        return df1, df2, df3
        
