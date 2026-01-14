import yaml
import numpy as np
import pandas as pd
from pathlib import Path
from base.base_silver_pipeline import BaseSilverPipeline

with open("src/silver/config/crm.yaml") as f:
    cfg = yaml.full_load(f)
    path = cfg['tables']['order_items']

class Order_itemsSilver(BaseSilverPipeline):

    def clean(self, ord_itm: pd.DataFrame) -> pd.DataFrame:

        order_item_id = ord_itm['order_item_id'].str.replace(r'\D','', regex=True).replace({'':np.nan,'nan':np.nan})
        order_item_id = ('OI'+order_item_id.astype('Int32').astype(str).str.zfill(8)).where(order_item_id.notnull() == True, np.nan)

        orders =        pd.read_parquet(path['orders_path'])
        order_id =      ord_itm['order_id'].replace(r'\D','',regex=True).replace({'':np.nan,'nan':np.nan})
        order_id =      ('O'+order_id.astype('Int32').astype(str).str.zfill(7)).where(order_id.notnull() == True, np.nan)
        order_id =      order_id.where(order_id.isin(orders['order_id']), np.nan)

        menu =          pd.read_parquet(path['menu_items_path'])
        item_id =       ord_itm['item_id'].str.strip().replace(r'\D','',regex=True).replace({'':np.nan,'nan':np.nan})
        item_id =       ('I'+item_id.astype('Int32').astype(str).str.zfill(4)).where(item_id.notnull() == True, np.nan)
        item_id['item_id'] = item_id.where(item_id.isin(menu['item_id']), np.nan)

        # item_id mapping (dictionary)
        unit_price =  pd.to_numeric(ord_itm['unit_price'].str.strip().str.replace('-','').replace({'nan':np.nan}), errors= 'coerce')
        ord_itm['unit_price'] =  unit_price.where((unit_price > 0) & (unit_price < 1000), np.nan)

        price_counts =           menu.groupby('selling_price')['item_id'].nunique()
        unique_price_menu =      menu.loc[menu['selling_price'].isin(price_counts[price_counts == 1].index),['item_id', 'selling_price']]

        # Fill NaN item_id 
        merg =    ord_itm.merge(unique_price_menu, left_on='unit_price', right_on='selling_price', how='left', suffixes=('', '_menu'))
        item_id['item_id'] = merg['item_id'].fillna(merg['item_id_menu'])
        item_id = item_id['item_id']

        quantity =   pd.to_numeric(ord_itm['quantity'].str.strip().str.replace('-','').replace({'nan':np.nan}), errors= 'coerce').astype('Int32')
        quantity =   quantity.where((quantity > 0) & (quantity < 50), np.nan)
        merg =       ord_itm.merge(menu[['item_id', 'selling_price']], on='item_id', how='left')
        unit_price = merg['unit_price'].fillna(merg['selling_price'])
        line_total = pd.to_numeric(ord_itm['line_total'].str.strip().str.replace('-','').replace({'nan':np.nan}), errors= 'coerce')
        line_total = line_total.where((line_total > 0) & (line_total < 1000), np.nan)

        merg =       ord_itm.merge(menu[['item_id', 'selling_price']], on='item_id', how='left')
        unit_price = merg['unit_price'].fillna(merg['selling_price'])

        mask = unit_price.isna() & quantity.gt(0)
        unit_price.loc[mask] = (line_total.loc[mask] / quantity.loc[mask]).round(2).astype('float64')

        mask = quantity.isna() & unit_price.gt(0)
        tmp = (line_total.loc[mask] / unit_price.loc[mask]).round()
        quantity.loc[mask] = tmp.astype('Int32')

        line_total =  line_total.fillna(unit_price*quantity)

        df = pd.DataFrame({
            'order_item_id':order_item_id,
            'order_id':order_id,
            'item_id':item_id,
            'quantity':quantity,
            'unit_price':unit_price,
            'line_total':line_total
        })

        mask = (df['unit_price'] * df['quantity']) != df['line_total']
        df.loc[mask,'line_total'] = (df.loc[mask,'quantity'] * df.loc[mask,'unit_price']).round(2).astype('float64')

        df = df.dropna(subset = 'order_id')

        return (
            df
            .dropna(subset=['order_item_id','order_id'])
            .drop_duplicates(subset='order_item_id')
            .sort_values(by='order_item_id')
            .reset_index(drop=True)
        )