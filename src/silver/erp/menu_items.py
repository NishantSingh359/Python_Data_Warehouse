import yaml
import numpy as np
import pandas as pd
from common.common import clean_id
from base.base_silver_pipeline import BaseSilverPipeline

with open("src/silver/config/erp.yaml") as f:
    cfg = yaml.safe_load(f)
    path = cfg['tables']['menu_items']

class Menu_itemsSilver(BaseSilverPipeline):

    def clean(self, df: pd.DataFrame) -> pd.DataFrame:

        item_id =    clean_id(df['item_id'], 'I', 4)

        item_name =  df['item_name'].str.strip().replace({'':np.nan})

        category =   df['category'].str.strip().str.title()

        cuisine  =   df['cuisine'].str.strip().str.title()

        sell_price = df['selling_price'].where((df['selling_price'] >= 50) & (df['selling_price'] <= 500))

        df = pd.DataFrame({
            'item_id':item_id,
            'item_name':item_name,
            'category':category,
            'cuisine':cuisine,
            'selling_price':sell_price
        })

        ord_itm =     pd.read_csv(r'C:\Users\TUF\OneDrive\Documents\Code\Vs Code\Python_Data_Warehouse\data\raw\crm\order_items.csv.gz')
        group =       ord_itm.groupby('item_id')['unit_price'].median().reset_index()
        join =        df.merge(group, on='item_id', how='left')
        df['selling_price'] = join['selling_price'].fillna(join['unit_price'])

        return (
            df
            .dropna(subset=['item_id'])
            .drop_duplicates('item_id')
            .sort_values('item_id')
            .reset_index(drop=True)
        )
