import numpy as np
import pandas as pd
from base.base_silver_pipeline import BaseSilverPipeline

class Menu_itemsSilver(BaseSilverPipeline):

    def clean(self, menu: pd.DataFrame) -> pd.DataFrame:

        item_id =    menu['item_id'].str.replace(r'\D', '', regex=True).replace({'':np.nan})
        item_id =    pd.to_numeric(item_id, errors='coerce').replace(0, np.nan).astype('Int16')
        item_id =    item_id.fillna(menu['item_name'].replace(r'\D', '', regex=True).replace({'':np.nan}))
        item_id =    ('I' + item_id.astype(str).str.zfill(4)).where(item_id.notnull(), np.nan)

        item_name = item_id.str.replace(r'\D','', regex=True).astype('Int16')
        item_name = ('Menu_Item_' + item_name.astype(str)).where(item_name.notnull(), np.nan)

        category =   menu['category'].str.strip().replace({'nan':np.nan}).str.lower()
        cuisine =    menu['cuisine'].str.strip().replace({'nan':np.nan}).str.lower()

        selling_price = menu['selling_price'].astype(float)
        selling_price = selling_price.where(selling_price > 0, np.nan)

        df = pd.DataFrame({
            'item_id':item_id,
            'item_name':item_name,
            'category':category,
            'cuisine':cuisine,
            'selling_price':selling_price
        })

        return (
            df
            .dropna(subset=['item_id'])
            .drop_duplicates('item_id')
            .sort_values('item_id')
            .reset_index(drop=True)
        )
