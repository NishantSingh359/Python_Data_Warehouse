import pandas as pd
from base.base_gold_pipeline import BaseGoldPipeline

class DimMenu_items(BaseGoldPipeline):

    def build(self) -> pd.DataFrame:

        menu = pd.read_parquet(
            self.silver_path
        )

        menu['item_key'] = menu.index + 1

        item_key =  menu['item_key']
        item_id =   menu['item_id']
        item_name = menu['item_name']
        category =  menu['category']
        cuisine =   menu['cuisine']

        df = pd.DataFrame({
            'item_key':item_key,
            'item_id':item_id,
            'item_name':item_name,
            'category':category,
            'cuisine':cuisine
        })

        return df