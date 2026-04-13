import pandas as pd
from base.base_gold_pipeline import BaseGoldPipeline

class DimMenu_items(BaseGoldPipeline):

    def build(self) -> pd.DataFrame:

        dim = pd.read_parquet(
            self.silver_path
        )

        dim['item_key'] = dim.index + 1

        return dim[[
            'item_key',
            'item_id',
            'item_name',
            'category',
            'cuisine',
            'selling_price'
        ]]