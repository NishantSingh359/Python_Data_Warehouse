import pandas as pd
from base.base_gold_pipeline import BaseGoldPipeline

class DimRestaurants(BaseGoldPipeline):

    def build(self) -> pd.DataFrame:

        dim = pd.read_parquet(
            self.silver_path
        )

        dim['restaurant_key'] = dim.index + 1

        return dim[[
            'restaurant_key',
            'restaurant_id',
            'restaurant_name',
            'city',
            'restaurant_type',
            'open_date'
        ]]
