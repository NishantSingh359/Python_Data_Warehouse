import pandas as pd
from base.base_gold_pipeline import BaseGoldPipeline

class DimRestaurants(BaseGoldPipeline):

    def build(self) -> pd.DataFrame:

        res = pd.read_parquet(
            self.silver_path
        )

        res['restaurant_key'] = res.index + 1

        restaurant_key =  res['restaurant_key']
        restaurant_id =   res['restaurant_id']
        restaurant_name = res['restaurant_name']
        city =            res['city']
        restaurant_type = res['restaurant_type']
        open_date =       res['open_date']

        df = pd.DataFrame({
            'restaurant_key':restaurant_key,
            'restaurant_id':restaurant_id,
            'restaurant_name':restaurant_name,
            'city':city,
            'restaurant_type':restaurant_type,
            'open_date':open_date
        })

        return df
