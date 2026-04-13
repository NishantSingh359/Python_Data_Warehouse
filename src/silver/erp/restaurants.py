import numpy as np
import pandas as pd
from common.common import clean_id
from base.base_silver_pipeline import BaseSilverPipeline

class RestaurantsSilver(BaseSilverPipeline):

    def clean(self, df: pd.DataFrame) -> pd.DataFrame:

        restaurant_id =   clean_id(df['restaurant_id'], 'R', 3)

        restaurant_name = df['restaurant_name'].str.strip().replace({'':np.nan})

        city =            df['city'].str.strip().str.title()
        restaurant_type = df['restaurant_type'].str.strip().str.title()
        open_date =       pd.to_datetime(df['open_date'], format= '%Y-%m-%d %H:%M:%S', errors= 'coerce')
        open_date =       (open_date).where((open_date >= '2018-01-01') & (open_date < '2050-01-01'))

        df = pd.DataFrame({
            'restaurant_id':restaurant_id,
            'restaurant_name':restaurant_name,
            'city':city,
            'restaurant_type':restaurant_type,
            'open_date':open_date
        })

        return (
            df
             .dropna(subset='restaurant_id')
             .drop_duplicates(subset='restaurant_id')
             .sort_values(by='restaurant_id')
             .reset_index(drop=True)
        )
