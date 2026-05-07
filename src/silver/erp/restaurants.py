import numpy as np
import pandas as pd
from common.common import clean_id, clean_text
from base.base_silver_pipeline import BaseSilverPipeline

class RestaurantsSilver(BaseSilverPipeline):

    def clean(self, df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:

        restaurant_id =   clean_id(df['restaurant_id'], 'R', 3)

        restaurant_name = clean_text(df['restaurant_name']).str.title()

        city =            clean_text(df['city']).str.title()
        city =            city.where(city.isin(['Mumbai', 'Bangalore', 'Hyderabad', 'Delhi', 'Pune']), np.nan)

        restaurant_type = df['restaurant_type'].str.strip().str.title().replace({'':np.nan})

        open_date =       pd.to_datetime(df['open_date'], format= '%Y-%m-%d %H:%M:%S', errors= 'coerce')
        open_date =       (open_date).where((open_date >= '2021-01-01') & (open_date <= '2025-12-31')) #type:ignore

        closed_date =     pd.to_datetime(df['closed_date'], format= '%Y-%m-%d', errors= 'coerce')
        closed_date =     closed_date.where(closed_date>open_date)


        df1 = pd.DataFrame({
            'restaurant_id':restaurant_id,
            'restaurant_name':restaurant_name,
            'city':city,
            'restaurant_type':restaurant_type,
            'open_date':open_date,
            'closed_date':closed_date
        })

        df2 = df1.dropna(subset='restaurant_id')
        df3 = df2.drop_duplicates(subset='restaurant_id').sort_values(by='restaurant_id').reset_index(drop=True)

        return df1, df2, df3
