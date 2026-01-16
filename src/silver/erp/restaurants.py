import numpy as np
import pandas as pd
from base.base_silver_pipeline import BaseSilverPipeline

class RestaurantsSilver(BaseSilverPipeline):

    def clean(self, df: pd.DataFrame) -> pd.DataFrame:

        restaurant_id =   df['restaurant_id'].str.replace(r'\D','',regex=True).replace({'':np.nan})
        restaurant_id =   pd.to_numeric(restaurant_id, errors='coerce').replace(0,np.nan).astype('Int16')
        restaurant_id =   restaurant_id.fillna(df['restaurant_name'].replace(r'\D','',regex=True).replace({'':np.nan}))
        restaurant_id =   ('R'+restaurant_id.astype(str).str.zfill(3)).where(restaurant_id.notnull(), np.nan)

        restaurant_name = restaurant_id.replace(r'\D','',regex=True).astype('Int16')
        restaurant_name = ('Restaurant_'+restaurant_name.astype(str)).where(restaurant_id.notnull(),np.nan)

        city =            df['city'].str.strip().str.lower()
        restaurant_type = df['restaurant_type'].str.strip().str.lower()
        open_date =       pd.to_datetime(df['open_date'], format= '%Y-%m-%d %H:%M:%S', errors= 'coerce')

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
