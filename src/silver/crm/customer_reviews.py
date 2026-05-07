import yaml
import numpy as np
import pandas as pd
import datetime
from pathlib import Path
from common.common import clean_id, clean_text
from base.base_silver_pipeline import BaseSilverPipeline

with open("src/silver/config/crm.yaml") as f:
    cfg = yaml.full_load(f)
    path = cfg['tables']['customer_reviews']

class Customer_reviewsSilver(BaseSilverPipeline):

    def clean(self, df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:

        review_id =        'RV' + (df.index + 1).astype(str).str.zfill(7)

        orders =            pd.read_parquet(path['orders_path'])
        order_id =          clean_id(df['order_id'], 'O', 7)
        order_id =          order_id.where(order_id.isin(orders['order_id']))

        rating =            pd.to_numeric(df['rating'], errors= 'coerce').abs()
        rating =            rating.where((rating >= 0) & (rating <=5))

        food_rating =       pd.to_numeric(df['food_rating'], errors= 'coerce').abs()
        food_rating =       food_rating.where((food_rating >= 0) & (food_rating <=5))

        delivery_rating =   pd.to_numeric(df['delivery_rating'], errors= 'coerce').abs()
        delivery_rating =   food_rating.where((delivery_rating >= 0) & (delivery_rating <= 5))

        review_text =       clean_text(df['review_text'])

        created_at =        df['created_at'].astype(str).str.strip()
        created_at =        pd.to_datetime(created_at, format= '%Y-%m-%d %H:%M:%S', errors='coerce')
        created_at =        created_at.where((created_at>= '2021-01-01') & (created_at <= '2025-12-31')) #type:ignore

        df1 = pd.DataFrame({
            'review_id':review_id,
            'order_id':order_id,
            'rating':rating,
            'food_rating':food_rating,
            'delivery_rating':delivery_rating,
            'review_text':review_text,
            'created_at':created_at
        })

        df2 = df1.dropna(subset= 'order_id')
        df3 = df2.drop_duplicates(subset= 'order_id').sort_values(by = 'review_id').reset_index(drop=True)

        return df1, df2, df3