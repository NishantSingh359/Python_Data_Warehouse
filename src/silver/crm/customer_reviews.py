import yaml
import numpy as np
import pandas as pd
import datetime
from pathlib import Path
from common.common import clean_id, clean_phone_n 
from base.base_silver_pipeline import BaseSilverPipeline

with open("src/silver/config/crm.yaml") as f:
    cfg = yaml.full_load(f)
    path = cfg['tables']['customer_reviews']

class Customer_reviewsSilver(BaseSilverPipeline):

    def clean(self, df: pd.DataFrame) -> pd.DataFrame:

        review_id =    clean_id(df['review_id'], 'RV', 5)

        orders =      pd.read_parquet(path['orders_path'])
        order_id =    clean_id(df['order_id'], 'O', 7)
        order_id =    order_id.where(order_id.isin(orders['order_id']))

        rating =      df['rating']
        review_text = df['review_text'].str.strip().replace({'nan':np.nan,'':np.nan}).str.title()

        created_at =  pd.to_datetime(df['created_at'], format= '%Y-%m-%d %H:%M:%S.%f', errors='coerce')
        created_at =  created_at.where((created_at>= '2018-01-01') & (created_at <= '2025-12-31'))

        df = pd.DataFrame({
            'review_id':review_id,
            'order_id':order_id,
            'rating':rating,
            'review_text':review_text,
            'created_at':created_at
        })

        return (
            df
            .dropna(subset= 'review_id')
            .drop_duplicates(subset= 'review_id')
            .sort_values(by = 'review_id')
            .reset_index(drop=True)
        )