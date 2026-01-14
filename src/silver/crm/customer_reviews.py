import yaml
import numpy as np
import pandas as pd
from pathlib import Path
from base.base_silver_pipeline import BaseSilverPipeline

with open("src/silver/config/crm.yaml") as f:
    cfg = yaml.full_load(f)
    path = cfg['tables']['customer_reviews']

class Customer_reviewsSilver(BaseSilverPipeline):

    def clean(self, rev: pd.DataFrame) -> pd.DataFrame:

        orders =      pd.read_parquet(path['orders_path'])
        order_id =    rev['order_id'].str.replace(r'\D','',regex=True).replace({'':np.nan})
        order_id =    order_id.fillna(rev['review_id'].str.replace(r'\D','',regex=True).replace({'':np.nan}))
        order_id =    ('O'+ order_id.astype('Int32').astype(str).str.zfill(7)).where(order_id.notnull(), np.nan)
        order_id =    order_id.where(order_id.isin(orders['order_id']), np.nan)

        review_id =   order_id.str.replace(r'\D','',regex=True).replace({'':np.nan})
        review_id =   ('RV_O'+review_id).where(review_id.notnull() == True, np.nan)

        rating =      pd.to_numeric(rev['rating'].str.strip().replace({'nan':np.nan}), errors= 'coerce').astype('Int16')
        review_text = rev['review_text'].str.strip().replace({'nan':np.nan,'':np.nan}).str.lower()

        created_at =  rev['created_at'].str.strip()
        created_at =  pd.to_datetime(created_at, format= '%Y-%m-%d %H:%M:%S.%f', errors='coerce')

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