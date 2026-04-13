import numpy as np
import pandas as pd
import datetime
from pathlib import Path
from common.common import clean_id, clean_phone_n
from base.base_silver_pipeline import BaseSilverPipeline

class CustomersSilver(BaseSilverPipeline):

    def clean(self, df: pd.DataFrame) -> pd.DataFrame:

        customer_id =   clean_id(df['customer_id'], 'C', 6)

        customer_name = df['customer_name'].str.strip().str.title()

        city =          df['city'].str.strip().str.title()

        phone =         clean_phone_n(df['phone'])

        created_at =    pd.to_datetime(df['created_at'], format= '%Y-%m-%d %H:%M:%S', errors= 'coerce')
        created_at =    created_at.where((created_at>= '2018-01-01') & (created_at <= '2025-12-31'))

        df = pd.DataFrame({
            'customer_id':customer_id,
            'customer_name':customer_name,
            'city':city,
            'phone':phone,
            'created_at':created_at
        })

        return (
            df
            .dropna(subset= 'customer_id')
            .drop_duplicates(subset= 'customer_id')
            .sort_values(by = 'customer_id')
            .reset_index(drop=True)
        )