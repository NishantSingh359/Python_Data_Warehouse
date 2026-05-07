import numpy as np
import yaml
import pandas as pd
import datetime
from pathlib import Path
from common.common import clean_id, clean_text
from base.base_silver_pipeline import BaseSilverPipeline

with open("src/silver/config/crm.yaml") as f:
    cfg = yaml.full_load(f)
    path = cfg['tables']['refunds']

class RefundsSilver(BaseSilverPipeline):

    def clean(self, df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:

        refund_id =  'RF' + (df.index + 1).astype(str).str.zfill(7)

        orders =            pd.read_parquet(path['orders_path'])
        order_id =          clean_id(df['order_id'], 'O', 7)
        order_id =          order_id.where(order_id.isin(orders['order_id']))

        refund_reason =  clean_text(df['refund_reason'])

        refund_amount =  pd.to_numeric(df['refund_amount'], errors='coerce')
        refund_amount =  refund_amount.where((refund_amount > 0) & (refund_amount < 5000))

        refund_method =  clean_text(df['refund_method'])
        refund_method =  refund_method.where(refund_method.isin(['card', 'cash', 'upi', 'wallet']))

        refund_status =  clean_text(df['refund_status'])
        refund_status =  refund_status.where(refund_status.isin(['failed', 'processed', 'pending']))

        initiated_at  =  pd.to_datetime(df['initiated_at'], format='%Y-%m-%d %H:%M:%S', errors='coerce')

        processed_at  =  pd.to_datetime(df['processed_at'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
        processed_at =   processed_at.where(processed_at>initiated_at)

        df1 = pd.DataFrame({
            'refund_id':refund_id,
            'order_id':order_id,
            'refund_reason':refund_reason,
            'refund_amount':refund_amount,
            'refund_method':refund_method,
            'refund_status':refund_status,
            'initiated_at': initiated_at,
            'processed_at':processed_at
        })

        df2 = df1.dropna(subset=['order_id', 'refund_amount'])
        df3 = df2.drop_duplicates(subset='order_id').sort_values(by='order_id').reset_index(drop=True)

        return df1, df2, df3
        