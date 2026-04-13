import yaml
import numpy as np
import pandas as pd
from pathlib import Path
from common.common import clean_id
from base.base_silver_pipeline import BaseSilverPipeline

with open("src/silver/config/crm.yaml") as f:
    cfg = yaml.full_load(f)
    path = cfg['tables']['orders']

GUEST = 'guest'
UNKNOWN = 'unknown'
NOT_ASSIGNED = 'not_assigned'

class OrdersSilver(BaseSilverPipeline):

    def clean(self, df: pd.DataFrame) -> pd.DataFrame:

        order_id =       clean_id(df['order_id'], 'O', 7)

        customers =      pd.read_parquet(path['customers_path'])
        customer_id =    clean_id(df['customer_id'], 'C', 6)
        customer_id =    customer_id.where(customer_id.isin(customers['customer_id']), GUEST)

        restaurants =    pd.read_parquet(path['restaurants_path'])
        restaurant_id =  clean_id(df['restaurant_id'], 'R', 3)
        restaurant_id =  restaurant_id.where(restaurant_id.isin(restaurants['restaurant_id']))

        order_datetime = pd.to_datetime(df['order_datetime'], format= '%Y-%m-%d %H:%M:%S', errors='coerce')
        order_datetime = order_datetime.where((order_datetime >= '2021-01-01') & (order_datetime <= "2025-12-31"))

        payment_mode =   df['payment_mode'].str.strip().replace({'nan':np.nan}).str.lower()
        order_status =   df['order_status'].str.strip().replace({'nan':np.nan}).str.lower()

        cancel_stage =   df['cancel_stage'].str.strip().replace({'nan':np.nan}).str.lower()
        cancel_stage =   cancel_stage.fillna('not_cancelled')

        cancel_reason =  df['cancel_reason'].str.strip().replace({'nan':np.nan}).str.lower()
        cancel_reason =  cancel_reason.fillna("not_cancelled")

        partners =       pd.read_parquet(path['delivery_partners_path'])
        partner_id =     clean_id(df['delivery_partner_id'], 'D', 4)
        partner_id =     partner_id.where(partner_id.isin(partners['delivery_partner_id']), np.nan)
        partner_id =     partner_id.fillna(NOT_ASSIGNED)

        df = pd.DataFrame({
            'order_id':order_id,
            'customer_id':customer_id,
            'restaurant_id':restaurant_id,
            'order_datetime':order_datetime,
            'payment_mode':payment_mode,
            'order_status':order_status,
            'cancel_stage':cancel_stage,
            'cancel_reason':cancel_reason,
            'delivery_partner_id':partner_id
        })

        df.loc[df['order_status'] == 'failed', "payment_mode"] = UNKNOWN
        
        return (
            df
            .dropna(subset= ['order_id', 'restaurant_id', 'order_status'])
            .drop_duplicates(subset= 'order_id')
            .sort_values(by = 'order_id')
            .reset_index(drop=True)
        )