import yaml
import numpy as np
import pandas as pd
from pathlib import Path
from common.common import clean_id
from base.base_silver_pipeline import BaseSilverPipeline

with open("src/silver/config/crm.yaml") as f:
    cfg = yaml.full_load(f)
    path = cfg['tables']['delivery_logs']

UNKNOWN = 'UNKNOWN'

class Delivery_logsSilver(BaseSilverPipeline):

    def clean(self, df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:

        delivery_id =          clean_id(df['delivery_id'], 'DLOG', 8)

        orders =                pd.read_parquet(path['orders_path'])
        order_id =              clean_id(df['order_id'], 'O', 7)
        order_id =              order_id.where(order_id.isin(orders['order_id']))

        delivery_partners =     pd.read_parquet(path['delivery_partners_path'])
        deli_partner_id =       clean_id(df['delivery_partner_id'], 'D', 4)
        deli_partner_id =       deli_partner_id.where(deli_partner_id.isin(delivery_partners['delivery_partner_id']), UNKNOWN)

        assigned_at =           df['assigned_at'].astype(str).str.strip()
        assigned_at =           pd.to_datetime(assigned_at, format= '%Y-%m-%d %H:%M:%S', errors='coerce')
        assigned_at =           assigned_at.where((assigned_at >=  '2021-01-01') & (assigned_at <= '2025-12-31')) #type:ignore  

        picked_at =             df['picked_at'].astype(str).str.strip()
        picked_at =             pd.to_datetime(picked_at, format= '%Y-%m-%d %H:%M:%S', errors='coerce')
        picked_at =             picked_at.where(picked_at > assigned_at)

        delivered_at =          df['delivered_at'].astype(str).str.strip()
        delivered_at =          pd.to_datetime(delivered_at, format= '%Y-%m-%d %H:%M:%S', errors='coerce')
        delivered_at =          delivered_at.where(delivered_at > picked_at)

        assign_to_pick_mins =   (picked_at - assigned_at).dt.total_seconds() / 60

        delivery_time_mins =    (delivered_at - picked_at).dt.total_seconds() / 60
 
        total_delivery_mins =   (delivered_at - assigned_at).dt.total_seconds() / 60


        df1 = pd.DataFrame({
            'delivery_id': delivery_id,
            'order_id': order_id,
            'delivery_partner_id': deli_partner_id,
            'assigned_at': assigned_at,
            'picked_at': picked_at,
            'delivered_at': delivered_at,
            'assign_to_pick_mins': assign_to_pick_mins,
            'delivery_time_mins': delivery_time_mins,
            'total_delivery_mins': total_delivery_mins
        })

        df2 = df1.dropna(subset= 'order_id')
        df3 = df2.drop_duplicates(subset= 'order_id').sort_values(by = 'delivery_id').reset_index(drop=True)

        return df1, df2, df3