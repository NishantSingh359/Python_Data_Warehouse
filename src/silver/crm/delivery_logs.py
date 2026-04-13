import yaml
import numpy as np
import pandas as pd
from pathlib import Path
from common.common import clean_id
from base.base_silver_pipeline import BaseSilverPipeline

with open("src/silver/config/crm.yaml") as f:
    cfg = yaml.full_load(f)
    path = cfg['tables']['delivery_logs']

class Delivery_logsSilver(BaseSilverPipeline):

    def clean(self, df: pd.DataFrame) -> pd.DataFrame:

        delivery_id =         clean_id(df['delivery_id'], 'DLOG', 8)

        orders =              pd.read_parquet(path['orders_path'])
        order_id =            clean_id(df['order_id'], 'O', 7)
        order_id =            order_id.where(order_id.isin(orders['order_id']))

        delivery_partners =   pd.read_parquet(path['delivery_partners_path'])
        deli_partner_id =     clean_id(df['delivery_partner_id'], 'D', 4)
        deli_partner_id =     deli_partner_id.where(deli_partner_id.isin(delivery_partners['delivery_partner_id']))

        assigned_at =         pd.to_datetime(df['assigned_at'], format= '%Y-%m-%d %H:%M:%S', errors='coerce')

        picked_at =           pd.to_datetime(df['picked_at'], format= '%Y-%m-%d %H:%M:%S', errors='coerce')
        picked_at =           picked_at.where(picked_at > assigned_at)

        delivered_at =        pd.to_datetime(df['delivered_at'], format= '%Y-%m-%d %H:%M:%S', errors='coerce')
        delivered_at =        delivered_at.where(delivered_at > picked_at)

        assign_to_pick_mins = (picked_at - assigned_at).dt.total_seconds() / 60
    
        delivery_time_mins =  (delivered_at - picked_at).dt.total_seconds() / 60

        total_delivery_mins = (delivered_at - assigned_at).dt.total_seconds() / 60

        df = pd.DataFrame({
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

        df["delivery_status"] = np.select(
            [
                df["delivered_at"].notna(),
                df["picked_at"].notna() & df["delivered_at"].isna(),
                df["assigned_at"].notna() & df["picked_at"].isna(),
            ],
            [
                "delivered",
                "in_transit",
                "assigned"
            ],
            default="not_delivered"
        )

        return (
            df
            .dropna(subset= ['delivery_id', 'assigned_at'])
            .drop_duplicates(subset= 'delivery_id')
            .sort_values(by = 'delivery_id')
            .reset_index(drop=True)
        )