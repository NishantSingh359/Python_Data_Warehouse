import yaml
import numpy as np
import pandas as pd
from pathlib import Path
from base.base_silver_pipeline import BaseSilverPipeline

with open("src/silver/config/crm.yaml") as f:
    cfg = yaml.full_load(f)
    path = cfg['tables']['orders']

class OrdersSilver(BaseSilverPipeline):

    def clean(self, orde: pd.DataFrame) -> pd.DataFrame:

        order_id =       orde['order_id'].str.replace(r'\D', '', regex=True).replace({'':np.nan}).astype('Int32')
        order_id =       ('O' + order_id.astype(str).str.zfill(7)).where(order_id.notnull(), np.nan) 

        customers =      pd.read_parquet(path['customers_path'])
        customer_id =    orde['customer_id'].str.replace(r'\D', '', regex=True).replace({'':np.nan}).astype('Int32')
        customer_id =    ('C' + customer_id.astype(str).str.zfill(6)).where(customer_id.notnull(), np.nan)
        customer_id =    customer_id.where(customer_id.isin(customers['customer_id']), np.nan)

        restaurants =    pd.read_parquet(path['restaurants_path'])
        restaurant_id =  orde['restaurant_id'].str.replace(r'\D', '', regex=True).replace({'':np.nan}).astype('Int32')
        restaurant_id =  ('R' + restaurant_id.astype(str).str.zfill(3)).where(restaurant_id.notnull(), np.nan)
        restaurant_id =  restaurant_id.where(restaurant_id.isin(restaurants['restaurant_id']), np.nan)

        order_datetime = orde['order_datetime'].str.strip()
        order_datetime = pd.to_datetime(order_datetime, format= '%Y-%m-%d %H:%M:%S', errors='coerce')

        payment_mode =   orde['payment_mode'].str.strip().replace({'nan':np.nan}).str.lower()
        order_status =   orde['order_status'].str.strip().replace({'nan':np.nan}).str.lower()
        is_delivery =    orde['is_delivery'].str.strip().replace({'nan':np.nan}).str.title()

        partners =       pd.read_parquet(path['delivery_partners_path'])
        partner_id =     orde['delivery_partner_id'].str.replace(r'\D', '', regex=True).replace({'':np.nan}).astype('Int32')
        partner_id =     ('D' + partner_id.astype(str).str.zfill(4)).where(partner_id.notnull(), np.nan)
        partner_id =     partner_id.where(partner_id.isin(partners['delivery_partner_id']), np.nan)
        partner_id =     partner_id.mask((is_delivery == 'True') & (partner_id.isna()), 'UNKNOWN')

        df = pd.DataFrame({
            'order_id':order_id,
            'customer_id':customer_id,
            'restaurant_id':restaurant_id,
            'order_datetime':order_datetime,
            'payment_mode':payment_mode,
            'order_status':order_status,
            'is_delivery':is_delivery,
            'delivery_partner_id':partner_id
        })

        return (
            df
            .dropna(subset= 'order_id')
            .drop_duplicates(subset= 'order_id')
            .sort_values(by = 'order_id')
            .reset_index(drop=True)
        )