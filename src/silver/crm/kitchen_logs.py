import yaml
import numpy as np
import pandas as pd
from pathlib import Path
from base.base_silver_pipeline import BaseSilverPipeline

with open("src/silver/config/crm.yaml") as f:
    cfg = yaml.full_load(f)
    path = cfg['tables']['kitchen_logs']

class Kitchen_logsSilver(BaseSilverPipeline):

    def clean(self, kic: pd.DataFrame) -> pd.DataFrame:

        order_item =     pd.read_parquet(path['order_items_path'])
        order_item_id =  kic['order_item_id'].str.replace(r'\D','',regex=True).replace({'':np.nan,'nan':np.nan})
        order_item_id =  pd.to_numeric(order_item_id, errors= 'coerce').astype('Int64')
        order_item_id =  ('OI'+order_item_id.astype(str).str.zfill(8)).where(order_item_id.notnull(),np.nan)
        order_item_id =  order_item_id.where(order_item_id.isin(order_item['order_item_id']), np.nan)

        emp =            pd.read_parquet(path['employees_path'])
        chef =           emp[emp['role'] == 'chef']['emp_id']
        chef_id =        kic['chef_id'].str.replace(r'\D','',regex=True).replace({'':np.nan,'nan':np.nan})
        chef_id =        pd.to_numeric(chef_id, errors= 'coerce').astype('Int64')
        chef_id =        ('E'+chef_id.astype(str).str.zfill(5)).where(chef_id.notnull(),np.nan)
        chef_id =        chef_id.where(chef_id.isin(chef), np.nan)

        started_at =     kic['started_at'].str.strip()
        started_at =     pd.to_datetime(started_at, format = '%Y-%m-%d %H:%M:%S', errors='coerce')

        completed_at =   kic['completed_at'].str.strip()
        completed_at =   pd.to_datetime(completed_at, format = '%Y-%m-%d %H:%M:%S', errors='coerce')

        status =         kic['status'].str.strip().replace({'nan':np.nan,'':np.nan}).str.lower()

        df = pd.DataFrame({
            'order_item_id':order_item_id,
            'chef_id':chef_id,
            'started_at':started_at,
            'completed_at':completed_at,
            'status':status
        })

        df = df.dropna(subset='order_item_id').drop_duplicates(subset='order_item_id').sort_values(by='order_item_id').reset_index(drop=True)
        df.insert(0,'kitchen_log_id','K'+(df.index+1).astype(str).str.zfill(8))

        return df