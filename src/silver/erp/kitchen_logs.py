import yaml
import numpy as np
import pandas as pd
import datetime
from pathlib import Path
from common.common import clean_id, completed_order, wasted, prepared
from base.base_silver_pipeline import BaseSilverPipeline

with open("src/silver/config/erp.yaml") as f:
    cfg = yaml.full_load(f)
    path = cfg['tables']['kitchen_logs']

class Kitchen_logsSilver(BaseSilverPipeline):

    def clean(self, df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:

        kitchen_log_id = clean_id(df['kitchen_log_id'], 'K', 8)

        order_item_id =  clean_id(df['order_item_id'], 'OI', 8)
        order_item_id =  order_item_id.where(order_item_id.isin(completed_order()))

        emp =            pd.read_parquet(path['employees_path'])
        chef =           emp[emp['role'] == 'chef']['emp_id']
        chef_id =        clean_id(df['chef_id'], 'E', 5)
        chef_id =        chef_id.where(chef_id.isin(chef), 'UNKNOWN')

        started_at =     pd.to_datetime(df['started_at'], format = '%Y-%m-%d %H:%M:%S', errors='coerce')
        started_at =     started_at.where((started_at>= '2021-01-01') & (started_at <= '2025-12-31')) #type:ignore

        completed_at =   pd.to_datetime(df['completed_at'], format = '%Y-%m-%d %H:%M:%S', errors='coerce')
        completed_at =   completed_at.where((completed_at >= '2021-01-01') & (completed_at <= '2025-12-31')) #type:ignore
        completed_at =   completed_at.where(completed_at > started_at)

        status =         df['status'].str.strip().replace({'nan':np.nan}).str.replace('@','').str.title()

        prep_time_mins =      (completed_at - started_at).dt.total_seconds() / 60

        df = pd.DataFrame({
            'kitchen_log_id': kitchen_log_id,
            'order_item_id':order_item_id,
            'chef_id':chef_id,
            'started_at':started_at,
            'completed_at':completed_at,
            'status':status,
            'prep_time_mins': prep_time_mins
        })

        df.loc[df['order_item_id'].isin(wasted()), 'status'] = 'Wasted'
        df.loc[df['order_item_id'].isin(prepared()),'status'] = 'Prepared'

        df2 = df.dropna(subset=['order_item_id'])
        df3 = df2.drop_duplicates(subset='order_item_id').sort_values(by = 'order_item_id').reset_index(drop=True)

        return df, df2, df3