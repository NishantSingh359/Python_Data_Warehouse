import yaml
import numpy as np
import pandas as pd
import datetime
from pathlib import Path
from common.common import clean_id
from base.base_silver_pipeline import BaseSilverPipeline

with open("src/silver/config/erp.yaml") as f:
    cfg = yaml.full_load(f)
    path = cfg['tables']['employees']

class EmployeesSilver(BaseSilverPipeline):

    def clean(self, df: pd.DataFrame) -> pd.DataFrame:

        emp_id =        clean_id(df['emp_id'], 'E', 5)

        name =          df['name'].str.strip().str.title() 

        restaurants =   pd.read_parquet(path['restaurants_path'])
        restaurant_id = clean_id(df['restaurant_id'], 'R', 3)
        restaurant_id = restaurant_id.where(restaurant_id.isin(restaurants['restaurant_id']))

        role =          df['role'].str.strip().str.title()

        hire_date =     pd.to_datetime(df['hire_date'], format= '%Y-%m-%d %H:%M:%S', errors= 'coerce')
        hire_date =     hire_date.where((hire_date >= '2021-01-01') & (hire_date <= '2025-12-31'))

        salary =        df['salary'].where((df['salary'] > 0) & (df['salary']  < 500000), np.nan)

        df = pd.DataFrame({
            'emp_id':emp_id,
            'name':name,
            'restaurant_id':restaurant_id,
            'role':role,
            'hire_date':hire_date,
            'salary':salary
        })

        return (
            df
            .dropna(subset= 'emp_id')
            .drop_duplicates(subset= 'emp_id')
            .sort_values(by = 'emp_id')
            .reset_index(drop=True)
        )
