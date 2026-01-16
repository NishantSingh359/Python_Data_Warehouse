import yaml
import numpy as np
import pandas as pd
from pathlib import Path
from base.base_silver_pipeline import BaseSilverPipeline

with open("src/silver/config/erp.yaml") as f:
    cfg = yaml.full_load(f)
    path = cfg['tables']['employees']

class EmployeesSilver(BaseSilverPipeline):

    def clean(self, df: pd.DataFrame) -> pd.DataFrame:

        emp_id =        df['emp_id'].str.replace(r'\D', '', regex=True).astype('Int16')
        emp_id =        ('E' + emp_id.astype(str).str.zfill(5)).where(emp_id.notnull() == True, np.nan) 

        restaurants =   pd.read_parquet(path['restaurants_path'])
        restaurant_id = pd.to_numeric(df['restaurant_id'].str.replace(r'\D', '', regex=True), errors= 'coerce')
        restaurant_id = ('R' + restaurant_id.astype('Int16').astype(str).str.zfill(3)).where(restaurant_id.notnull() == True, np.nan)
        restaurant_id = restaurant_id.where(restaurant_id.isin(restaurants['restaurant_id']), np.nan)

        role =          df['role'].str.strip().replace({'nan':np.nan})

        hire_date =     df['hire_date'].str.strip()
        hire_date =     pd.to_datetime(hire_date, format= '%Y-%m-%d %H:%M:%S', errors= 'coerce')

        salary =        pd.to_numeric(df['salary'], errors= 'coerce').astype('Int64')
        salary =        salary.where(salary > 0, np.nan)

        df = pd.DataFrame({
            'emp_id':emp_id,
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
