import pandas as pd
from base.base_gold_pipeline import BaseGoldPipeline

class DimChef(BaseGoldPipeline):

    def build(self) -> pd.DataFrame:

        dim = pd.read_parquet(
            self.silver_path
        )

        dim  = pd.read_parquet(r"data/silver/erp/employees.parquet")
        dim = dim[dim['role'] == 'chef'].reset_index(drop=True)
        dim['chef_key'] = dim.index + 1
        dim = dim.rename(columns={'emp_id':'chef_id'})

        return dim[[
            'chef_key',
            'chef_id',
            'name',
            'restaurant_id'
        ]]