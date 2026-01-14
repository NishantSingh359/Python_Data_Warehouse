import pandas as pd
from base.base_gold_pipeline import BaseGoldPipeline

class DimEmployees(BaseGoldPipeline):

    def build(self) -> pd.DataFrame:

        emp = pd.read_parquet(self.silver_path)
                              
        emp['emp_key'] = emp.index + 1

        emp_key = emp['emp_key']
        emp_id =  emp['emp_id']
        role =    emp['role']

        df = pd.DataFrame({
            'emp_key':emp_key,
            'emp_id': emp_id,
            'role':   role
        })

        return df
