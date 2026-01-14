import pandas as pd
from base.base_gold_pipeline import BaseGoldPipeline

class DimCustomers(BaseGoldPipeline):

    def build(self) -> pd.DataFrame:

        cust = pd.read_parquet(
            self.silver_path
        )

        cust['customer_key'] = cust.index + 1

        customer_key = cust['customer_key']
        customer_id = cust['customer_id']
        city = cust['city']
        created_at = cust['created_at']

        df = pd.DataFrame({
            'customer_key':customer_key,
            'customer_id':customer_id,
            'city':city,
            'created_at':created_at
        })

        return df