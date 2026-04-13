import pandas as pd
from base.base_gold_pipeline import BaseGoldPipeline

class DimCustomers(BaseGoldPipeline):

    def build(self) -> pd.DataFrame:

        dim = pd.read_parquet(
            self.silver_path
        )

        dim['customer_key'] = dim.index + 1

        return (
            dim[[
                'customer_key',
                'customer_id',
                'customer_name',
                'city',
                'created_at'
            ]]
        )