import pandas as pd
from base.base_gold_pipeline import BaseGoldPipeline

class DimOrder_status(BaseGoldPipeline):

    def build(self) -> pd.DataFrame:
        status_key = [1,2,3]
        order_status = ['completed', 'cancelled', 'failed']

        df = pd.DataFrame({
            'status_key':  status_key,
            'order_status':order_status
        })

        return df