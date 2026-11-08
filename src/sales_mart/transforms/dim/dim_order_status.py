import pandas as pd
from base.base_gold_pipeline import BaseGoldPipeline

class DimOrderStatus(BaseGoldPipeline):

    def build(self) -> pd.DataFrame:

        df = pd.read_parquet(
            self.silver_path
        )

        df = df[['order_status', 'cancel_stage', 'cancel_reason']]
        dup = df[['order_status', 'cancel_stage', 'cancel_reason']].duplicated()
        dup = ~dup
        df = df.iloc[dup]

        df['order_status_key'] = df.index + 1

        return (
            df[[
                'order_status_key',
                'order_status',
                'cancel_stage',
                'cancel_reason'
            ]].reset_index(drop=True)
        )