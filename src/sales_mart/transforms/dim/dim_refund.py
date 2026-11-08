import pandas as pd
from base.base_gold_pipeline import BaseGoldPipeline

class DimRefund(BaseGoldPipeline):

    def build(self) -> pd.DataFrame:

        dim = pd.read_parquet(
            self.silver_path
        )

        dim['refund_key'] = dim.index + 1

        return (
            dim[[
                'refund_key',
                'order_id',
                'refund_reason',
                'refund_amount',
                'refund_method',
                'refund_status',
                'initiated_at',
                'processed_at'
            ]]
        )