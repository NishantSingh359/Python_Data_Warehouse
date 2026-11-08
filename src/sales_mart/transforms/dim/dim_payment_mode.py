import pandas as pd
from base.base_gold_pipeline import BaseGoldPipeline

class DimPaymentMode(BaseGoldPipeline):

    def build(self) -> pd.DataFrame:

        df = pd.read_parquet(
            self.silver_path
        )

        df = df[['payment_mode']]
        dup = df[['payment_mode']].duplicated()
        dup = ~dup
        df = df.iloc[dup]

        df['payment_mode_key'] = df.index + 1

        return (
            df[[
                'payment_mode_key',
                'payment_mode'
            ]].reset_index(drop=True)
        )