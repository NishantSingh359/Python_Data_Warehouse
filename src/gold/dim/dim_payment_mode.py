import pandas as pd
from base.base_gold_pipeline import BaseGoldPipeline

class DimPayment_mode(BaseGoldPipeline):

    def build(self) -> pd.DataFrame:
        payment_key = [1,2,3,4]
        payment_mode = ['wallet', 'upi', 'cash', 'card']

        df = pd.DataFrame({
            'payment_key': payment_key,
            'payment_mode':payment_mode
        })

        return df