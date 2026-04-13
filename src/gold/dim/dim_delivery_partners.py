import pandas as pd
from base.base_gold_pipeline import BaseGoldPipeline

class DimDelivery_partners(BaseGoldPipeline):

    def build(self) -> pd.DataFrame:

        dim = pd.read_parquet(
            self.silver_path
        )

        dim['delivery_partner_key'] = dim.index + 1

        return dim[[
            'delivery_partner_key',
            'delivery_partner_id',
            'name',
            'partner_type',
            'vehicle_type'
        ]]