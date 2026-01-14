import pandas as pd
from base.base_gold_pipeline import BaseGoldPipeline

class DimDelivery_partners(BaseGoldPipeline):

    def build(self) -> pd.DataFrame:

        del_part = pd.read_parquet(
            self.silver_path
        )

        del_part['delivery_partner_key'] = del_part.index + 1

        delivery_partner_key = del_part['delivery_partner_key']
        delivery_partner_id = del_part['delivery_partner_id']
        partner_type = del_part['partner_type']
        vehicle_type = del_part['vehicle_type']

        df = pd.DataFrame({
            'delivery_partner_key':delivery_partner_key,
            'delivery_partner_id':delivery_partner_id,
            'partner_type':partner_type,
            'vehicle_type':vehicle_type
        })

        return df