import numpy as np
import pandas as pd
from common.common import clean_id, clean_phone_n
from base.base_silver_pipeline import BaseSilverPipeline

class Delivery_partnersSilver(BaseSilverPipeline):

    def clean(self, df: pd.DataFrame) -> pd.DataFrame:

        partner_id =    clean_id(df['delivery_partner_id'], 'D', 4)

        name =          df['name'].str.strip().str.title()

        partner_type =  df['partner_type'].str.strip().str.title()
        vehicle_type =  df['vehicle_type'].str.strip().str.title()

        phone =         clean_phone_n(df['phone'])


        df = pd.DataFrame({
            'delivery_partner_id':partner_id,
            'name':name,
            'partner_type':partner_type,
            'vehicle_type':vehicle_type,
            'phone':phone
        })

        return (
            df
            .dropna(subset='delivery_partner_id')
            .drop_duplicates(subset='delivery_partner_id')
            .sort_values(by ='delivery_partner_id')
            .reset_index(drop=True)
        )
