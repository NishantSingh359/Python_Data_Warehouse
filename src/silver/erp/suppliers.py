import numpy as np
import pandas as pd
from base.base_silver_pipeline import BaseSilverPipeline
from common.common import clean_id, clean_phone_n

class SuppliersSilver(BaseSilverPipeline):

    def clean(self, df: pd.DataFrame) -> pd.DataFrame:

        supplier_id =     clean_id(df['supplier_id'], 'S', 4)

        supplier_name =   df['supplier_name'].str.strip().str.title()

        city =            df['city'].str.strip().str.title()

        phone =           clean_phone_n(df['phone'])

        email =           supplier_name.str.lower().str.replace('_','')+'@mail.com'

        df = pd.DataFrame({
            'supplier_id': supplier_id,
            'supplier_name': supplier_name,
            'city': city,
            'phone': phone,
            'email': email
        })

        return (
            df
            .dropna(subset=['supplier_id'])
            .drop_duplicates('supplier_id')
            .sort_values('supplier_id')
            .reset_index(drop=True)
        )
