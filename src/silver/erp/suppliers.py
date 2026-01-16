import numpy as np
import pandas as pd
from base.base_silver_pipeline import BaseSilverPipeline

class SuppliersSilver(BaseSilverPipeline):

    def clean(self, df: pd.DataFrame) -> pd.DataFrame:

        supplier_id =   df['supplier_id'].str.replace(r'\D', '', regex=True).replace({'':np.nan})
        supplier_id =   pd.to_numeric(supplier_id, errors= 'coerce').replace(0,np.nan).astype('Int16')
        supplier_id =   supplier_id.fillna(df['supplier_name'].replace(r'\D', '', regex=True).replace({'':np.nan}))
        supplier_id =   ('S' + supplier_id.astype(str).str.zfill(4)).where(supplier_id.notnull(), np.nan)

        supplier_name = supplier_id.replace(r'\D','', regex=True).astype('Int16')
        supplier_name = ('Supplier_' + supplier_name.astype(str)).where(supplier_name.notnull(), np.nan)

        city =          df['city'].astype(str).str.strip().str.lower().replace({'nan':np.nan})

        phone =         df['phone'].str.replace('ext.99','').replace(r'\D','', regex=True).str.extract(r'(\d{10}$)')[0].astype('Int64').astype(str)
        phone =         ('+91' + phone).where(phone.str.len()==10, np.nan)

        email =         supplier_name.str.lower().str.replace('_','')+'@mail.com'

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
