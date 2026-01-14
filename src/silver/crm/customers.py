import numpy as np
import pandas as pd
from pathlib import Path
from base.base_silver_pipeline import BaseSilverPipeline

class CustomersSilver(BaseSilverPipeline):

    def clean(self, cust: pd.DataFrame) -> pd.DataFrame:

        customer_id =   cust['customer_id'].str.replace(r'\D','', regex=True).replace({'':np.nan})
        customer_id =   customer_id.fillna(cust['customer_name'].str.replace(r'\D','',regex=True).replace({'':np.nan}))
        customer_id =   ('C' + customer_id.astype('Int32').astype(str).str.zfill(6)).where(customer_id.notnull(), np.nan)

        customer_name = customer_id.str.replace(r'\D','', regex=True).astype('Int32')
        customer_name = ('Customer_' + customer_name.astype(str)).where(customer_name.notnull(), np.nan)

        city =          cust['city'].str.strip().replace({'nan':np.nan}).str.lower()

        phone =         cust['phone'].str.replace('ext.99','').replace(r'\D','', regex=True).str.extract(r'(\d{10}$)')[0].astype('Int64').astype(str)
        phone =         ('+91' + phone).where(phone.str.len() == 10, np.nan)

        created_at =    cust['created_at'].str.strip()
        created_at =    pd.to_datetime(created_at, format= '%Y-%m-%d %H:%M:%S', errors='coerce')

        df = pd.DataFrame({
            'customer_id':customer_id,
            'customer_name':customer_name,
            'city':city,
            'phone':phone,
            'created_at':created_at
        })

        return (
            df
            .dropna(subset= 'customer_id')
            .drop_duplicates(subset= 'customer_id')
            .sort_values(by = 'customer_id')
            .reset_index(drop=True)
        )