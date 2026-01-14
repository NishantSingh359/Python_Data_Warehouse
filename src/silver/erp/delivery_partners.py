import numpy as np
import pandas as pd
from base.base_silver_pipeline import BaseSilverPipeline

class Delivery_partnersSilver(BaseSilverPipeline):

    def clean(self, del_part: pd.DataFrame) -> pd.DataFrame:

        partner_id =   del_part['delivery_partner_id'].str.replace(r'\D', '', regex=True).replace({'':np.nan})
        partner_id =   pd.to_numeric(partner_id, errors='coerce').replace(0, np.nan).astype('Int16')
        partner_id =   partner_id.fillna(del_part['name'].replace(r'\D', '', regex=True).replace({'':np.nan}))
        partner_id =   ('D'+partner_id.astype(str).str.zfill(4).where(partner_id.notnull(), np.nan))

        
        name =         partner_id.str.replace(r'\D', '',regex=True).astype('Int16')
        name =         ('Rider_' + name.astype(str)).where(name.notnull(), np.nan)

        partner_type = del_part['partner_type'].str.strip().str.lower().replace({'nan':np.nan})
        vehicle_type = del_part['vehicle_type'].str.strip().str.lower().replace({'nan':np.nan})

        phone =         del_part['phone'].str.replace('ext.99','').replace(r'\D','', regex= True).str.extract(r'(\d{10}$)')[0].astype('Int64').astype(str)
        phone =         ('+91' + phone).where(phone.str.len() == 10, np.nan)

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
