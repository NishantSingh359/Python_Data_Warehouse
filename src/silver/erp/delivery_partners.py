import pandas as pd
from common.common import clean_id, clean_text, clean_phone_n
from base.base_silver_pipeline import BaseSilverPipeline

class Delivery_partnersSilver(BaseSilverPipeline):

    def clean(self, df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:

        partner_id =    clean_id(df['delivery_partner_id'], 'D', 4)

        name =          clean_text(df['name']).str.title()

        partner_type =  clean_text(df['partner_type']).str.lower()
        partner_type =  partner_type.replace({'inhouse':'in_house', 'thirdparty':'third_party'})
        partner_type =  partner_type.where(partner_type.isin(['in_house', 'third_party']))

        vehicle_type =  clean_text(df['vehicle_type']).str.lower()
        vehicle_type =  vehicle_type.where(vehicle_type.isin(['scooter', 'bike']))

        phone =         clean_phone_n(df['phone'])

        join_date =     df['join_date'].str.replace('@','').str.replace('/','-')
        join_date =     pd.to_datetime(join_date, errors='coerce')

        rating =        pd.to_numeric(df['avg_rating'], errors='coerce')
        rating =        rating.where((rating >=0) & (rating <= 5))

        df1 = pd.DataFrame({
            'delivery_partner_id':partner_id,
            'name':name,
            'partner_type':partner_type,
            'vehicle_type':vehicle_type,
            'phone':phone,
            'join_date':join_date,
            'avg_rating':rating
        })

        df2 = df1.dropna(subset='delivery_partner_id')
        df3 = df2.drop_duplicates(subset='delivery_partner_id').sort_values(by ='delivery_partner_id').reset_index(drop=True)

        return  df1, df2, df3
        
