import numpy as np
import pandas as pd
import datetime
from pathlib import Path
from common.common import clean_id, clean_text, clean_phone_n
from base.base_silver_pipeline import BaseSilverPipeline


# age groups
bins = [0, 17, 24, 34, 44, 54, 64, 200]
labels = ["<18", "18-24", "25-34", "35-44", "45-54", "55-64", "65+"]

class CustomersSilver(BaseSilverPipeline):

    def clean(self, df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:

        import datetime

        customer_id =   clean_id(df['customer_id'], 'C', 6)

        customer_name = clean_text(df['customer_name']).str.title()

        city =          clean_text(df['city']).str.title()
        city =          city.where(city.isin(['Mumbai', 'Hyderabad', 'Bangalore', 'Pune', 'Delhi']))

        phone =         clean_phone_n(df['phone'])

        curr_date =     datetime.datetime.now().strftime('%Y-%m-%d')
        birthdate =     pd.to_datetime(df['birthdate'], format='%Y-%m-%d', errors='coerce')
        birthdate =     birthdate.where((birthdate>'1950-01-01') & (birthdate< curr_date)) #type:ignore

        age =           pd.Timestamp.today().year - birthdate.dt.year

        age_group =      pd.cut(age, bins=bins, labels=labels, right=True)

        gender =        clean_text(df['gender']).str.title()
        gender =        gender.where(gender.isin(['Male', 'Female', 'Prefer Not To Say']))

        created_at =    pd.to_datetime(df['created_at'], format= '%Y-%m-%d %H:%M:%S', errors= 'coerce')
        created_at =    created_at.where((created_at>= '2021-01-01') & (created_at <= '2025-12-31')) #type:ignore

        tier =          clean_text(df['tier']).str.lower()
        tier =          tier.where(tier.isin(['casual', 'regular', 'loyal']))

        df1 = pd.DataFrame({
            'customer_id':customer_id,
            'customer_name':customer_name,
            'city':city,
            'phone':phone,
            'birthdate':birthdate,
            'age_group': age_group,
            'gender':gender,
            'created_at':created_at,
            'tier':tier
        })

        df2 = df1.dropna(subset='customer_id')
        df3 = df2.drop_duplicates(subset= 'customer_id').sort_values(by = 'customer_id').reset_index(drop=True)

        return df1, df2, df3
