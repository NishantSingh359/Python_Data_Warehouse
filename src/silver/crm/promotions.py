import numpy as np
import pandas as pd
import datetime
from pathlib import Path
from common.common import clean_id, clean_text
from base.base_silver_pipeline import BaseSilverPipeline

class PromotionsSilver(BaseSilverPipeline):

    def clean(self, df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:

        promo_id =      clean_id(df['promo_id'], 'PROMO', 3)

        promo_name =    clean_text(df['promo_name']).str.title()

        dis_type =      clean_text(df['discount_type']).str.lower()

        dis_value =     pd.to_numeric(df['discount_value'], errors='coerce')
        dis_value =     dis_value.where((dis_value > 0) & (dis_value < 1000))

        min_ord_value = pd.to_numeric(df['min_order_value'], errors='coerce').abs()
        min_ord_value = min_ord_value.where((min_ord_value > 0) & (min_ord_value < 1000))

        valid_from =    df['valid_from'].astype(str).str.strip()
        valid_from =    pd.to_datetime(valid_from, format='%Y-%m-%d', errors='coerce')

        valid_to =      df['valid_to'].astype(str).str.replace('@','')
        valid_to =      pd.to_datetime(valid_to, format='%Y-%m-%d', errors='coerce')

        df1 = pd.DataFrame({
            'promo_id': promo_id,
            'promo_name': promo_name,
            'discount_type': dis_type,
            'discount_value': dis_value,
            'min_order_value': min_ord_value,
            'valid_from': valid_from,
            'valid_to': valid_to,
        })

        df2 = df1.dropna(subset='promo_id')
        df3 = df2.drop_duplicates(subset= 'promo_id').sort_values(by = 'promo_id').reset_index(drop=True)

        return df1, df2, df3