import yaml
import numpy as np
import pandas as pd
from pathlib import Path
from common.common import clean_id, clean_text, item_total
from base.base_silver_pipeline import BaseSilverPipeline

with open("src/silver/config/crm.yaml") as f:
    cfg = yaml.full_load(f)
    path = cfg['tables']['orders']


class OrdersSilver(BaseSilverPipeline):

    def clean(self, df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:

        cancel_stage_map =  {'beforeprepare': 'before_prepare', 'outfordelivery': 'out_of_delivery', 'afterprepare': 'after_prepare'}
        cancel_reason_map = {'restaurantissue': 'restaurant_issue', 'usercancel': 'user_cancel', 'delay': 'delay'}


        order_id =       clean_id(df['order_id'], 'O', 7)

        customers =       pd.read_parquet(path['customers_path'])
        customer_id =     clean_id(df['customer_id'], 'C', 6)
        customer_id =     customer_id.where(customer_id.isin(customers['customer_id']), np.nan)

        restaurants =     pd.read_parquet(path['restaurants_path'])
        restaurant_id =   clean_id(df['restaurant_id'], 'R', 3)
        restaurant_id =   restaurant_id.where(restaurant_id.isin(restaurants['restaurant_id']), np.nan)

        order_datetime =  df['order_datetime'].str.replace('@','').str.replace('/','').str.strip()
        order_datetime =  pd.to_datetime(df['order_datetime'], format= '%Y-%m-%d %H:%M:%S', errors='coerce')
        order_datetime =  order_datetime.where((order_datetime >= '2021-01-01') & (order_datetime <= "2025-12-31")) #type:ignore

        payment_mode =    clean_text(df['payment_mode']).str.lower()
        order_status =    clean_text(df['order_status']).str.lower()

        cancel_stage =    clean_text(df['cancel_stage']).str.lower()
        cancel_stage =    cancel_stage.map(cancel_stage_map)  

        cancel_reason =   clean_text(df['cancel_reason']).str.lower()
        cancel_reason =   cancel_reason.map(cancel_reason_map)

        partners =        pd.read_parquet(path['delivery_partners_path'])
        partner_id =      clean_id(df['delivery_partner_id'], 'D', 4)
        partner_id =      partner_id.where(partner_id.isin(partners['delivery_partner_id']))

        promo =           pd.read_parquet(path['promotions_path'])
        promo_id  =       clean_id(df['promo_id'], 'PROMO', 3)
        promo_id =        promo_id.where(promo_id.isin(promo['promo_id']))

        dis_amount =      df['discount_amount']

        delivery_fee =    pd.to_numeric(df['delivery_fee'], errors='coerce')
        delivery_fee=     delivery_fee.where(delivery_fee.isin([40, 50, 60]))
        delivery_fee =    delivery_fee.fillna(delivery_fee.mode()[0])


        df = pd.DataFrame({
            'order_id':order_id,
            'customer_id':customer_id,
            'restaurant_id':restaurant_id,
            'order_datetime':order_datetime,
            'payment_mode':payment_mode,
            'order_status':order_status,
            'cancel_stage':cancel_stage,
            'cancel_reason':cancel_reason,
            'delivery_partner_id':partner_id,
            'promo_id':promo_id,
            'discount_amount':dis_amount,
            'delivery_fee':delivery_fee
        })

        # FAILED ORDER
        df.loc[df['order_status'] == 'failed', 'cancel_stage'] =        'order_failed'
        df.loc[df['order_status'] == 'failed', 'cancel_reason'] =       'order_failed'
        df.loc[df['order_status'] == 'failed', 'payment_mode'] =        'order_failed'
        df.loc[df['order_status'] == 'failed', 'delivery_partner_id'] = 'order_failed'

        # COMPLETED ORDER
        df.loc[df['order_status'] == 'completed', 'cancel_stage'] =  'not_cancelled'
        df.loc[df['order_status'] == 'completed', 'cancel_reason'] = 'not_cancelled'

        # FILL NULL VALUES
        df.loc[(df['order_status'] == 'cancelled') & df['cancel_stage'].isna(), 'cancel_stage'] = 'UNKNOWN'
        df.loc[(df['order_status'] == 'cancelled') & (df['cancel_reason'].isna()), 'cancel_reason'] = 'UNKNOWN'
        df.loc[(df['order_status'] == 'completed') & df['delivery_partner_id'].isna(), 'delivery_partner_id'] = 'UNKNOWN'
        df.loc[(df['order_status'] == 'cancelled') & (df['cancel_stage'] == 'out_of_delivery') & df['delivery_partner_id'].isna(), 'delivery_partner_id'] = 'UNKNOWN'


        df.loc[(df['order_status'] == 'cancelled') & (df['cancel_stage'] != 'out_of_delivery') & (df['delivery_partner_id'].isna()), 'delivery_partner_id'] = 'order_cancelled'

        # CLEAN PROMO_ID
        df =               df.merge(promo, on= 'promo_id', how='left')
        df['promo_id'] =   df['promo_id'].where((df['order_datetime'].dt.strftime('%Y-%m-%d')  >=  df['valid_from'].dt.strftime('%Y-%m-%d')) &
                          (df['order_datetime'].dt.strftime('%Y-%m-%d')  <=  df['valid_to'].dt.strftime('%Y-%m-%d')), 'not_apply')

        df =               df.merge(item_total(), how='left', on='order_id')
        df['promo_id'] =   df['promo_id'].where(df['item_total'] >= df['min_order_value'], 'not_apply')
        
        # ADD DISCOUNT AMOUNT
        df.loc[df['discount_type'] == 'percent', 'discount_amount'] = df['item_total'] / df['discount_value']
        df.loc[df['discount_type'] == 'flat', 'discount_amount'] = df['discount_value'] 

        # CREATE COLUMN
        df['order_total'] = df['item_total'] - df['discount_amount'] + df['delivery_fee']

        df1 = df[[
            'order_id',
            'customer_id',
            'restaurant_id', 
            'order_datetime', 
            'payment_mode', 
            'order_status', 
            'cancel_stage', 
            'cancel_reason', 
            'delivery_partner_id', 
            'promo_id', 
            'discount_amount', 
            'delivery_fee', 
            'item_total', 
            'order_total']]

        
        df2 = df1.dropna(subset=['order_id', 'customer_id', 'restaurant_id'])
        df3 = df2.drop_duplicates(subset= 'order_id').reset_index(drop=True)

        return df1, df2, df3