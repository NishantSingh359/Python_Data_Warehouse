import pandas as pd
import numpy as np 

def clean_id(series:pd.Series, prefix:str, length:int):
    id = series.str.replace(r'\D', '', regex=True).replace({'':np.nan}).astype('Int32').replace({0:np.nan})
    id = (prefix + id.astype(str).str.zfill(length)).where(id.notnull())
    return id

def clean_phone_n(series:pd.Series):
    phone = series.astype('Int64').astype(str).str.replace(r'\D','', regex= True).str.extract(r'(\d{10}$)')[0]
    phone = ('+91' + phone).where(phone.str.len() == 10)
    return phone