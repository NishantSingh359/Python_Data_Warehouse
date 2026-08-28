import pandas as pd
import numpy as np 

def clean_id(series:pd.Series, prefix:str, length:int) -> pd.Series:
    """
    Clean and format an ID series by:
    - Removing non-digit characters
    - Converting to integer (nullable Int32)
    - Replacing empty/zero values with NaN
    - Padding with leading zeros
    - Adding a prefix
    
    Parameters
    ----------
    series : pd.Series
        Input series containing raw IDs.
    prefix : str
        Prefix to prepend to each cleaned ID.
    length : int
        Total length of the numeric part.
    
    Returns
    -------
    pd.Series
        Cleaned and formatted ID series.
    """
        
    digits =    series.str.replace(r'\D', '', regex=True)

    digits =    digits.replace({'':np.nan}).astype('Int32')

    digits =    digits.replace({0:np.nan})

    formatted = (prefix + digits.astype(str).str.zfill(length))

    return formatted.where(digits.notnull())

def clean_text(series:pd.Series) -> pd.Series:

    """
    Clean text series by:
    - Removing digit characters
    - Removing extra spaces
    - Replacing empty or some string values with NaN
    
    Parameters
    ----------
    series : pd.Series
        Input series containing raw text.
    
    Returns
    -------
    pd.Series
        Cleaned text series.
    """

    text = series.astype(str).str.replace(r'[^A-Za-z\s]', '', regex=True)

    text =  text.str.strip()

    return text.replace({'':np.nan, 'nan':np.nan, 'None':np.nan})

def clean_phone_n(series:pd.Series) -> pd.Series:

    """
    Clean and format an phone number series by:
    - Removing none-digit characters
    - Extracting 10 digits from right side
    - Add prefix
    
    Parameters
    ----------
    series : pd.Series
        Input series containing raw phone numbers.
    
    Returns
    -------
    pd.Series
        Cleaned phone numbers series.
    """

    phone = series.astype(str).str.replace(r'\D','', regex= True)

    phone = phone.str.extract(r'(\d{10}$)')[0]

    phone = ('+91' + phone).where(phone.str.len() == 10)

    return phone

# ============ HELPERS

def item_total() -> pd.DataFrame:
        "Return order_id with item_total"

        df =           pd.read_csv(r".\.\.\data\raw\crm\order_items.csv.gz")
        menu =         pd.read_parquet(r".\.\.\data\silver\erp\menu_items.parquet")
        
        order_id =      clean_id(df['order_id'], 'O', 7)

        item_id =       clean_id(df['item_id'], 'I', 4)
        item_id =       item_id.where(item_id.isin(menu['item_id']))

        quantity =      pd.to_numeric(df['quantity'], errors='coerce')
        quantity =      quantity.where((quantity > 0) & (quantity < 10))

        unit_price =    pd.to_numeric(df['unit_price'], errors='coerce')
        unit_price =    unit_price.where((unit_price > 0) & (unit_price < 500))

        line_total =    pd.to_numeric(df['line_total'], errors='coerce')
        line_total =    line_total.where((line_total > 0) & (line_total < 5000))

        df = pd.DataFrame({
            'order_id':order_id,
            'item_id':item_id,
            'quantity':quantity,
            'unit_price':unit_price,
            'line_total':line_total
        })

        join =                df.merge(menu, on='item_id', how='left')
        df['unit_price'] =    join['unit_price'].fillna(join['selling_price'])

        df.loc[df['quantity'].isna(), 'quantity'] =     df['line_total'] / df['unit_price']
        df['quantity'] = df['quantity'].fillna(1).astype(int)
        df.loc[df['line_total'].isna(), 'line_total'] = df['unit_price'] * df['quantity']

        order_amt = df.groupby('order_id')['line_total'].sum().reset_index()
        order_amt.rename(columns={'line_total':'item_total'}, inplace=True)
        
        return order_amt


def completed_order() -> pd.Series:

    "Return order_item_id of those order that order cancel_stage is after_prepare or not_cancelled or out_of_delivery"

    order =   pd.read_parquet(r".\.\.\data\silver\crm\orders.parquet")
    order =   order[(order['cancel_stage'] != 'order_failed') | (order['cancel_stage'] !=  'before_prepare')]
    ord_itm = pd.read_parquet(r".\.\.\data\silver\crm\order_items.parquet")

    return order.merge(ord_itm, on='order_id', how='left')['order_item_id']

def wasted() -> pd.Series:

    "Return order_item_id of those order that user cancelled after_prepare"

    order =   pd.read_parquet(r".\.\.\data\silver\crm\orders.parquet")
    order =   order[order['cancel_stage'] == 'after_prepare']
    ord_itm = pd.read_parquet(r".\.\.\data\silver\crm\order_items.parquet")

    return order.merge(ord_itm, on='order_id', how='left')['order_item_id']

def prepared() -> pd.Series:

    "Return order_item_id of those order that order cancel_stage is not_cancelled or out_of_delivery"

    order =   pd.read_parquet(r".\.\.\data\silver\crm\orders.parquet")
    order =   order[(order['cancel_stage'] == 'not_cancelled') | (order['cancel_stage'] == 'out_of_delivery')]
    ord_itm = pd.read_parquet(r".\.\.\data\silver\crm\order_items.parquet")

    return order.merge(ord_itm, on='order_id', how='left')['order_item_id']