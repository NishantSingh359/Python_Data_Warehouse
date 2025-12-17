import datetime
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT_DIR))

import os
import pandas as pd

import importlib
import Module as m
importlib.reload(m) 

# -----------------------------------------
# ========================== ETL CRM TABLES
# -----------------------------------------

if __name__ == "__main__":
    
    print('=============================================')
    print('================ SILVER LAYER ===============')
    print('=============================================')

    print('---------------------------------------------')
    print('================= CRM TABLES ================')
    print('---------------------------------------------')

    # ============ restaurants.csv.gz
    time1 = datetime.datetime.now()

    print("===================== LOADING restaurants.csv")
    path = os.path.join("Dataset","CRM", "restaurants.csv.gz")
    res = pd.read_csv(path)

    print("=============== CREATING DATAFRAME restaurant")
    restaurant = pd.DataFrame()

    print("==================== CLEANING restaurants.csv")
    restaurant['restaurant_id'] = res['restaurant_id'].apply(lambda x: x.strip().replace("Y","1").replace("nan &#x1F60A;","R004"))
    restaurant['name'] = res['name'].astype('str').apply(lambda x: x.strip().replace("<b>bad</b>","").replace('nan','FoodHub Branch 7').replace("3 ","3"))
    restaurant['city'] = res['city'].astype('str').apply(lambda x: m.value_cleaning(x))
    restaurant['open_date'] = pd.to_datetime(res['open_date'].astype('str').apply(lambda x: x.replace("<b>bad</b>","").replace(" ","")),format= "%Y-%m-%d")
    restaurant['phone'] = res['phone'].astype('str').apply(lambda x: m.value_cleaning(x)[2::] if len(m.value_cleaning(x)[2::]) == 10 else None)

    restaurant = restaurant.sort_values(by = 'restaurant_id').reset_index().drop("index", axis = 1).dropna(subset= ['restaurant_id'],axis = 0)
    restaurant.drop_duplicates(subset= ['restaurant_id']).to_pickle(r"Layers/Silver/CRM/restaurant.pkl")

    time2 = datetime.datetime.now()
    time = time2 - time1
    print("TABLE LOADING TIME")
    print(time)


    # ============ delivery_partners.csv.gz
    time1 = datetime.datetime.now()

    print("=============== LOADING delivery_partners.csv")
    path = os.path.join("Dataset","CRM", "delivery_partners.csv.gz")
    deli = pd.read_csv(path)

    print("======== CREATING DATAFRAME delivery_partners")
    delivery_partners = pd.DataFrame()

    print("============== CLEANING delivery_partners.csv")
    delivery_partners['delivery_id'] = deli['delivery_id'].astype('str').apply(lambda x: m.value_cleaning(x).replace('DO17','D0017').replace('D0036x1F60A','D0036') if len(m.value_cleaning(x)) == 5 else None)
    delivery_partners['name'] = deli['name'].astype('str').apply(lambda x: m.value_cleaning(x) if len(m.value_cleaning(x)) in (9,10,11) else None)
    delivery_partners['vehicle_type'] = deli['vehicle_type'].astype('str').apply(lambda x: None if m.value_cleaning(x) == 'nan' else m.value_cleaning(x))
    delivery_partners['phone'] = deli['phone'].astype('str').apply(lambda x: None if len(m.value_cleaning(x)[2:12]) != 10 else m.value_cleaning(x)[2:12])

    deliv = delivery_partners.sort_values(by = 'delivery_id').reset_index().drop('index', axis = 1).dropna(subset= ['delivery_id'], axis = 0)
    deliv.drop_duplicates(subset=['delivery_id']).to_pickle(r"Layers/Silver/CRM/delivery_partners.pkl")

    time2 = datetime.datetime.now()
    time = time2 - time1
    print("TABLE LOADING TIME")
    print(time)


    # ============ customers.csv.gz
    time1 = datetime.datetime.now()

    print("======================== LOADING customer.csv")
    path = os.path.join("Dataset","CRM", "customers.csv.gz")
    cust = pd.read_csv(path)

    print("================= CREATING DATAFRAME customer")
    customer = pd.DataFrame()

    print("====================== CLEANING customers.csv")
    customer['customer_id'] = cust['customer_id'].astype('str').apply(lambda x: m.word_cleaning(m.word_cleaning(m.value_cleaning(x))) if len(m.value_cleaning(x)) in (6,7) else None)
    customer['first_name'] = cust['first_name'].astype('str').apply(lambda x: None if m.word_cleaning(m.value_cleaning(x)) in ('nan',) else m.word_cleaning(m.value_cleaning(x)))
    customer['last_name']  = cust['last_name'].astype('str').apply(lambda x: None if m.word_cleaning(m.value_cleaning(x)) in ('nan',) else m.word_cleaning(m.value_cleaning(x)))
    customer['phone'] = cust['phone'].astype('str').apply(lambda x: None if len(m.word_cleaning(m.value_cleaning(x))[2:12]) != 10 else m.word_cleaning(m.value_cleaning(x))[2:12])
    customer['city'] = cust['city'].astype('str').apply(lambda x: None if m.word_cleaning(m.value_cleaning(x)) in ('nan',) else m.word_cleaning(m.value_cleaning(x)))
    customer['created_at'] = cust['created_at'].astype('str').apply(lambda x: None if len(m.word_cleaning(m.value_cleaning_m(x))) != 10 else m.word_cleaning(m.value_cleaning_m(x)))

    cstmr = customer.sort_values(by = 'customer_id').dropna(subset= ['customer_id']).drop_duplicates(subset= ['customer_id']).reset_index().drop('index',axis = 1)
    cstmr.to_pickle(r"Layers/Silver/CRM/customer.pkl")

    time2 = datetime.datetime.now()
    time = time2 - time1
    print("TABLE LOADING TIME")
    print(time)


    # ============ order.csv.gz
    time1 = datetime.datetime.now()

    print("========================== LOADING orders.csv")
    path = os.path.join("Dataset","CRM", "orders.csv.gz")
    orde = pd.read_csv(path)

    print("==================== CREATING DATAFRAME order")
    order = pd.DataFrame()

    print("========================= CLEANING orders.csv")
    # ----- order_id
    order_id = orde['order_id'].astype('str').apply(lambda x: m.letter_cleaning(m.word_cleaning(m.value_cleaning(x))))
    order_id1 = ['O0'+i[1::] if len(i) == 7 else i for i in order_id]
    order['order_id'] = [ i if len(i) == 8 else None for i in order_id1]
    # ----- customer_id
    cust_id = orde['customer_id'].astype('str').apply(lambda x: m.letter_cleaning(m.word_cleaning(m.value_cleaning(x))) if len(m.letter_cleaning(m.word_cleaning(m.value_cleaning(x)))) in (6,7) else None)
    customer = pd.read_pickle("./Layers/Silver/CRM/customer.pkl")
    customer_ids = set(customer['customer_id'])
    order['customer_id'] = [i if i in customer_ids else None for i in cust_id]
    # ----- restaurant_id
    rest_id = orde['restaurant_id'].astype('str').apply(lambda x: m.fix_ids(m.letter_cleaning(m.word_cleaning(m.value_cleaning(x)))))
    rest_id1 = [i if len(i) == 4 else None for i in rest_id]
    restaurant = pd.read_pickle(r"C:\Users\TUF\OneDrive\Documents\Code\Vs Code\Python_DataWarehouse\Layers\Silver\CRM\restaurant.pkl")
    order['restaurant_id'] = [i if i in restaurant['restaurant_id'].tolist() else None for i in rest_id1]
    # ----- order_datetime
    datetime1 = orde['order_datetime'].astype('str').apply(lambda x: m.word_cleaning(m.value_cleaning_sc(x)))
    datetime2 = [i if len(i) == 19 else None for i in datetime1]
    order['order_datetime'] = pd.to_datetime(datetime2, format= "%Y-%m-%d %H:%M:%S")
    # ----- payment_mode
    pay_mode = orde['payment_mode'].astype('str').apply(lambda x: m.word_cleaning(m.value_cleaning_sc(x)))
    order['payment_mode'] = [i if i in ['Card', 'UPI', 'Cash', 'NetBanking'] else None for i in pay_mode]
    # ----- is_delivery
    is_deli = orde['is_delivery'].astype('str').apply(lambda x: m.word_cleaning(m.value_cleaning_sc(x)))
    order['is_delivery'] = [ i if i in ['1','0'] else None for i in is_deli]
    order['is_delivery'] = order['is_delivery'].astype('Int16')
    # ----- delivery_id
    del_id = orde['delivery_id'].astype('str').apply(lambda x: m.letter_cleaning(m.word_cleaning(m.value_cleaning(x))))
    del_id1 = [i[0]+'0'+i[1::] if len(i) == 4 else i if len(i) == 5 else None for i in del_id ]
    deli_part = pd.read_pickle("./Layers/Silver/CRM/delivery_partners.pkl")
    order['delivery_id'] = [ None if i not in deli_part['delivery_id'].tolist() else i for i in del_id1]
    # ----- order_total
    total = orde['order_total'].astype('str').apply(lambda x: m.word_cleaning(m.value_cleaning_d(x)))
    order['total'] = [None if i in ('nan','na','n') or float(i) <= 0 else float(i) for i in total]
    # ----- order_status
    status = orde['order_status'].astype('str').apply(lambda x: m.word_cleaning(m.value_cleaning(x)))
    order['order_status'] = [i if i in ['Completed', 'Delivered', 'Preparing', 'Refunded', 'Cancelled'] else None for i in status]

    ordr = order.dropna(subset= 'order_id').drop_duplicates(subset = 'order_id').sort_values(by = 'order_id').reset_index().drop('index', axis = 1)
    ordr.to_pickle(r"Layers/Silver/CRM/order.pkl")

    time2 = datetime.datetime.now()
    time = time2 - time1
    print("TABLE LOADING TIME")
    print(time)


    # ============ menu_items.csv.gz
    time1 = datetime.datetime.now()

    print("====================== LOADING menu_items.csv")
    path = os.path.join("Dataset","CRM", "menu_items.csv.gz")
    menu = pd.read_csv(path)

    print("================ CREATING DATAFRAME menu_item")
    menu_item = pd.DataFrame()

    print("===================== CLEANING menu_items.csv")
    # item_id
    item_id = menu['item_id'].astype('str').apply(lambda x: m.word_cleaning(m.value_cleaning(x)))
    menu_item['item_id'] = [i[0]+'0'+i[1::] if len(i) == 4 else i if len(i) == 5 else None for i in item_id]
    # item_name
    menu_item['item_name'] = menu['item_name'].astype('str').apply(lambda x: m.word_cleaning(m.value_cleaning_s(x)))
    # category
    category = menu['category'].astype('str').apply(lambda x: m.word_cleaning(m.value_cleaning(x)))
    menu_item['category'] = [ None if i in ('nan','na','n') else i for i in category]
    # price
    price = menu['price'].astype('str').apply(lambda x:  m.word_cleaning(m.value_cleaning_d(x)))
    menu_item['price'] = [None if i in ('nan','na','n') or float(i) <= 0 else float(i) for i in price]
    # supplier_id
    supplier_id = menu['supplier_id'].astype('str').apply(lambda x:  m.letter_cleaning(m.word_cleaning(m.value_cleaning_d(x))))
    menu_item['supplier_id'] = [i[0]+'0'+i[1::] if len(i) == 4 else i if len(i) == 5 else None for i in supplier_id]

    menu_ite = menu_item.dropna(subset= 'item_id').drop_duplicates(subset= 'item_id').sort_values(by = 'item_id').reset_index().drop('index', axis = 1)
    menu_ite.to_pickle(r"Layers/Silver/CRM/menu_item.pkl")

    time2 = datetime.datetime.now()
    time = time2 - time1
    print("TABLE LOADING TIME")
    print(time)


    # ============ order_items.csv.gz
    time1 = datetime.datetime.now()

    print("===================== LOADING order_items.csv")
    path = os.path.join("Dataset","CRM", "order_items.csv.gz")
    ord_itm = pd.read_csv(path)

    print("============== CREATING DATAFRAME order_items")
    order_item = pd.DataFrame()

    print("==================== CLEANING order_items.csv")
    order_item = pd.DataFrame()
    # order_item_id
    ord_itm_id = ord_itm['order_item_id'].astype('str').apply(lambda x: m.letter_cleaning(m.word_cleaning(m.value_cleaning(x))))
    ord_itm_id1 = ['OI0'+i[2::] if len(i) == 8 else i for i in ord_itm_id]
    order_item['order_item_id'] = [i if len(i) == 9 else None for i in ord_itm_id1]
    # order_id
    order_id = ord_itm['order_id'].astype('str').apply(lambda x: m.letter_cleaning(m.word_cleaning(m.value_cleaning(x))))
    order_id1 = ['O0'+i[1::] if len(i) == 7 else i for i in order_id]
    order_id2 = [ i if len(i) == 8 else None for i in order_id1]
    order = pd.read_pickle("./Layers/Silver/CRM/order.pkl")
    order_id3 = set(order['order_id'])
    order_item['order_id'] = [i if i in order_id3 else None for i in order_id]
    # item_id
    item_id = ord_itm['item_id'].astype('str').apply(lambda x: m.word_cleaning(m.value_cleaning(x)))
    item_id1 = [i[0]+'0'+i[1::] if len(i) == 4 else i if len(i) == 5 else None for i in item_id]
    menu_item = pd.read_pickle("./Layers/Silver/CRM/menu_item.pkl")
    item_id2 = set(menu_item['item_id'])
    order_item['item_id'] = [None if i not in item_id2 else i for i in item_id1]
    # quantity
    qty = ord_itm['quantity'].astype('str').apply(lambda x: m.word_cleaning(m.value_cleaning(x)))
    order_item['quantity'] = [0 if i in ('nan', 'na', '0') or len(i) > 2 else i for i in qty]
    # unit_price
    price = ord_itm['unit_price'].astype('str').apply(lambda x: m.word_cleaning(m.value_cleaning_d(x)))
    order_item['price'] = [0 if i in ('nan','na','n') or float(i) <= 0 else float(i) for i in price]
    # line_total
    price = ord_itm['line_total'].astype('str').apply(lambda x: m.word_cleaning(m.value_cleaning_d(x)))
    order_item['line_total'] = [0 if i in ('nan','na','n') or float(i) <= 0 else float(i) for i in price]

    ord_itm = order_item.dropna(subset = 'order_item_id').drop_duplicates(subset = 'order_item_id').sort_values(by = 'order_item_id').reset_index().drop('index', axis = 1)
    ord_itm.to_pickle(r"Layers/Silver/CRM/order_item.pkl")

    time2 = datetime.datetime.now()
    time = time2 - time1
    print("TABLE LOADING TIME")
    print(time)






















