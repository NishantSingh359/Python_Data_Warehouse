import datetime
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT_DIR))

import os
import pandas as pd

import importlib
import module as m
importlib.reload(m) 



if __name__ == "__main__":
    
    print('=============================================')
    print('================ SILVER LAYER ===============')
    print('=============================================')
    print()

    silver_time1 = datetime.datetime.now()

    print('=============================================')
    print('================= CRM TABLES ================')
    print('=============================================')
    print('=====')

    crm_time1 = datetime.datetime.now()
    
    # ---------------------------------------------------
    # ---------------- restaurants.csv.gz ---------------
    # ---------------------------------------------------

    time1 = datetime.datetime.now()

    print("===================== LOADING restaurants.csv")
    path = os.path.join("dataset","crm", "restaurants.csv.gz")
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
    restaurant.drop_duplicates(subset= ['restaurant_id']).to_pickle(r"layers/silver/crm/restaurant.pkl")

    time2 = datetime.datetime.now()
    time = time2 - time1
    print("TABLE CLEANING TIME")
    print(time)


    # ---------------------------------------------------
    # ------------- delivery_partners.csv.gz ------------
    # ---------------------------------------------------

    time1 = datetime.datetime.now()

    print("=============== LOADING delivery_partners.csv")
    path = os.path.join("dataset","crm", "delivery_partners.csv.gz")
    deli = pd.read_csv(path)

    print("======== CREATING DATAFRAME delivery_partners")
    delivery_partners = pd.DataFrame()

    print("============== CLEANING delivery_partners.csv")
    delivery_partners['delivery_id'] = deli['delivery_id'].astype('str').apply(lambda x: m.value_cleaning(x).replace('DO17','D0017').replace('D0036x1F60A','D0036') if len(m.value_cleaning(x)) == 5 else None)
    delivery_partners['name'] = deli['name'].astype('str').apply(lambda x: m.value_cleaning(x) if len(m.value_cleaning(x)) in (9,10,11) else None)
    delivery_partners['vehicle_type'] = deli['vehicle_type'].astype('str').apply(lambda x: None if m.value_cleaning(x) == 'nan' else m.value_cleaning(x))
    delivery_partners['phone'] = deli['phone'].astype('str').apply(lambda x: None if len(m.value_cleaning(x)[2:12]) != 10 else m.value_cleaning(x)[2:12])

    deliv = delivery_partners.sort_values(by = 'delivery_id').reset_index().drop('index', axis = 1).dropna(subset= ['delivery_id'], axis = 0)
    deliv.drop_duplicates(subset=['delivery_id']).to_pickle(r"layers/silver/crm/delivery_partners.pkl")

    time2 = datetime.datetime.now()
    time = time2 - time1
    print("TABLE CLEANING TIME")
    print(time)


    # ---------------------------------------------------
    # ----------------- customers.csv.gz ----------------
    # ---------------------------------------------------

    time1 = datetime.datetime.now()

    print("======================== LOADING customer.csv")
    path = os.path.join("dataset","crm", "customers.csv.gz")
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
    cstmr.to_pickle(r"layers/silver/crm/customer.pkl")

    time2 = datetime.datetime.now()
    time = time2 - time1
    print("TABLE CLEANING TIME")
    print(time)


    # ---------------------------------------------------
    # ------------------- orders.csv.gz -----------------
    # ---------------------------------------------------

    time1 = datetime.datetime.now()

    print("========================== LOADING orders.csv")
    path = os.path.join("dataset","crm", "orders.csv.gz")
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
    customer = pd.read_pickle("./layers/silver/crm/customer.pkl")
    customer_ids = set(customer['customer_id'])
    order['customer_id'] = [i if i in customer_ids else None for i in cust_id]
    # ----- restaurant_id
    rest_id = orde['restaurant_id'].astype('str').apply(lambda x: m.fix_ids(m.letter_cleaning(m.word_cleaning(m.value_cleaning(x)))))
    rest_id1 = [i if len(i) == 4 else None for i in rest_id]
    restaurant = pd.read_pickle(r"C:\Users\TUF\OneDrive\Documents\Code\Vs Code\Python_DataWarehouse\layers\silver\crm\restaurant.pkl")
    order['restaurant_id'] = [i if i in restaurant['restaurant_id'].tolist() else None for i in rest_id1]
    # ----- order_datetime
    datetime1 = orde['order_datetime'].astype('str').apply(lambda x: m.word_cleaning(m.value_cleaning_scm(x)))
    datetime2 = [i if len(i) == 19 else None for i in datetime1]
    order['order_datetime'] = pd.to_datetime(datetime2, format= "%Y-%m-%d %H:%M:%S")
    # ----- payment_mode
    pay_mode = orde['payment_mode'].astype('str').apply(lambda x: m.word_cleaning(m.value_cleaning_scm(x)))
    order['payment_mode'] = [i if i in ['Card', 'UPI', 'Cash', 'NetBanking'] else None for i in pay_mode]
    # ----- is_delivery
    is_deli = orde['is_delivery'].astype('str').apply(lambda x: m.word_cleaning(m.value_cleaning_scm(x)))
    order['is_delivery'] = [ i if i in ['1','0'] else None for i in is_deli]
    order['is_delivery'] = order['is_delivery'].astype('Int16')
    # ----- delivery_id
    del_id = orde['delivery_id'].astype('str').apply(lambda x: m.letter_cleaning(m.word_cleaning(m.value_cleaning(x))))
    del_id1 = [i[0]+'0'+i[1::] if len(i) == 4 else i if len(i) == 5 else None for i in del_id ]
    deli_part = pd.read_pickle("./layers/silver/crm/delivery_partners.pkl")
    order['delivery_id'] = [ None if i not in deli_part['delivery_id'].tolist() else i for i in del_id1]
    # ----- order_total
    total = orde['order_total'].astype('str').apply(lambda x: m.word_cleaning(m.value_cleaning_d(x)))
    order['total'] = [None if i in ('nan','na','n') or float(i) <= 0 else float(i) for i in total]
    # ----- order_status
    status = orde['order_status'].astype('str').apply(lambda x: m.word_cleaning(m.value_cleaning(x)))
    order['order_status'] = [i if i in ['Completed', 'Delivered', 'Preparing', 'Refunded', 'Cancelled'] else None for i in status]

    ordr = order.dropna(subset= 'order_id').drop_duplicates(subset = 'order_id').sort_values(by = 'order_id').reset_index().drop('index', axis = 1)
    ordr.to_pickle(r"layers/silver/crm/order.pkl")

    time2 = datetime.datetime.now()
    time = time2 - time1
    print("TABLE CLEANING TIME")
    print(time)


    # ---------------------------------------------------
    # ---------------- menu_items.csv.gz ----------------
    # ---------------------------------------------------

    time1 = datetime.datetime.now()

    print("====================== LOADING menu_items.csv")
    path = os.path.join("dataset","crm", "menu_items.csv.gz")
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
    menu_ite.to_pickle(r"layers/silver/crm/menu_item.pkl")

    time2 = datetime.datetime.now()
    time = time2 - time1
    print("TABLE CLEANING TIME")
    print(time)


    # ---------------------------------------------------
    # ---------------- order_items.csv.gz ---------------
    # ---------------------------------------------------

    time1 = datetime.datetime.now()

    print("===================== LOADING order_items.csv")
    path = os.path.join("dataset","crm", "order_items.csv.gz")
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
    order = pd.read_pickle("./layers/silver/crm/order.pkl")
    order_id3 = set(order['order_id'])
    order_item['order_id'] = [i if i in order_id3 else None for i in order_id]
    # item_id
    item_id = ord_itm['item_id'].astype('str').apply(lambda x: m.word_cleaning(m.value_cleaning(x)))
    item_id1 = [i[0]+'0'+i[1::] if len(i) == 4 else i if len(i) == 5 else None for i in item_id]
    menu_item = pd.read_pickle("./layers/silver/crm/menu_item.pkl")
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
    ord_itm.to_pickle(r"layers/silver/crm/order_item.pkl")

    time2 = datetime.datetime.now()
    time = time2 - time1
    print("TABLE CLEANING TIME")
    print(time)
    print()
    # crm tables CLEANING TIME
    crm_time2 = datetime.datetime.now()
    crm_time = crm_time2 - crm_time1
    print("crm TABLES CLEANING TIME")
    print(crm_time)   
    print()


    print('=============================================')
    print('================= ERP TABLES ================')
    print('=============================================')
    print('=====')

    erp_time1 = datetime.datetime.now()

    # ---------------------------------------------------
    # ----------------- suppliers.csv.gz ----------------
    # ---------------------------------------------------

    time1 = datetime.datetime.now()

    print("======================= LOADING suppliers.csv")
    path = os.path.join("dataset","erp", "suppliers.csv.gz")
    suplir = pd.read_csv(path)

    print("================= CREATING DATAFRAME supplier")
    supplier = pd.DataFrame()

    print("======================= CLEANING supplier.csv")
    # supplier_id
    supp_id = suplir['supplier_id'].astype('str').apply(lambda x: m.word_cleaning(m.value_cleaning(x)))
    supplier['supplier_id'] = [i if len(i) == 5 else None for i in supp_id]
    # supplier_name
    supp_name = suplir['supplier_name'].astype('str').apply(lambda x: m.word_cleaning(m.value_cleaning(x)))
    supplier['supplier_name'] = [None if i in ('nan','na','n') else i for i in supp_name]
    # phone
    phone = suplir['phone'].astype('str').apply(lambda x: m.word_cleaning(m.value_cleaning(x)))
    supplier['phone'] = [i[2::] if len(i) == 12 else i[1::] if len(i) == 11 else None if i in ('nan','na','n') else i for i in phone]
    # city
    city = suplir['city'].astype('str').apply(lambda x: m.word_cleaning(m.value_cleaning(x)))
    supplier['city'] = [ None if i in ('nan','na','n') else i for i in city]

    suplr = supplier.dropna( subset= 'supplier_id').drop_duplicates(subset= 'supplier_id').sort_values(by = 'supplier_id').reset_index().drop('index', axis = 1)
    suplr.to_pickle(r"layers/silver/erp/order_item.pkl")

    time2 = datetime.datetime.now()
    time = time2 - time1
    print("TABLE CLEANING TIME")
    print(time)


    # ---------------------------------------------------
    # --------------- supplier_items.csv.gz -------------
    # ---------------------------------------------------

    time1 = datetime.datetime.now()
    
    print("================= LOADING suppliers_items.csv")
    path = os.path.join("dataset","erp", "supplier_items.csv.gz")
    sup_itm = pd.read_csv(path)

    print("============ CREATING DATAFRAME supplier_item")
    supplier_item = pd.DataFrame()

    print("================= CLEANING supplier_items.csv")
    # item_id
    item_id = sup_itm['item_id'].astype('str').apply(lambda x: m.letter_cleaning(m.word_cleaning(m.value_cleaning(x))))
    supplier_item['item_id'] = ['M0'+i[1::] if len(i) == 4 else  None if i in ('nan','na','n') else i for i in item_id]
    # supplier_id
    supplier_id = sup_itm['supplier_id'].astype('str').apply(lambda x: m.letter_cleaning(m.word_cleaning(m.value_cleaning(x))))
    supplier_id1 = [ 'S0'+i[1::] if len(i) == 4 else None if i in ('nan','na','n') else i for i in supplier_id]
    order_item = pd.read_pickle("./layers/silver/erp/order_item.pkl")
    supplier_id2 = set(order_item['supplier_id'])
    supplier_item['supplier_id'] = [i if i in supplier_id2 else None for i in supplier_id1]

    suplr_itm = supplier_item.dropna(subset = 'item_id').drop_duplicates(subset = 'item_id').sort_values(by = 'item_id').reset_index().drop('index', axis = 1)
    suplr_itm.to_pickle(r"layers/silver/erp/supplier_item.pkl")

    time2 = datetime.datetime.now()
    time = time2 - time1
    print("TABLE CLEANING TIME")
    print(time)


    # ---------------------------------------------------
    # ----------------- inventory.csv.gz ----------------
    # ---------------------------------------------------

    time1 = datetime.datetime.now()
    
    print("======================= LOADING inventory.csv")
    path = os.path.join("dataset","erp", "inventory.csv.gz")
    inty = pd.read_csv(path)

    print("================ CREATING DATAFRAME inventory")
    inventory = pd.DataFrame()

    print("====================== CLEANING inventory.csv")
    # restaurant_id
    rest_id = inty['restaurant_id'].astype('str').apply(lambda x: m.fix_ids(m.letter_cleaning(m.word_cleaning(m.value_cleaning(x)))))
    rest = pd.read_pickle("./layers/silver/crm/restaurant.pkl")
    rest_id1 = set(rest['restaurant_id'])
    inventory['restaurant_id'] = [ i if i in rest_id1 else None for i in rest_id]
    # item_id
    itm_id = inty['item_id'].astype('str').apply(lambda x: m.letter_cleaning(m.word_cleaning(m.value_cleaning(x))))
    itm_id2 = ['M0'+i[1::] if len(i) == 4 else i if len(i) == 5 else None for i in itm_id]
    sup_itm = pd.read_pickle("./layers/silver/erp/supplier_item.pkl")
    itm_id3 = set(sup_itm['item_id'])
    inventory['item_id'] = [ i if i in itm_id3 else None for i in itm_id2]
    # stock_qty
    sto_qty = inty['stock_qty'].astype('str').apply(lambda x: m.word_cleaning(m.value_cleaning(x)))
    inventory['stock_qty'] = [ None if i in ('nan','na','999','999999') else i for i in sto_qty]
    # reorder_lavel
    reor_lavel = inty['reorder_level'].astype('str').apply(lambda x: m.word_cleaning(m.value_cleaning(x)))
    reor_lavel['reorder_level'] = [None if i in ('nan','na','n') else i for i in reor_lavel]

    inventory.to_pickle(r"layers/silver/erp/inventory.pkl")

    time2 = datetime.datetime.now()
    time = time2 - time1
    print("TABLE CLEANING TIME")
    print(time)


    # ---------------------------------------------------
    # ----------------- employees.csv.gz ----------------
    # ---------------------------------------------------

    time1 = datetime.datetime.now()
    
    print("======================= LOADING employees.csv")
    path = os.path.join("dataset","erp", "employees.csv.gz")
    emp = pd.read_csv(path)

    print("================= CREATING DATAFRAME employee")
    employee = pd.DataFrame()

    print("====================== CLEANING employees.csv")
    # emp_id
    emp_id = emp['emp_id'].astype('str').apply(lambda x: m.word_cleaning(m.value_cleaning(x)))
    employee['emp_id'] = ['E0'+i[1::] if len(i) == 5 else i if len(i) == 6 else None for i in emp_id]
    # first_name
    first_name = emp['first_name'].astype('str').apply(lambda x: m.word_cleaning(m.value_cleaning(x)))
    employee['first_name'] = [ None if i in ('nan','na','n') else i for i in first_name]
    # last_name
    last_name = emp['last_name'].astype('str').apply(lambda x: m.word_cleaning(m.value_cleaning(x)))
    employee['last_name'] = [ None if i in ('nan','na','n') else i for i in last_name]
    # role
    role = emp['role'].astype('str').apply(lambda x: m.word_cleaning(m.value_cleaning_s(x)))
    employee['role'] = [ None if i in ('nan','na','n') else i for i in role]
    # restaurant_id
    rest_id = emp['restaurant_id'].astype('str').apply(lambda x: m.letter_cleaning(m.word_cleaning(m.value_cleaning_s(x))))
    rest = pd.read_pickle("./layers/silver/crm/restaurant.pkl")
    rest_id1 = set(rest['restaurant_id'])
    employee['restaurant_id'] =  [ i if i in rest_id1 else None for i in rest_id]
    # hire_date
    hire_date = emp['hire_date'].astype('str').apply(lambda x: m.word_cleaning(m.value_cleaning_m(x)))
    employee['hire_date'] = [None if i in ('nan','na','n') else i for i in hire_date]
    employee['hire_date'] = pd.to_datetime(employee['hire_date'], format = "%Y-%m-%d")
    # salary
    sly = emp['salary'].astype('str').apply(lambda x: m.word_cleaning(m.value_cleaning_d(x)))
    employee['salary'] = [None if i in ('nan','na','n','999') else float(i) for i in sly]

    emply = employee.dropna(subset= 'emp_id').drop_duplicates(subset= 'emp_id').sort_values(by = 'emp_id').reset_index().drop('index',axis = 1)
    emply.to_pickle(r"layers/silver/erp/employee.pkl")

    time2 = datetime.datetime.now()
    time = time2 - time1
    print("TABLE CLEANING TIME")
    print(time)


    # ---------------------------------------------------
    # --------------- kitchen_logs.csv.gz ---------------
    # ---------------------------------------------------

    time1 = datetime.datetime.now()
    
    print("==================== LOADING kitchen_logs.csv")
    path = os.path.join("dataset","erp", "kitchen_logs.csv.gz")
    kic = pd.read_csv(path)

    print("============== CREATING DATAFRAME kitchen_log")
    kitchen_log = pd.DataFrame()

    print("=================== CLEANING kitchen_logs.csv")
    # kitchen_log_id
    kit_id = kic['kitchen_log_id'].astype('str').apply(lambda x: m.letter_cleaning(m.word_cleaning(m.value_cleaning(x))))
    kitchen_log['kitchen_log_id'] = ['KL0'+i[2::] if len(i) == 9 else i if len(i) == 10 else None for i in kit_id]
    # order_item_id
    ord_itm_id = kic['order_item_id'].astype('str').apply(lambda x: m.letter_cleaning(m.word_cleaning(m.value_cleaning(x))))
    ord_itm_id1 = [ 'OI0'+i[2::] if len(i) == 8 else i if len(i) == 9 else None for i in ord_itm_id]
    ord_itm = pd.read_pickle("./layers/silver/crm/order_item.pkl")
    ord_itm_id2 = set(ord_itm['order_item_id'])
    kitchen_log['order_item_id'] = [i if i in ord_itm_id2 else None for i in ord_itm_id1]
    # order_id
    ord_id = kic['order_id'].astype('str').apply(lambda x: m.letter_cleaning(m.word_cleaning(m.value_cleaning(x))))
    ord_id1 = [ 'O0'+i[1::] if len(i) == 7 else i if len(i) == 8 else None for i in ord_id]
    ordr = pd.read_pickle("./layers/silver/crm/order.pkl")
    ord_id2 = set(ordr['order_id'])
    kitchen_log['order_id'] = [i if i in ord_id2 else None for i in ord_id1 ]
    # item_id
    item_id = kic['item_id'].astype('str').apply(lambda x: m.letter_cleaning(m.word_cleaning(m.value_cleaning(x))))
    item_id1 = ['M0'+i[1::] if len(i) == 4 else i if len(i) == 5 else None for i in item_id]
    menu_itm = pd.read_pickle("./layers/silver/crm/menu_item.pkl")
    item_id2 = set(menu_itm['item_id'])
    kitchen_log['item_id'] = [i if i in item_id2 else None for i in item_id1 ]
    # started_at
    start_at = kic['started_at'].astype('str').apply(lambda x: m.word_cleaning(m.value_cleaning_scm(x)).replace('T',' '))
    start_at2 = [i if len(i) == 19 else None for i in start_at]
    kitchen_log['started_at'] = pd.to_datetime(start_at2, format = "%Y-%m-%d %H:%M:%S")
    # completed_at
    comple_at = kic['completed_at'].astype('str').apply(lambda x: m.word_cleaning(m.value_cleaning_scm(x)).replace('T',' '))
    comple_at2 = [i if len(i) == 19 else None for i in comple_at]
    kitchen_log['completed_at'] = pd.to_datetime(comple_at2, format = "%Y-%m-%d %H:%M:%S")

    kitchen_log['started_at'] = kitchen_log['started_at'].where(kitchen_log['started_at'] < kitchen_log['completed_at'],pd.NaT)
    # chef_id
    chef_id = kic['chef_id'].astype('str').apply(lambda x: m.letter_cleaning(m.word_cleaning(m.value_cleaning(x))))
    chef_id1 = ['E0'+i[1::] if len(i) == 5 else i if len(i) == 6 else None for i in chef_id]
    emp = pd.read_pickle("./layers/silver/erp/employee.pkl")
    chef_id2 = set(emp['emp_id'])
    kitchen_log['chef_id'] = [ i if i in chef_id2 else None for i in chef_id1]
    # stauts
    status = kic['status'].astype('str').apply(lambda x: m.word_cleaning(m.value_cleaning_scm(x)))
    kitchen_log['status'] = [i if i in ('Completed', 'Started', 'Cancelled') else None for i in status]

    kit_log = kitchen_log.dropna(subset= 'kitchen_log_id').drop_duplicates(subset= 'kitchen_log_id').sort_values(by = 'kitchen_log_id').reset_index().drop('index', axis = 1)
    kit_log.to_pickle(r"layers/silver/erp/kitchen_log.pkl")

    time2 = datetime.datetime.now()
    time = time2 - time1
    print("TABLE CLEANING TIME")
    print(time)
    print()

    erp_time2 = datetime.datetime.now()
    erp_time = erp_time2 - erp_time1
    print("erp TABLES CLEANING TIME")
    print(erp_time)
    print()

    silver_time2 = datetime.datetime.now()
    silver_time = silver_time2 - silver_time1
    print("silver LAYER LOADING TIME")
    print(silver_time)
    print()

    print('=============================================')
    print('=========== SILVER LAYER COMPLETED ==========')
    print('=============================================')  
    print()