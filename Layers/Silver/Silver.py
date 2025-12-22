import datetime
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT_DIR))

import os
import pandas as pd
import numpy as np
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

    print("==================== CLEANING restaurants.csv")
    
    restaurant_id = res['restaurant_id'].apply(lambda x: x.strip().replace("Y","1").replace("nan &#x1F60A;","R004"))
    name =          res['name'].astype(str).apply(lambda x: m.word_cleaning(x).replace('nan','FoodHub Branch 7'))
    city =          res['city'].astype(str).apply(lambda x: m.value_cleaning(x))
    open_date =     res['open_date'].astype(str).apply(lambda x: m.word_cleaning(x))
    open_date =     pd.to_datetime(open_date, format= "%Y-%m-%d", errors= 'coerce')
    phone =         res['phone'].astype(str).apply(lambda x: m.value_cleaning(x)[2::] if len(m.value_cleaning(x)[2::]) == 10 else None)
    
    restaurant = pd.DataFrame({
        'restaurant_id': restaurant_id,
        'name':          name,
        'city':          city,
        'open_date':     open_date,
        'phone':         phone 
    })
    
    restaurant = restaurant.dropna(subset= ['restaurant_id'],axis = 0).drop_duplicates(subset= ['restaurant_id']).sort_values(by = 'restaurant_id').reset_index().drop("index", axis = 1)
    
    print("===================== LOADING restaurants.csv")
    restaurant.to_pickle(r"layers/silver/crm/restaurant.pkl")

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

    print("============== CLEANING delivery_partners.csv")
    
    delivery_id =  deli['delivery_id'].astype('str').apply(lambda x: m.value_cleaning(x).replace('DO17','D0017').replace('D0036x1F60A','D0036') if len(m.value_cleaning(x)) == 5 else None)
    name =         deli['name'].astype('str').apply(lambda x: m.value_cleaning(x) if len(m.value_cleaning(x)) in (9,10,11) else None)
    vehicle_type = deli['vehicle_type'].astype('str').apply(lambda x: m.value_cleaning(x) if m.value_cleaning(x) in ['Bike', 'Scooter', 'Car'] else None)
    phone =        deli['phone'].astype('str').apply(lambda x: m.value_cleaning(x)[2:12] if len(m.value_cleaning(x)[2:12]) == 10 else None)

    delivery_partner = pd.DataFrame({
        'delivery_id': delivery_id,
        'name':        name,
        'vehcle_type': vehicle_type,
        'phone':       phone
    })

    delivery_partner = delivery_partner.dropna(subset= ['delivery_id'], axis = 0).drop_duplicates(subset=['delivery_id']).sort_values(by = 'delivery_id').reset_index().drop('index', axis = 1)
    
    print("================ SAVING delivery_partners.pkl")
    delivery_partner.to_pickle(r"layers/silver/crm/delivery_partner.pkl")

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

    customer_id = cust['customer_id'].astype(str).apply(lambda x: m.word_cleaning(m.word_cleaning(m.value_cleaning(x))))
    customer_id =  ['C0'+i[1::] if len(i) == 6 else i for i in customer_id]
    customer_id =  [i if len(i) == 7 else None for i in customer_id]
    first_name =  cust['first_name'].astype(str).apply(lambda x: None if m.word_cleaning(m.value_cleaning(x)) in ('nan','na') else m.word_cleaning(m.value_cleaning(x)))
    last_name =   cust['last_name'].astype(str).apply(lambda x: None if m.word_cleaning(m.value_cleaning(x)) in ('nan','na') else m.word_cleaning(m.value_cleaning(x)))
    phone =       cust['phone'].astype(str).apply(lambda x: m.word_cleaning(m.value_cleaning(x)))
    phone =       [i[2::] if len(i[2::]) == 10 else None for i in phone]
    city =        cust['city'].astype(str).apply(lambda x: None if m.word_cleaning(m.value_cleaning(x)) in ('nan','na') else m.word_cleaning(m.value_cleaning(x)))
    created_at =  cust['created_at'].astype(str).apply(lambda x: m.word_cleaning(m.value_cleaning_m(x)))
    created_at =  pd.to_datetime(created_at, format = "%Y-%m-%d", errors= 'coerce')

    customer = pd.DataFrame({
        'customer_id': customer_id,
        'first_name':  first_name,
        'last_name':   last_name,
        'phone':       phone,
        'city':        city,
        'created_at':  created_at
    })

    customer = customer.sort_values(by = 'customer_id').dropna(subset= ['customer_id']).drop_duplicates(subset= ['customer_id']).reset_index().drop('index',axis = 1)
    
    print("======================== SAVING customer.pkl")
    customer.to_pickle(r"layers/silver/crm/customer.pkl")

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

    print("========================= CLEANING orders.csv")
    # ----- order_id
    order_id = orde['order_id'].astype('str').apply(lambda x: m.letter_cleaning(m.word_cleaning(m.value_cleaning(x))))
    order_id = ['O0'+i[1::] if len(i) == 7 else i for i in order_id]
    order_id = [i if len(i) == 8 else None for i in order_id]

    # ----- customer_id
    customer_id =  orde['customer_id'].astype('str').apply(lambda x: m.letter_cleaning(m.word_cleaning(m.value_cleaning(x))))
    customer_id =  ['C0'+i[1::] if len(i) == 6 else i for i in customer_id]
    customer_id =  [i if len(i) == 7 else None for i in customer_id]
    customer =     pd.read_pickle("./layers/silver/crm/customer.pkl")
    customer_ids = set(customer['customer_id'])
    customer_ids =  [i if i in customer_ids else None for i in customer_id]

    # ----- restaurant_id
    restaurant_id =  orde['restaurant_id'].astype('str').apply(lambda x: m.fix_ids(m.letter_cleaning(m.word_cleaning(m.value_cleaning(x)))))
    restaurant_id =  [i if len(i) == 4 else None for i in restaurant_id]
    restaurant =     pd.read_pickle("./layers/silver/crm/restaurant.pkl")
    restaurant_ids = set(restaurant['restaurant_id'])
    restaurant_id =  [i if i in restaurant_ids else None for i in restaurant_id]

    # ----- order_datetime
    order_datetime = orde['order_datetime'].astype('str').apply(lambda x: m.word_cleaning(m.value_cleaning_scm(x)))
    order_datetime = pd.to_datetime(order_datetime, format= "%Y-%m-%d %H:%M:%S", errors='coerce')

    # ----- payment_mode
    payment_mode = orde['payment_mode'].astype('str').apply(lambda x: m.word_cleaning(m.value_cleaning_scm(x)))
    payment_mode = [i if i in ['Card', 'UPI', 'Cash', 'NetBanking'] else None for i in payment_mode]

    # ----- is_delivery
    is_delivery = orde['is_delivery'].astype('str').apply(lambda x: m.word_cleaning(m.value_cleaning_scm(x)))
    is_delivery = [int(i) if i in ['1','0'] else None for i in is_delivery]

    # ----- delivery_id
    delivery_id =       orde['delivery_id'].astype('str').apply(lambda x: m.letter_cleaning(m.word_cleaning(m.value_cleaning(x))))
    delivery_id =       ['D0'+i[1::] if len(i) == 4 else i for i in delivery_id ]
    delivery_id =       [i if len(i) == 5 else None for i in delivery_id]
    delivery_partners = pd.read_pickle("./layers/silver/crm/delivery_partner.pkl")
    delivery_ids =      set(delivery_partners['delivery_id'])
    delivery_id =       [None if i not in delivery_ids else i for i in delivery_id]

    # ----- order_total
    order_total = orde['order_total'].astype('str').apply(lambda x: m.word_cleaning(m.value_cleaning_d(x)))
    order_total = [None if i in ('nan','na','n') or float(i) <= 0 else float(i) for i in order_total]

    # ----- order_status
    order_status = orde['order_status'].astype('str').apply(lambda x: m.word_cleaning(m.value_cleaning(x)))
    order_status = [i if i in ['Completed', 'Delivered', 'Preparing', 'Refunded', 'Cancelled'] else None for i in order_status]

    order = pd.DataFrame({
        'order_id':       order_id,
        'customer_id':    customer_id,
        'restaurant_id':  restaurant_id,
        'order_datatime': order_datetime,
        'payment_mode':   payment_mode,
        'is_delivery':    is_delivery,
        'delivery_id':    delivery_id,
        'order_total':    order_total,
        'order_status':   order_status
    })

    order = order.dropna(subset= 'order_id').drop_duplicates(subset = 'order_id').sort_values(by = 'order_id').reset_index().drop('index', axis = 1)

    print("=========================== SAVING orders.pkl")
    order.to_pickle(r"layers/silver/crm/order.pkl")

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

    print("===================== CLEANING menu_items.csv")
    # item_id
    item_id = menu['item_id'].astype('str').apply(lambda x: m.word_cleaning(m.value_cleaning(x)))
    item_id = [i[0]+'0'+i[1::] if len(i) == 4 else i if len(i) == 5 else None for i in item_id]

    # item_name
    item_name = menu['item_name'].astype('str').apply(lambda x: m.word_cleaning(m.value_cleaning_s(x)))

    # category
    category = menu['category'].astype('str').apply(lambda x: m.word_cleaning(m.value_cleaning(x)))
    category = [ None if i in ('nan','na','n') else i for i in category]

    # price
    price = menu['price'].astype('str').apply(lambda x:  m.word_cleaning(m.value_cleaning_d(x)))
    price = [None if i in ('nan','na','n') or float(i) <= 0 else float(i) for i in price]

    # supplier_id
    supplier_id = menu['supplier_id'].astype('str').apply(lambda x:  m.letter_cleaning(m.word_cleaning(m.value_cleaning_d(x))))
    supplier_id = [i[0]+'0'+i[1::] if len(i) == 4 else i if len(i) == 5 else None for i in supplier_id]

    menu_item = pd.DataFrame({
        'item_id': item_id,
        'item_name': item_name,
        'category': category,
        'price': price,
        'supplier_id': supplier_id
    })

    menu_item = menu_item.dropna(subset= 'item_id').drop_duplicates(subset= 'item_id').sort_values(by = 'item_id').reset_index().drop('index', axis = 1)

    print("======================== SAVING menu_item.pkl")
    menu_item.to_pickle(r"layers/silver/crm/menu_item.pkl")

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

    print("==================== CLEANING order_items.csv")
    # order_item_id
    order_item_id = ord_itm['order_item_id'].astype('str').apply(lambda x: m.letter_cleaning(m.word_cleaning(m.value_cleaning(x))))
    order_item_id = ['OI0'+i[2::] if len(i) == 8 else i for i in order_item_id]
    order_item_id = [i if len(i) == 9 else None for i in order_item_id]

    # order_id
    order_id =  ord_itm['order_id'].astype('str').apply(lambda x: m.letter_cleaning(m.word_cleaning(m.value_cleaning(x))))
    order_id =  ['O0'+i[1::] if len(i) == 7 else i for i in order_id]
    order_id =  [i if len(i) == 8 else None for i in order_id]
    order =     pd.read_pickle("./layers/silver/crm/order.pkl")
    order_ids = set(order['order_id'])
    order_id =  [i if i in order_ids else None for i in order_id]

    # item_id
    item_id =   ord_itm['item_id'].astype('str').apply(lambda x: m.word_cleaning(m.value_cleaning(x)))
    item_id =   [i[0]+'0'+i[1::] if len(i) == 4 else i if len(i) == 5 else None for i in item_id]
    menu_item = pd.read_pickle("./layers/silver/crm/menu_item.pkl")
    item_ids =  set(menu_item['item_id'])
    item_id =   [None if i not in item_ids else i for i in item_id]

    # quantity
    quantity = ord_itm['quantity'].astype('str').apply(lambda x: m.word_cleaning(m.value_cleaning(x)))
    quantity = [0 if i in ('nan', 'na','n') else 0 if len(i) >2 else int(i) for i in quantity]

    # unit_price
    unit_price = ord_itm['unit_price'].astype('str').apply(lambda x: m.word_cleaning(m.value_cleaning_d(x)))
    unit_price = [0 if i in ('nan','na','n','999','999999') else round(float(i),2) for i in unit_price]

    # unit_price
    line_total = ord_itm['line_total'].astype('str').apply(lambda x: m.word_cleaning(m.value_cleaning_d(x)))
    line_total = [0 if i in ('nan','na','n','999999','999') else round(float(i),2) for i in line_total]

    order_item = pd.DataFrame({
        'order_item_id': order_item_id,
        'order_id': order_id,
        'item_id': item_id,
        'quantity': quantity,
        'unit_price': unit_price,
        'line_total': line_total
    })

    order_item = order_item.dropna(subset = 'order_item_id').drop_duplicates(subset = 'order_item_id').sort_values(by = 'order_item_id').reset_index().drop('index', axis = 1)

    # price cleaning
    menu_item = pd.read_pickle("./Layers/silver/crm/menu_item.pkl")
    merge = pd.merge(order_item, menu_item, on = 'item_id',how= 'left')
    order_item['unit_price'] = round(merge['price']).where(merge['unit_price'] == 0, merge['unit_price'])
    order_item['unit_price'] = order_item['unit_price'].fillna(order_item['line_total']/order_item['quantity'])
    order_item['unit_price'] = order_item['unit_price'].replace(0, np.nan).pipe(pd.to_numeric, errors='coerce')

    # quantity cleaning
    order_item['quantity'] = (round(order_item['line_total']/order_item['unit_price'])).where(order_item['quantity'] == 0, order_item['quantity'])
    order_item['quantity'] = order_item['quantity'].fillna(0).astype(int)
    order_item['quantity'] = order_item['quantity'].replace(0, np.nan)

    # line_total_cleaning
    order_item['line_total'] = (round(order_item['quantity']*order_item['unit_price'],2)).where(order_item['line_total'] == 0, order_item['line_total'])
    order_item['line_total'] = order_item['line_total'].replace(0, np.nan)

    print("======================= SAVING order_item.pkl")
    order_item.to_pickle(r"layers/silver/crm/order_item.pkl")

    time2 = datetime.datetime.now()
    time = time2 - time1
    print("TABLE CLEANING TIME")
    print(time)
    print()

    # CRM TABLES CLEANING TIME
    crm_time2 = datetime.datetime.now()
    crm_time = crm_time2 - crm_time1
    print("CRM TABLES CLEANING TIME")
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
    supplier = pd.read_csv(path)


    print("======================= CLEANING supplier.csv")
    # supplier_id
    supplier_id = supplier['supplier_id'].astype('str').apply(lambda x: m.word_cleaning(m.value_cleaning(x)))
    supplier_id = [i if len(i) == 5 else None for i in supplier_id]

    # supplier_name
    supplier_name = supplier['supplier_name'].astype('str').apply(lambda x: m.word_cleaning(m.value_cleaning(x)))
    supplier_name = [None if i in ('nan','na','n') else i for i in supplier_name]

    # phone
    phone = supplier['phone'].astype('str').apply(lambda x: m.word_cleaning(m.value_cleaning(x)))
    phone = [i[2::] if len(i) == 12 else i[1::] if len(i) == 11 else None if i in ('nan','na','n') else i for i in phone]
    # city
    city = supplier['city'].astype('str').apply(lambda x: m.word_cleaning(m.value_cleaning(x)))
    city = [ None if i in ('nan','na','n') else i for i in city]

    supplier = pd.DataFrame({
        'supplier_id':   supplier_id,
        'supplier_name': supplier_name,
        'phone':         phone,
        'city':          city
    })

    supplier = supplier.dropna( subset= 'supplier_id').drop_duplicates(subset= 'supplier_id').sort_values(by = 'supplier_id').reset_index().drop('index', axis = 1)

    print("========================= SAVING supplier.pkl")
    supplier.to_pickle(r"layers/silver/erp/supplier.pkl")

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
    supplier_item = pd.read_csv(path)

    print("================= CLEANING supplier_items.csv")
    # item_id
    item_id = supplier_item['item_id'].astype('str').apply(lambda x: m.letter_cleaning(m.word_cleaning(m.value_cleaning(x))))
    item_id = ['M0'+i[1::] if len(i) == 4 else  None if i in ('nan','na','n') else i for i in item_id]

    # supplier_id
    supplier_id =  supplier_item['supplier_id'].astype('str').apply(lambda x: m.letter_cleaning(m.word_cleaning(m.value_cleaning(x))))
    supplier_id =  ['S0'+i[1::] if len(i) == 4 else None if i in ('nan','na','n') else i for i in supplier_id]
    supplier =     pd.read_pickle("./layers/silver/erp/supplier.pkl")
    supplier_ids = set(supplier['supplier_id'])
    supplier_id =  [i if i in supplier_ids else None for i in supplier_id]

    supplier_item = pd.DataFrame({
        'item_id':     item_id,
        'supplier_id': supplier_id
    }) 

    supplier_item = supplier_item.dropna(subset = 'item_id').drop_duplicates(subset = 'item_id').sort_values(by = 'item_id').reset_index().drop('index', axis = 1)

    print("==================== SAVING supplier_item.pkl")
    supplier_item.to_pickle(r"layers/silver/erp/supplier_item.pkl")

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
    inventory = pd.read_csv(path)

    print("====================== CLEANING inventory.csv")
    # restaurant_id
    restaurant_id =  inventory['restaurant_id'].astype('str').apply(lambda x: m.fix_ids(m.letter_cleaning(m.word_cleaning(m.value_cleaning(x)))))
    restaurant =     pd.read_pickle("./layers/silver/crm/restaurant.pkl")
    restaurant_ids = set(restaurant['restaurant_id'])
    restaurant_id =  [i if i in restaurant_ids else None for i in restaurant_id]

    # item_id
    item_id =       inventory['item_id'].astype('str').apply(lambda x: m.letter_cleaning(m.word_cleaning(m.value_cleaning(x))))
    item_id =       ['M0'+i[1::] if len(i) == 4 else i for i in item_id]
    itetm_id =      [i if len(i) == 5 else None for i in item_id]
    supplier_item = pd.read_pickle("./layers/silver/erp/supplier_item.pkl")
    item_ids =      set(supplier_item['item_id'])
    item_id =       [i if i in item_ids else None for i in item_id]

    # stock_qty
    stock_qty = inventory['stock_qty'].astype('str').apply(lambda x: m.word_cleaning(m.value_cleaning(x)))
    stock_qty = [ None if i in ('nan','na','999','999999') else i for i in stock_qty]

    # reorder_lavel
    reorder_level = inventory['reorder_level'].astype('str').apply(lambda x: m.word_cleaning(m.value_cleaning(x)))
    reorder_level = [None if i in ('nan','na','n') else i for i in reorder_level]

    inventory = pd.DataFrame({
        'restaurant_id': restaurant_id,
        'item_id': item_id,
        'stock_qty': stock_qty,
        'reorder_lavel': reorder_level
    }) 

    print("======================== SAVING inventory.pkl")
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
    employee = pd.read_csv(path)

    print("====================== CLEANING employees.csv")
    # emp_id
    emp_id = employee['emp_id'].astype('str').apply(lambda x: m.word_cleaning(m.value_cleaning(x)))
    emp_id = ['E0'+i[1::] if len(i) == 5 else i if len(i) == 6 else None for i in emp_id]

    # first_name
    first_name = employee['first_name'].astype('str').apply(lambda x: m.word_cleaning(m.value_cleaning(x)))
    first_name = [ None if i in ('nan','na','n') else i for i in first_name]

    # last_name
    last_name = employee['last_name'].astype('str').apply(lambda x: m.word_cleaning(m.value_cleaning(x)))
    last_name = [ None if i in ('nan','na','n') else i for i in last_name]

    # role
    role = employee['role'].astype('str').apply(lambda x: m.word_cleaning(m.value_cleaning_s(x)))
    role = [ None if i in ('nan','na','n') else i for i in role]

    # restaurant_id
    restaurant_id = employee['restaurant_id'].astype('str').apply(lambda x: m.letter_cleaning(m.word_cleaning(m.value_cleaning_s(x))))
    restaurant = pd.read_pickle("./layers/silver/crm/restaurant.pkl")
    restaurant_ids = set(restaurant['restaurant_id'])
    employee['restaurant_id'] =  [ i if i in restaurant_ids else None for i in restaurant_id]

    # hire_date
    hire_date = employee['hire_date'].astype('str').apply(lambda x: m.word_cleaning(m.value_cleaning_m(x)))
    hire_date = pd.to_datetime(hire_date, format = "%Y-%m-%d", errors= 'coerce')

    # salary
    salary = employee['salary'].astype('str').apply(lambda x: m.word_cleaning(m.value_cleaning_d(x)))
    salary = [None if i in ('nan','na','n','999') else float(i) for i in salary]

    employee = pd.DataFrame({
        'emp_id':        emp_id,
        'first_name':    first_name,
        'last_name':     last_name,
        'role':          role,
        'restaurant_id': restaurant_id,
        'hire_date':     hire_date,
        'salary':        salary
    })

    employee = employee.dropna(subset= 'emp_id').drop_duplicates(subset= 'emp_id').sort_values(by = 'emp_id').reset_index().drop('index',axis = 1)

    print("========================= SAVING employee.pkl")
    employee.to_pickle(r"layers/silver/erp/employee.pkl")

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

    print("=================== CLEANING kitchen_logs.csv")
    # kitchen_log_id
    kitchen_log_id = kic['kitchen_log_id'].astype('str').apply(lambda x: m.letter_cleaning(m.word_cleaning(m.value_cleaning(x))))
    kitchen_log_id = ['KL0'+i[2::] if len(i) == 9 else i for i in kitchen_log_id]
    kitchen_log_id = [i if len(i) == 10 else None for i in kitchen_log_id]

    # order_item_id
    order_item_id =  kic['order_item_id'].astype('str').apply(lambda x: m.letter_cleaning(m.word_cleaning(m.value_cleaning(x))))
    order_item_id =  ['OI0'+i[2::] if len(i) == 8 else i for i in order_item_id]
    order_item_id =  [i if len(i) == 9 else None for i in order_item_id]
    order_item =     pd.read_pickle("./layers/silver/crm/order_item.pkl")
    order_item_ids = set(order_item['order_item_id'])
    order_item_id =  [i if i in order_item_ids else None for i in order_item_id]

    # order_id
    order_id = kic['order_id'].astype('str').apply(lambda x: m.letter_cleaning(m.word_cleaning(m.value_cleaning(x))))
    order_id = ['O0'+i[1::] if len(i) == 7 else i for i in order_id]
    order_id = [i if len(i) == 8 else None for i in order_id]
    order =    pd.read_pickle("./layers/silver/crm/order.pkl")
    order_ids =  set(order['order_id'])
    order_id = [i if i in order_ids else None for i in order_id]

    # item_id
    item_id =   kic['item_id'].astype('str').apply(lambda x: m.letter_cleaning(m.word_cleaning(m.value_cleaning(x))))
    item_id =   ['M0'+i[1::] if len(i) == 4 else i for i in item_id]
    item_id =   [i if len(i) == 5 else None for i in item_id]
    menu_item = pd.read_pickle("./layers/silver/crm/menu_item.pkl")
    item_ids =  set(menu_item['item_id'])
    item_id =   [i if i in item_ids else None for i in item_id ]

    # started_at
    started_at = kic['started_at'].astype('str').apply(lambda x: m.word_cleaning(m.value_cleaning_scm(x)).replace('T',' '))
    started_at = pd.to_datetime(started_at, format = "%Y-%m-%d %H:%M:%S", errors = 'coerce')

    # completed_at
    completed_at = kic['completed_at'].astype('str').apply(lambda x: m.word_cleaning(m.value_cleaning_scm(x)).replace('T',' '))
    completed_at = pd.to_datetime(completed_at, format = "%Y-%m-%d %H:%M:%S", errors='coerce')

    # chef_id
    chef_id =  kic['chef_id'].astype('str').apply(lambda x: m.letter_cleaning(m.word_cleaning(m.value_cleaning(x))))
    chef_id =  ['E0'+i[1::] if len(i) == 5 else i for i in chef_id]
    chef_id =  [i if len(i) == 6 else None for i in chef_id]
    employee = pd.read_pickle("./layers/silver/erp/employee.pkl")
    chef_ids = set(employee['emp_id'])
    chef_id =  [i if i in chef_ids else None for i in chef_id]

    # stauts
    status = kic['status'].astype('str').apply(lambda x: m.word_cleaning(m.value_cleaning_scm(x)))
    status = [i if i in ('Completed', 'Started', 'Cancelled') else None for i in status]

    kitchen_log = pd.DataFrame({
        'kitchen_log_id': kitchen_log_id,
        'order_item_id': order_item_id,
        'order_id': order_id,
        'item_id': item_id,
        'started_at': started_at,
        'completed_at': completed_at,
        'chef_id': chef_id,
        'status': status
    })

    kitchen_log = kitchen_log.dropna(subset= 'kitchen_log_id').drop_duplicates(subset= 'kitchen_log_id').sort_values(by = 'kitchen_log_id').reset_index().drop('index', axis = 1)
    kitchen_log.to_pickle(r"layers/silver/erp/kitchen_log.pkl")

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