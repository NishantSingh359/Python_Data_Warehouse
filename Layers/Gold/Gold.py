
import logging
import datetime
import pandas as pd
import numpy as np


logging.basicConfig(
    level= logging.INFO,
    filemode = 'w',
    filename = 'Layers/gold/gold.log',
    format = "%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S"
)

if __name__ == "__main__":

    layer = "GLOD"
    gold_time1 = datetime.datetime.now()
    logging.info(f"{layer} | LAYER_START")  

    # ===================================================
    # ===================== DIM =========================
    # ===================================================


    logging.info("-" * 21)
    dim_time1 = datetime.datetime.now()
    domain = "DIM"
    logging.info(f"{layer} | {domain} | DOMAIN_START")  

    # ---------------------------------------------------
    # --------------------- dim_date --------------------
    # ---------------------------------------------------

    try:
        logging.info("-" * 21)

        time1 = datetime.datetime.now()
        table = "dim_date"

        step = "CREAT"
        logging.info(f"{layer} | {domain} | {step} | {table}")

        date_range = pd.date_range(start="2023-01-01", end="2024-05-31")

        dim_date = pd.DataFrame({
            "date": date_range
        })

        dim_date["date_key"] = dim_date["date"].dt.strftime("%Y%m%d").astype(int)
        dim_date["day"] = dim_date["date"].dt.day
        dim_date["month"] = dim_date["date"].dt.month
        dim_date["month_name"] = dim_date["date"].dt.month_name()
        dim_date["quarter"] = dim_date["date"].dt.quarter
        dim_date["year"] = dim_date["date"].dt.year
        dim_date["week_of_year"] = dim_date["date"].dt.isocalendar().week
        dim_date["is_weekend"] = dim_date["date"].dt.weekday >= 5

        dim_date = dim_date[
            ["date_key","date","day","month","month_name","quarter","year","week_of_year","is_weekend"]
        ]

        step = "SAVE"
        logging.info(f"{layer} | {domain} | {step} | {table}")
        dim_date.to_pickle(r"Layers/gold/dim_date.pkl")

        step = "SHAPE"
        before = dim_date.shape
        logging.info(f"{layer} | {domain} | {step} | {table} | rows={before[0]} columns={before[1]}")

        step = "TIME"
        time2 = datetime.datetime.now()
        time =  time2 - time1
        time = round(time.total_seconds(), 4)
        logging.info(f"{layer} | {domain} | {step} | {table} | duration_sec={time}")     
        logging.info("-" * 21)

    except Exception as e:
        logging.exception(
            f"{layer} | {domain} | {step} | {table} | "
            f"error_type={type(e).__name__} message={e}"
        )


    # ---------------------------------------------------
    # ----------------- dim_payment_mode ----------------
    # ---------------------------------------------------

    try:
        time1 = datetime.datetime.now()
        table = "dim_payment_mode"

        step = "CREAT"        
        logging.info(f"{layer} | {domain} | {step} | {table}")

        payment_key = [1,2,3,4]
        payment_mode = ['wallet', 'upi', 'cash', 'card']

        dim_payment_mode = pd.DataFrame({
            'payment_key': payment_key,
            'payment_mode':payment_mode
        })

        step = "SAVE"
        logging.info(f"{layer} | {domain} | {step} | {table}")
        dim_payment_mode.to_pickle(r"Layers/gold/dim_payment_mode.pkl")

        step = "SHAPE"
        before = dim_payment_mode.shape
        logging.info(f"{layer} | {domain} | {step} | {table} | rows={before[0]} columns={before[1]}")

        step = "TIME"
        time2 = datetime.datetime.now()
        time =  time2 - time1
        time = round(time.total_seconds(), 4)
        logging.info(f"{layer} | {domain} | {step} | {table} | duration_sec={time}")     
        logging.info("-" * 21)

    except Exception as e:
        logging.exception(
            f"{layer} | {domain} | {step} | {table} | "
            f"error_type={type(e).__name__} message={e}"
        )


    # ---------------------------------------------------
    # ----------------- dim_order_status ----------------
    # ---------------------------------------------------

    try:
        time1 = datetime.datetime.now()
        table = "dim_order_status"

        step = "CREAT"
        logging.info(f"{layer} | {domain} | {step} | {table}")

        status_key = [1,2,3]
        order_status = ['completed', 'cancelled', 'failed']

        dim_order_status = pd.DataFrame({
            'status_key':  status_key,
            'order_status':order_status
        })

        step = "SAVE"
        logging.info(f"{layer} | {domain} | {step} | {table}")
        dim_order_status.to_pickle(r"Layers/gold/dim_order_status.pkl")

        step = "SHAPE"
        before = dim_order_status.shape
        logging.info(f"{layer} | {domain} | {step} | {table} | rows={before[0]} columns={before[1]}")

        step = "TIME"
        time2 = datetime.datetime.now()
        time =  time2 - time1
        time = round(time.total_seconds(), 4)
        logging.info(f"{layer} | {domain} | {step} | {table} | duration_sec={time}")     
        logging.info("-" * 21)

    except Exception as e:
        logging.exception(
            f"{layer} | {domain} | {step} | {table} | "
            f"error_type={type(e).__name__} message={e}"
        )


    # ---------------------------------------------------
    # --------------- dim_restaurants.pkl ---------------
    # ---------------------------------------------------

    try:
        time1 = datetime.datetime.now()
        table = "dim_restaurants"

        step = "LOAD"
        logging.info(f"{layer} | {domain} | {step} | restaurants")
        path = r"Layers/silver/erp/restaurants.pkl"
        res = pd.read_pickle(path)

        step = "CREAT"
        logging.info(f"{layer} | {domain} | {step} | {table}")
        res['restaurant_key'] = res.index + 1

        restaurant_key =  res['restaurant_key']
        restaurant_id =   res['restaurant_id']
        restaurant_name = res['restaurant_name']
        city =            res['city']
        restaurant_type = res['restaurant_type']
        open_date =       res['open_date']

        dim_restaurants = pd.DataFrame({
            'restaurant_key': restaurant_key,
            'restaurant_id':  restaurant_id,
            'restaurant_name':restaurant_name,
            'city':           city,
            'restaurant_type':restaurant_type,
            'open_date':      open_date
        })

        step = "SAVE"
        logging.info(f"{layer} | {domain} | {step} | {table}")
        dim_restaurants.to_pickle(r"Layers/gold/dim_restaurants.pkl")

        step = "SHAPE"
        before = dim_restaurants.shape
        logging.info(f"{layer} | {domain} | {step} | {table} | rows={before[0]} columns={before[1]}")

        step = "TIME"
        time2 = datetime.datetime.now()
        time =  time2 - time1
        time = round(time.total_seconds(), 4)
        logging.info(f"{layer} | {domain} | {step} | {table} | duration_sec={time}")     
        logging.info("-" * 21)

    except Exception as e:
        logging.exception(
            f"{layer} | {domain} | {step} | {table} | "
            f"error_type={type(e).__name__} message={e}"
        )


    # ---------------------------------------------------
    # ------------------ dim_employees ------------------
    # ---------------------------------------------------

    try:
        time1 = datetime.datetime.now()
        table = "dim_employees"

        step = "LOAD"
        logging.info(f"{layer} | {domain} | {step} | employees")
        path = r"Layers/silver/erp/employees.pkl"
        emp = pd.read_pickle(path)

        step = "CREAT"
        logging.info(f"{layer} | {domain} | {step} | {table}")
        emp['emp_key'] = emp.index + 1

        emp_key = emp['emp_key']
        emp_id =  emp['emp_id']
        role =    emp['role']

        dim_employees = pd.DataFrame({
            'emp_key':emp_key,
            'emp_id': emp_id,
            'role':   role
        })

        step = "SAVE"
        logging.info(f"{layer} | {domain} | {step} | {table}")
        dim_employees.to_pickle(r"Layers/gold/dim_employees.pkl")

        step = "SHAPE"
        before = dim_employees.shape
        logging.info(f"{layer} | {domain} | {step} | {table} | rows={before[0]} columns={before[1]}")

        step = "TIME"
        time2 = datetime.datetime.now()
        time =  time2 - time1
        time = round(time.total_seconds(), 4)
        logging.info(f"{layer} | {domain} | {step} | {table} | duration_sec={time}")     
        logging.info("-" * 21)

    except Exception as e:
        logging.exception(
            f"{layer} | {domain} | {step} | {table} | "
            f"error_type={type(e).__name__} message={e}"
        )


    # ---------------------------------------------------
    # ------------------ dim_customers ------------------
    # ---------------------------------------------------

    try:
        time1 = datetime.datetime.now()
        table = "dim_customers"

        step = "LOAD"
        logging.info(f"{layer} | {domain} | {step} | customers")
        path = r"Layers/silver/crm/customers.pkl"
        cust = pd.read_pickle(path)

        step = "CREAT"
        logging.info(f"{layer} | {domain} | {step} | {table}")
        cust['customer_key'] = cust.index + 1

        customer_key = cust['customer_key']
        customer_id = cust['customer_id']
        city = cust['city']
        created_at = cust['created_at']

        dim_customers = pd.DataFrame({
            'customer_key':customer_key,
            'customer_id':customer_id,
            'city':city,
            'created_at':created_at
        })

        step = "SAVE"
        logging.info(f"{layer} | {domain} | {step} | {table}")
        dim_customers.to_pickle(r"Layers/gold/dim_customers.pkl")

        step = "SHAPE"
        before = dim_customers.shape
        logging.info(f"{layer} | {domain} | {step} | {table} | rows={before[0]} columns={before[1]}")

        step = "TIME"
        time2 = datetime.datetime.now()
        time =  time2 - time1
        time = round(time.total_seconds(), 4)
        logging.info(f"{layer} | {domain} | {step} | {table} | duration_sec={time}")     
        logging.info("-" * 21)

    except Exception as e:
        logging.exception(
            f"{layer} | {domain} | {step} | {table} | "
            f"error_type={type(e).__name__} message={e}"
        )

    # ---------------------------------------------------
    # ------------------ dim_menu_item ------------------
    # ---------------------------------------------------

    try:
        time1 = datetime.datetime.now()
        table = "dim_menu_items"

        step = "LOAD"
        logging.info(f"{layer} | {domain} | {step} | menu_items")
        path = r"Layers/silver/erp/menu_items.pkl"
        menu = pd.read_pickle(path)

        step = "CREAT"
        logging.info(f"{layer} | {domain} | {step} | {table}")
        menu['item_key'] = menu.index + 1

        item_key =  menu['item_key']
        item_id =   menu['item_id']
        item_name = menu['item_name']
        category =  menu['category']
        cuisine =   menu['cuisine']

        dim_menu_items = pd.DataFrame({
            'item_key':item_key,
            'item_id':item_id,
            'item_name':item_name,
            'category':category,
            'cuisine':cuisine
        })

        step = "SAVE"
        logging.info(f"{layer} | {domain} | {step} | {table}")
        dim_menu_items.to_pickle(r"Layers/gold/dim_menu_items.pkl")

        step = "SHAPE"
        before = dim_menu_items.shape
        logging.info(f"{layer} | {domain} | {step} | {table} | rows={before[0]} columns={before[1]}")

        step = "TIME"
        time2 = datetime.datetime.now()
        time =  time2 - time1
        time = round(time.total_seconds(), 4)
        logging.info(f"{layer} | {domain} | {step} | {table} | duration_sec={time}")     
        logging.info("-" * 21)

    except Exception as e:
        logging.exception(
            f"{layer} | {domain} | {step} | {table} | "
            f"error_type={type(e).__name__} message={e}"
        )


    # ---------------------------------------------------
    # -------------- dim_delivery_partners --------------
    # ---------------------------------------------------

    try:
        time1 = datetime.datetime.now()
        table = "dim_delivery_partners"

        step = "LOAD"
        logging.info(f"{layer} | {domain} | {step} | delivery_partners")
        path = r"Layers/silver/erp/delivery_partners.pkl"
        del_part = pd.read_pickle(path)

        step = "CREAT"
        logging.info(f"{layer} | {domain} | {step} | {table}")
        del_part['delivery_partner_key'] = del_part.index + 1

        delivery_partner_key = del_part['delivery_partner_key']
        delivery_partner_id = del_part['delivery_partner_id']
        partner_type = del_part['partner_type']
        vehicle_type = del_part['vehicle_type']

        dim_delivery_partners = pd.DataFrame({
            'delivery_partner_key':delivery_partner_key,
            'delivery_partner_id':delivery_partner_id,
            'partner_type':partner_type,
            'vehicle_type':vehicle_type
        })

        step = "SAVE"
        logging.info(f"{layer} | {domain} | {step} | {table}")
        dim_delivery_partners.to_pickle(r"Layers/gold/dim_delivery_partners.pkl")

        step = "SHAPE"
        before = dim_delivery_partners.shape
        logging.info(f"{layer} | {domain} | {step} | {table} | rows={before[0]} columns={before[1]}")

        step = "TIME"
        time2 = datetime.datetime.now()
        time =  time2 - time1
        time = round(time.total_seconds(), 4)
        logging.info(f"{layer} | {domain} | {step} | {table} | duration_sec={time}")     
        logging.info("-" * 21)
        
    except Exception as e:
        logging.exception(
            f"{layer} | {domain} | {step} | {table} | "
            f"error_type={type(e).__name__} message={e}"
        )

    dim_time2 = datetime.datetime.now()
    dim_time =  dim_time2 - dim_time1
    dim_time = round(dim_time.total_seconds(), 4)
    logging.info(f"{layer} | {domain} | DOMAIN_END | duration_sec={dim_time}")



    # ===================================================
    # ==================== FACT =========================
    # ===================================================

    logging.info("-" * 21)
    fact_time1 = datetime.datetime.now()
    domain = "FACT"
    logging.info(f"{layer} | {domain} | DOMAIN_START")  

    # ---------------------------------------------------
    # ------------------- fact_sales --------------------
    # ---------------------------------------------------

    try:

        logging.info("-" * 21)
        table = "fact_sales"
        
        step = "LOAD"
        logging.info(f"{layer} | {domain} | {step} | kitchen_logs, order_items, orders")        
        kic_path = r"Layers/silver/crm/kitchen_logs.pkl"
        ord_itm_path = r"Layers/silver/crm/order_items.pkl"
        ordr_path = r"Layers/silver/crm/orders.pkl"

        kic = pd.read_pickle(kic_path)
        ord_itm = pd.read_pickle(ord_itm_path)
        ordr = pd.read_pickle(ordr_path)

        step = "CREAT"
        logging.info(f"{layer} | {domain} | {step} | {table}")
        fact_sales = kic.merge(ord_itm, on='order_item_id', how='left')
        fact_sales = fact_sales.merge(ordr, on='order_id', how='left')

        pay_mode = pd.read_pickle("Layers/gold/dim_payment_mode.pkl")
        fact_sales = fact_sales.merge(pay_mode, on='payment_mode', how='left')

        ord_status = pd.read_pickle("Layers/gold/dim_order_status.pkl")
        fact_sales = fact_sales.merge(ord_status, on='order_status', how='left')

        cust = pd.read_pickle("Layers/gold/dim_customers.pkl")
        fact_sales = fact_sales.merge(cust[['customer_id','customer_key']], on='customer_id', how='left')

        res = pd.read_pickle("Layers/gold/dim_restaurants.pkl")
        fact_sales = fact_sales.merge(res[['restaurant_id','restaurant_key']], on='restaurant_id',how='left')

        menu = pd.read_pickle("Layers/gold/dim_menu_items.pkl")
        fact_sales = fact_sales.merge(menu[['item_id', 'item_key']], on='item_id', how='left')

        emp = pd.read_pickle("Layers/gold/dim_employees.pkl")
        fact_sales = fact_sales.merge(emp[['emp_id','emp_key']], left_on='chef_id', right_on='emp_id', how='left')

        del_part = pd.read_pickle("Layers/gold/dim_delivery_partners.pkl")
        fact_sales = fact_sales.merge(del_part[['delivery_partner_id', 'delivery_partner_key']], how='left')


        order_item_id = fact_sales['order_item_id']

        order_id =       fact_sales['order_id']
        order_datetime = fact_sales['order_datetime']
        order_date =     pd.to_datetime(fact_sales["order_datetime"]).dt.date

        date_key =              fact_sales["order_datetime"].dt.strftime("%Y%m%d").astype('Int64')
        customer_key =          fact_sales['customer_key'].astype('Int64')
        restaurant_key =        fact_sales['restaurant_key'].astype('Int16')
        item_key =              fact_sales['item_key'].astype('Int16')
        employee_key =          fact_sales['emp_key'].astype('Int64')
        delivery_partner_key = 	fact_sales['delivery_partner_key'].astype('Int32')
        payment_key =           fact_sales['payment_key'].astype('Int8')
        status_key =            fact_sales['status_key'].astype('Int8')

        quantity =   fact_sales['quantity']
        unit_price = fact_sales['unit_price']
        line_total = fact_sales['line_total']

        is_delivery = fact_sales['is_delivery']
        is_completed = fact_sales['status']


        fact_sales = pd.DataFrame({
            'order_item_id':order_item_id,
            'order_id':order_id,
            'order_datetime':order_datetime,
            'order_date':order_date,
            'date_key':date_key,
            'customer_key':customer_key,
            'restaurant_key':restaurant_key,
            'item_key':item_key,
            'employee_key':employee_key,
            'delivery_partner_key':delivery_partner_key,
            'payment_key':payment_key,
            'status_key':status_key,
            'quantity':quantity,
            'unit_price':unit_price,
            'line_total':line_total,
            'is_delivery':is_delivery,
            'is_completed':is_completed
        })

        step = "SAVE"
        logging.info(f"{layer} | {domain} | {step} | {table}")
        fact_sales.to_pickle(r"Layers/gold/fact_sales.pkl")

        step = "SHAPE"
        before = fact_sales.shape
        logging.info(f"{layer} | {domain} | {step} | {table} | rows={before[0]} columns={before[1]}")

        logging.info("-" * 21)

    except Exception as e:
        logging.exception(
            f"{layer} | {domain} | {step} | {table} | "
            f"error_type={type(e).__name__} message={e}"
        )

    fact_time2 = datetime.datetime.now()
    fact_time =  fact_time2 - fact_time1
    fact_time = round(fact_time.total_seconds(), 4)
    logging.info(f"{layer} | {domain} | DOMAIN_END | duration_sec={fact_time}")
    logging.info("-" * 21)

    gold_time2 = datetime.datetime.now()
    gold_time = gold_time2 - gold_time1
    gold_time = round(gold_time.total_seconds(), 4)
    logging.info(f"{layer} | LAYER_END | duration_sec={gold_time}")

