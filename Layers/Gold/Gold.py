
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

    gold_time1 = datetime.datetime.now()
    logging.info(f"GOLD | LAYER_START")  

    # ===================================================
    # ===================== DIM =========================
    # ===================================================

    logging.info("-" * 5)
    dim_time1 = datetime.datetime.now()
    logging.info(f"GOLD | DIM | DOMAIN_START")  

    # ---------------------------------------------------
    # --------------------- dim_date --------------------
    # ---------------------------------------------------

    try:
        logging.info("-" * 21)

        time1 = datetime.datetime.now()
        table = "dim_date"

        logging.info(f"GOLD | DIM | CREAT | {table}")

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

        logging.info(f"GOLD | DIM | SAVE | {table}")
        dim_date.to_pickle(r"Layers/gold/dim_date.pkl")

        before = dim_date.shape
        logging.info(f"GOLD | DIM | SHAPE | {table} | rows={before[0]} columns={before[1]}")

        time2 = datetime.datetime.now()
        time =  time2 - time1
        time = round(time.total_seconds(), 4)
        logging.info(f"GOLD | DIM | TIME | {table} | duration_sec={time}")     
        logging.info("-" * 21)

    except ValueError as e:
        logging.error(f"ValueError: {e}")
    except KeyError as e:
        logging.error(f"KeyError: {e}")
    except AttributeError as e:
        logging.error(f"AttributeError: {e}")
    except NameError as e:
        logging.error(f"NameError: {e}")


    # ---------------------------------------------------
    # ----------------- dim_payment_mode ----------------
    # ---------------------------------------------------

    try:
        time1 = datetime.datetime.now()
        table = "dim_payment_mode"

        logging.info(f"GOLD | DIM | CREAT | {table}")

        payment_key = [1,2,3,4]
        payment_mode = ['wallet', 'upi', 'cash', 'card']

        dim_payment_mode = pd.DataFrame({
            'payment_key': payment_key,
            'payment_mode':payment_mode
        })

        logging.info(f"GOLD | DIM | SAVE | {table}")
        dim_payment_mode.to_pickle(r"Layers/gold/dim_payment_mode.pkl")

        before = dim_payment_mode.shape
        logging.info(f"GOLD | DIM | SHAPE | {table} | rows={before[0]} columns={before[1]}")

        time2 = datetime.datetime.now()
        time =  time2 - time1
        time = round(time.total_seconds(), 4)
        logging.info(f"GOLD | DIM | TIME | {table} | duration_sec={time}")     
        logging.info("-" * 21)

    except ValueError as e:
        logging.error(f"ValueError: {e}")
    except KeyError as e:
        logging.error(f"KeyError: {e}")
    except AttributeError as e:
        logging.error(f"AttributeError: {e}")
    except NameError as e:
        logging.error(f"NameError: {e}")


    # ---------------------------------------------------
    # ----------------- dim_order_status ----------------
    # ---------------------------------------------------

    try:
        time1 = datetime.datetime.now()
        table = "dim_order_status"

        logging.info(f"GOLD | DIM | CREAT | {table}")

        status_key = [1,2,3]
        order_status = ['completed', 'cancelled', 'failed']

        dim_order_status = pd.DataFrame({
            'status_key':  status_key,
            'order_status':order_status
        })

        logging.info(f"GOLD | DIM | SAVE | {table}")
        dim_order_status.to_pickle(r"Layers/gold/dim_order_status.pkl")

        before = dim_order_status.shape
        logging.info(f"GOLD | DIM | SHAPE | {table} | rows={before[0]} columns={before[1]}")

        time2 = datetime.datetime.now()
        time =  time2 - time1
        time = round(time.total_seconds(), 4)
        logging.info(f"GOLD | DIM | TIME | {table} | duration_sec={time}")     
        logging.info("-" * 21)

    except ValueError as e:
        logging.error(f"ValueError: {e}")
    except KeyError as e:
        logging.error(f"KeyError: {e}")
    except AttributeError as e:
        logging.error(f"AttributeError: {e}")
    except NameError as e:
        logging.error(f"NameError: {e}")


    # ---------------------------------------------------
    # --------------- dim_restaurants.pkl ---------------
    # ---------------------------------------------------

    try:
        time1 = datetime.datetime.now()
        table = "dim_restaurants"

        logging.info(f"GOLD | DIM | LOAD | restaurants")
        path = r"Layers/silver/erp/restaurants.pkl"
        res = pd.read_pickle(path)

        logging.info(f"GOLD | DIM | CREAT | {table}")
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

        logging.info(f"GOLD | DIM | SAVE | {table}")
        dim_restaurants.to_pickle(r"Layers/gold/dim_restaurants.pkl")

        before = dim_restaurants.shape
        logging.info(f"GOLD | DIM | SHAPE | {table} | rows={before[0]} columns={before[1]}")

        time2 = datetime.datetime.now()
        time =  time2 - time1
        time = round(time.total_seconds(), 4)
        logging.info(f"GOLD | DIM | TIME | {table} | duration_sec={time}")     
        logging.info("-" * 21)

    except FileNotFoundError as e:
        logging.error(f"FileNotFoundError: {e}")
    except ValueError as e:
        logging.error(f"ValueError: {e}")
    except KeyError as e:
        logging.error(f"KeyError: {e}")
    except AttributeError as e:
        logging.error(f"AttributeError: {e}")
    except NameError as e:
        logging.error(f"NameError: {e}")


    # ---------------------------------------------------
    # ------------------ dim_employees ------------------
    # ---------------------------------------------------

    try:
        time1 = datetime.datetime.now()
        table = "dim_employees"

        logging.info(f"GOLD | DIM | LOAD | employees")
        path = r"Layers/silver/erp/employees.pkl"
        emp = pd.read_pickle(path)

        logging.info(f"GOLD | DIM | CREAT | {table}")
        emp['emp_key'] = emp.index + 1

        emp_key = emp['emp_key']
        emp_id =  emp['emp_id']
        role =    emp['role']

        dim_employees = pd.DataFrame({
            'emp_key':emp_key,
            'emp_id': emp_id,
            'role':   role
        })

        logging.info(f"GOLD | DIM | SAVE | {table}")
        dim_employees.to_pickle(r"Layers/gold/dim_employees.pkl")

        before = dim_employees.shape
        logging.info(f"GOLD | DIM | SHAPE | {table} | rows={before[0]} columns={before[1]}")

        time2 = datetime.datetime.now()
        time =  time2 - time1
        time = round(time.total_seconds(), 4)
        logging.info(f"GOLD | DIM | TIME | {table} | duration_sec={time}")     
        logging.info("-" * 21)

    except FileNotFoundError as e:
        logging.error(f"FileNotFoundError: {e}")
    except ValueError as e:
        logging.error(f"ValueError: {e}")
    except KeyError as e:
        logging.error(f"KeyError: {e}")
    except AttributeError as e:
        logging.error(f"AttributeError: {e}")
    except NameError as e:
        logging.error(f"NameError: {e}")


    # ---------------------------------------------------
    # ------------------ dim_customers ------------------
    # ---------------------------------------------------

    try:
        time1 = datetime.datetime.now()
        table = "dim_customers"

        logging.info(f"GOLD | DIM | LOAD | customers")
        path = r"Layers/silver/crm/customers.pkl"
        cust = pd.read_pickle(path)

        logging.info(f"GOLD | DIM | CREAT | {table}")
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

        logging.info(f"GOLD | DIM | SAVE | {table}")
        dim_customers.to_pickle(r"Layers/gold/dim_customers.pkl")

        before = dim_customers.shape
        logging.info(f"GOLD | DIM | SHAPE | {table} | rows={before[0]} columns={before[1]}")

        time2 = datetime.datetime.now()
        time =  time2 - time1
        time = round(time.total_seconds(), 4)
        logging.info(f"GOLD | DIM | TIME | {table} | duration_sec={time}")     
        logging.info("-" * 21)

    except FileNotFoundError as e:
        logging.error(f"FileNotFoundError: {e}")
    except ValueError as e:
        logging.error(f"ValueError: {e}")
    except KeyError as e:
        logging.error(f"KeyError: {e}")
    except AttributeError as e:
        logging.error(f"AttributeError: {e}")
    except NameError as e:
        logging.error(f"NameError: {e}")

    # ---------------------------------------------------
    # ------------------ dim_menu_item ------------------
    # ---------------------------------------------------

    try:
        time1 = datetime.datetime.now()
        table = "dim_menu_items"

        logging.info(f"GOLD | DIM | LOAD | menu_items")
        path = r"Layers/silver/erp/menu_items.pkl"
        menu = pd.read_pickle(path)

        logging.info(f"GOLD | DIM | CREAT | {table}")
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

        logging.info(f"GOLD | DIM | SAVE | {table}")
        dim_menu_items.to_pickle(r"Layers/gold/dim_menu_items.pkl")

        before = dim_menu_items.shape
        logging.info(f"GOLD | DIM | SHAPE | {table} | rows={before[0]} columns={before[1]}")

        time2 = datetime.datetime.now()
        time =  time2 - time1
        time = round(time.total_seconds(), 4)
        logging.info(f"GOLD | DIM | TIME | {table} | duration_sec={time}")     
        logging.info("-" * 21)

    except FileNotFoundError as e:
        logging.error(f"FileNotFoundError: {e}")
    except ValueError as e:
        logging.error(f"ValueError: {e}")
    except KeyError as e:
        logging.error(f"KeyError: {e}")
    except AttributeError as e:
        logging.error(f"AttributeError: {e}")
    except NameError as e:
        logging.error(f"NameError: {e}")


    # ---------------------------------------------------
    # -------------- dim_delivery_partners --------------
    # ---------------------------------------------------

    try:
        time1 = datetime.datetime.now()
        table = "dim_delivery_partners"

        logging.info(f"GOLD | DIM | LOAD | delivery_partners")
        path = r"Layers/silver/erp/delivery_partners.pkl"
        del_part = pd.read_pickle(path)

        logging.info(f"GOLD | DIM | CREAT | {table}")
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

        logging.info(f"GOLD | DIM | SAVE | {table}")
        dim_delivery_partners.to_pickle(r"Layers/gold/dim_delivery_partners.pkl")

        before = dim_delivery_partners.shape
        logging.info(f"GOLD | DIM | SHAPE | {table} | rows={before[0]} columns={before[1]}")

        time2 = datetime.datetime.now()
        time =  time2 - time1
        time = round(time.total_seconds(), 4)
        logging.info(f"GOLD | DIM | TIME | {table} | duration_sec={time}")     
        logging.info("-" * 21)
        
    except FileNotFoundError as e:
        logging.error(f"FileNotFoundError: {e}")
    except ValueError as e:
        logging.error(f"ValueError: {e}")
    except KeyError as e:
        logging.error(f"KeyError: {e}")
    except AttributeError as e:
        logging.error(f"AttributeError: {e}")
    except NameError as e:
        logging.error(f"NameError: {e}")

    dim_time2 = datetime.datetime.now()
    dim_time =  dim_time2 - dim_time1
    dim_time = round(dim_time.total_seconds(), 4)
    logging.info(f"GOLD | DIM | DOMAIN_END | duration_sec={dim_time}")
    logging.info("-" * 5)


    # ===================================================
    # ==================== FACT =========================
    # ===================================================

    logging.info(f"GOLD | FACT | DOMAIN_START")  
    logging.info("-" * 21)

    # ---------------------------------------------------
    # ------------------- fact_sales --------------------
    # ---------------------------------------------------

    try:

        time1 = datetime.datetime.now()
        table = "fact_sales"

        logging.info(f"GOLD | FACT | LOAD | kitchen_logs, order_items, orders")        
        kic_path = r"Layers/silver/crm/kitchen_logs.pkl"
        ord_itm_path = r"Layers/silver/crm/order_items.pkl"
        ordr_path = r"Layers/silver/crm/orders.pkl"

        kic = pd.read_pickle(kic_path)
        ord_itm = pd.read_pickle(ord_itm_path)
        ordr = pd.read_pickle(ordr_path)

        logging.info(f"GOLD | FACT | CREAT | {table}")
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

        logging.info(f"GOLD | FACT | SAVE | {table}")
        fact_sales.to_pickle(r"Layers/gold/fact_sales.pkl")

        before = fact_sales.shape
        logging.info(f"GOLD | FACT | SHAPE | {table} | rows={before[0]} columns={before[1]}")

        time2 = datetime.datetime.now()
        time =  time2 - time1
        time = round(time.total_seconds(), 4)
        logging.info(f"GOLD | FACT | TIME | {table} | duration_sec={time}")     
        logging.info("-" * 21)

    except FileNotFoundError as e:
        logging.error(f"FileNotFoundError: {e}")
    except ValueError as e:
        logging.error(f"ValueError: {e}")
    except KeyError as e:
        logging.error(f"KeyError: {e}")
    except AttributeError as e:
        logging.error(f"AttributeError: {e}")
    except NameError as e:
        logging.error(f"NameError: {e}")

    logging.info(f"GOLD | FACT | DOMAIN_END")
    logging.info("-" * 5)

    gold_time2 = datetime.datetime.now()
    gold_time = gold_time2 - gold_time1
    gold_time = round(gold_time.total_seconds(), 4)
    logging.info(f"GOLD | LAYER_END | duration_sec={gold_time}")

