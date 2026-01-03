
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

    logging.info('=============================================')
    logging.info('================= GOLD LAYER ================')
    logging.info('=============================================')
    logging.info('')

    gold_time1 = datetime.datetime.now()

    logging.info('=============================================')
    logging.info('============== DIMENSION TABLES =============')
    logging.info('=============================================')
    logging.info('=====')

    dim_time1 = datetime.datetime.now()

    # ---------------------------------------------------
    # ------------------- dim_date.pkl ------------------
    # ---------------------------------------------------

    try:
        time1 = datetime.datetime.now()

        logging.info("======================= CREATING dim_date.pkl")

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

        logging.info("========================= SAVING dim_date.pkl")
        dim_date.to_pickle(r"Layers/gold/dim_date.pkl")

        before = dim_date.shape
        logging.info(f"TABLE ROWS & COLUMNS: {before[0]} & {before[1]}")

        time2 = datetime.datetime.now()
        time =  time2 - time1
        logging.info("TABLE CREATING TIME")
        logging.info(time)

    except ValueError as e:
        logging.error(f"ValueError: {e}")
    except KeyError as e:
        logging.error(f"KeyError: {e}")
    except AttributeError as e:
        logging.error(f"AttributeError: {e}")
    except NameError as e:
        logging.error(f"NameError: {e}")


    # ---------------------------------------------------
    # --------------- dim_order_status.pkl --------------
    # ---------------------------------------------------

    try:
        time1 = datetime.datetime.now()

        logging.info("=============== CREATING dim_order_status.pkl")

        payment_key = [1,2,3,4]
        payment_mode = ['wallet', 'upi', 'cash', 'card']

        dim_payment_mode = pd.DataFrame({
            'payment_key': payment_key,
            'payment_mode':payment_mode
        })

        logging.info("================= SAVING dim_payment_mode.pkl")
        dim_payment_mode.to_pickle(r"Layers/gold/dim_payment_mode.pkl")

        before = dim_payment_mode.shape
        logging.info(f"TABLE ROWS & COLUMNS: {before[0]} & {before[1]}")

        time2 = datetime.datetime.now()
        time =  time2 - time1
        logging.info("TABLE CREATING TIME")
        logging.info(time)

    except ValueError as e:
        logging.error(f"ValueError: {e}")
    except KeyError as e:
        logging.error(f"KeyError: {e}")
    except AttributeError as e:
        logging.error(f"AttributeError: {e}")
    except NameError as e:
        logging.error(f"NameError: {e}")

    # ---------------------------------------------------
    # --------------- dim_order_status.pkl --------------
    # ---------------------------------------------------

    try:
        time1 = datetime.datetime.now()

        logging.info("=============== CREATING dim_order_status.pkl")

        status_key = [1,2,3]
        order_status = ['completed', 'cancelled', 'failed']

        dim_order_status = pd.DataFrame({
            'status_key':  status_key,
            'order_status':order_status
        })

        logging.info("================= SAVING dim_order_status.pkl")
        dim_order_status.to_pickle(r"Layers/gold/dim_order_status.pkl")

        before = dim_order_status.shape
        logging.info(f"TABLE ROWS & COLUMNS: {before[0]} & {before[1]}")

        time2 = datetime.datetime.now()
        time =  time2 - time1
        logging.info("TABLE CREATING TIME")
        logging.info(time)

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

        logging.info("===================== LOADING restaurants.pkl")
        path = r"C:\Users\TUF\OneDrive\Documents\Code\Vs Code\Python_DataWarehouse\Layers\silver\erp\restaurants.pkl"
        res = pd.read_pickle(path)

        logging.info("================= CREATING dim_restaurant.pkl")
        res['restaurant_key'] = res.index + 1

        restaurant_key =  res['restaurant_key']
        restaurant_id =   res['restaurant_id']
        restaurant_name = res['restaurant_name']
        city =            res['city']
        restaurant_type = res['restaurant_type']
        open_date =       res['open_date']

        dim_restaurant = pd.DataFrame({
            'restaurant_key': restaurant_key,
            'restaurant_id':  restaurant_id,
            'restaurant_name':restaurant_name,
            'city':           city,
            'restaurant_type':restaurant_type,
            'open_date':      open_date
        })

        logging.info("=================== SAVING dim_restaurant.pkl")
        dim_restaurant.to_pickle(r"Layers/gold/dim_restaurant.pkl")

        before = dim_restaurant.shape
        logging.info(f"TABLE ROWS & COLUMNS: {before[0]} & {before[1]}")

        time2 = datetime.datetime.now()
        time =  time2 - time1
        logging.info("TABLE CREATING TIME")
        logging.info(time)

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
    # ---------------- dim_employees.pkl ----------------
    # ---------------------------------------------------

    try:
        time1 = datetime.datetime.now()

        logging.info("=================== LOADING dim_employees.pkl")
        path = r"C:\Users\TUF\OneDrive\Documents\Code\Vs Code\Python_DataWarehouse\Layers\silver\erp\employess.pkl"
        emp = pd.read_pickle(path)

        logging.info("================== CREATING dim_employees.pkl")
        emp['emp_key'] = emp.index + 1

        emp_key = emp['emp_key']
        emp_id =  emp['emp_id']
        role =    emp['role']

        dim_employees = pd.DataFrame({
            'emp_key':emp_key,
            'emp_id': emp_id,
            'role':   role
        })

        logging.info("==================== SAVING dim_employees.pkl")
        dim_employees.to_pickle(r"Layers/gold/dim_employees.pkl")

        before = dim_employees.shape
        logging.info(f"TABLE ROWS & COLUMNS: {before[0]} & {before[1]}")

        time2 = datetime.datetime.now()
        time =  time2 - time1
        logging.info("TABLE CREATING TIME")
        logging.info(time)

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
    # ---------------- dim_customer.pkl ----------------
    # ---------------------------------------------------

    try:
        time1 = datetime.datetime.now()

        logging.info("======================= LOADING customers.pkl")
        path = r"C:\Users\TUF\OneDrive\Documents\Code\Vs Code\Python_DataWarehouse\Layers\silver\crm\customers.pkl"
        cust = pd.read_pickle(path)

        logging.info("=================== CREATING dim_customer.pkl")
        cust['customer_key'] = cust.index + 1

        customer_key = cust['customer_key']
        customer_id = cust['customer_id']
        city = cust['city']
        created_at = cust['created_at']

        dim_customer = pd.DataFrame({
            'customer_key':customer_key,
            'customer_id':customer_id,
            'city':city,
            'created_at':created_at
        })

        logging.info("===================== SAVING dim_customer.pkl")
        dim_customer.to_pickle(r"Layers/gold/dim_customer.pkl")

        before = dim_customer.shape
        logging.info(f"TABLE ROWS & COLUMNS: {before[0]} & {before[1]}")

        time2 = datetime.datetime.now()
        time =  time2 - time1
        logging.info("TABLE CREATING TIME")
        logging.info(time)

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
    # ---------------- dim_employess.pkl ----------------
    # ---------------------------------------------------

    try:
        time1 = datetime.datetime.now()

        logging.info("=================== LOADING dim_employess.pkl")
        path = r"C:\Users\TUF\OneDrive\Documents\Code\Vs Code\Python_DataWarehouse\Layers\silver\erp\employess.pkl"
        emp = pd.read_pickle(path)

        logging.info("================== CREATING dim_employess.pkl")
        emp['emp_key'] = emp.index + 1

        emp_key = emp['emp_key']
        emp_id = emp['emp_id']
        role = emp['role']

        dim_customer = pd.DataFrame({
            'emp_key':emp_key,
            'emp_id':emp_id,
            'role':role,
        })

        logging.info("==================== SAVING dim_employess.pkl")
        dim_customer.to_pickle(r"Layers/gold/dim_employess.pkl")

        before = dim_customer.shape
        logging.info(f"TABLE ROWS & COLUMNS: {before[0]} & {before[1]}")

        time2 = datetime.datetime.now()
        time =  time2 - time1
        logging.info("TABLE CREATING TIME")
        logging.info(time)

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
    # ---------------- dim_menu_item.pkl ----------------
    # ---------------------------------------------------

    try:
        time1 = datetime.datetime.now()

        logging.info("====================== LOADING menu_items.pkl")
        path = r"C:\Users\TUF\OneDrive\Documents\Code\Vs Code\Python_DataWarehouse\Layers\silver\erp\menu_items.pkl"
        menu = pd.read_pickle(path)

        logging.info("================== CREATING dim_menu_item.pkl")
        menu['item_key'] = menu.index + 1

        item_key =  menu['item_key']
        item_id =   menu['item_id']
        item_name = menu['item_name']
        category =  menu['category']
        cuisine =   menu['cuisine']

        dim_menu_item = pd.DataFrame({
            'item_key':item_key,
            'item_id':item_id,
            'item_name':item_name,
            'category':category,
            'cuisine':cuisine
        })

        logging.info("==================== SAVING dim_menu_item.pkl")
        dim_menu_item.to_pickle(r"Layers/gold/dim_menu_item.pkl")

        before = dim_menu_item.shape
        logging.info(f"TABLE ROWS & COLUMNS: {before[0]} & {before[1]}")

        time2 = datetime.datetime.now()
        time =  time2 - time1
        logging.info("TABLE CREATING TIME")
        logging.info(time)

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
    # ------------ dim_delivery_partners.pkl ------------
    # ---------------------------------------------------

    try:
        time1 = datetime.datetime.now()

        logging.info("=============== LOADING delivery_partners.pkl")
        path = r"C:\Users\TUF\OneDrive\Documents\Code\Vs Code\Python_DataWarehouse\Layers\silver\erp\delivery_partners.pkl"
        del_part = pd.read_pickle(path)

        logging.info("=========== CREATING dim_delivery_partner.pkl")
        del_part['delivery_partner_key'] = del_part.index + 1

        delivery_partner_key = del_part['delivery_partner_key']
        delivery_partner_id = del_part['delivery_partner_id']
        partner_type = del_part['partner_type']
        vehicle_type = del_part['vehicle_type']

        dim_delivery_partner = pd.DataFrame({
            'delivery_partner_key':delivery_partner_key,
            'delivery_partner_id':delivery_partner_id,
            'partner_type':partner_type,
            'vehicle_type':vehicle_type
        })

        logging.info("============= SAVING dim_delivery_partner.pkl")
        dim_delivery_partner.to_pickle(r"Layers/gold/dim_delivery_partner.pkl")

        before = dim_delivery_partner.shape
        logging.info(f"TABLE ROWS & COLUMNS: {before[0]} & {before[1]}")

        time2 = datetime.datetime.now()
        time =  time2 - time1
        logging.info("TABLE CREATING TIME")
        logging.info(time)
        logging.info('')
        
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
    logging.info("DIMENSION TABLES CREATING TIME")
    logging.info(dim_time)   
    logging.info('')

    # ---------------------------------------------------
    # ------------------ fact_sales.pkl -----------------
    # ---------------------------------------------------

    logging.info('=============================================')
    logging.info('================= FACT TABLE ================')
    logging.info('=============================================')
    logging.info('=====')

    time1 = datetime.datetime.now()
    try:

        logging.info("LOAD & JOIN kitchen_logs, order_items, orders")
        kic = pd.read_pickle(r"C:\Users\TUF\OneDrive\Documents\Code\Vs Code\Python_DataWarehouse\Layers\silver\crm\kitchen_logs.pkl")
        ord_itm = pd.read_pickle(r"C:\Users\TUF\OneDrive\Documents\Code\Vs Code\Python_DataWarehouse\Layers\silver\crm\order_items.pkl")
        order = pd.read_pickle(r"C:\Users\TUF\OneDrive\Documents\Code\Vs Code\Python_DataWarehouse\Layers\silver\crm\orders.pkl")
        fact_sales = kic.merge(ord_itm, on='order_item_id', how='left')
        fact_sales = fact_sales.merge(order, on='order_id', how='left')

        logging.info("=========== LOAD & JOIN dim_payment_modes.pkl")
        pay_mode = pd.read_pickle("Layers/gold/dim_payment_mode.pkl")
        fact_sales = fact_sales.merge(pay_mode, on='payment_mode', how='left')

        logging.info("============ LOAD & JOIN dim_order_status.pkl")
        ord_status = pd.read_pickle("Layers/gold/dim_order_status.pkl")
        fact_sales = fact_sales.merge(ord_status, on='order_status', how='left')

        logging.info("=============== LOAD & JOIN dim_customers.pkl")
        cust = pd.read_pickle("Layers/gold/dim_customer.pkl")
        fact_sales = fact_sales.merge(cust[['customer_id','customer_key']], on='customer_id', how='left')

        logging.info("============= LOAD & JOIN dim_restaurents.pkl")
        res = pd.read_pickle("Layers/gold/dim_restaurant.pkl")
        fact_sales = fact_sales.merge(res[['restaurant_id','restaurant_key']], on='restaurant_id',how='left')

        logging.info("============== LOAD & JOIN dim_menu_items.pkl")
        menu = pd.read_pickle("Layers/gold/dim_menu_item.pkl")
        fact_sales = fact_sales.merge(menu[['item_id', 'item_key']], on='item_id', how='left')

        logging.info("=============== LOAD & JOIN dim_employess.pkl")
        emp = pd.read_pickle("Layers/gold/dim_employess.pkl")
        fact_sales = fact_sales.merge(emp[['emp_id','emp_key']], left_on='chef_id', right_on='emp_id', how='left')

        logging.info("======= LOAD & JOIN dim_delivery_partners.pkl")
        del_part = pd.read_pickle("Layers/gold/dim_delivery_partner.pkl")
        fact_sales = fact_sales.merge(del_part[['delivery_partner_id', 'delivery_partner_key']], how='left')


        logging.info("============================ Assign Variables")
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

        logging.info("========================= CREATING DATA_FRAME")
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

        logging.info("======================= SAVING fact_sales.pkl")
        fact_sales.to_pickle(r"Layers/gold/fact_sales.pkl")

        before = fact_sales.shape
        logging.info(f"TABLE ROWS & COLUMNS: {before[0]} & {before[1]}")

        time2 = datetime.datetime.now()
        time =  time2 - time1
        logging.info("TABLE CREATING TIME")
        logging.info(time)
        logging.info('')

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

    gold_time2 = datetime.datetime.now()
    gold_time = gold_time2 - gold_time1
    logging.info("TOTAL GOLD LAYER TIME")
    logging.info(gold_time)
    logging.info('')

    logging.info('=============================================')
    logging.info('============ GOLD LAYER COMPLETED ===========')
    logging.info('=============================================')  