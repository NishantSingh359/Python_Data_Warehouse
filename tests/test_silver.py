import pandas as pd
import numpy as np
import datetime
import logging
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
SILVER_DIR = BASE_DIR / "data" / "silver"
LOG_DIR = BASE_DIR / "log" 

logging.basicConfig(
    level= logging.INFO,
    filemode = 'w',
    filename = LOG_DIR / "test_silver.log",
    format = "%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S"
)

def test_run():

    layer = "SILVER"
    logging.info(f"{layer} | LAYER_START")  

    # ===================================================
    # ===================== ERP =========================
    # ===================================================

    logging.info("-" * 21)
    domain = "ERP"
    logging.info(f"{layer} | {domain} | DOMAIN_START")

    # ---------------------------------------------------
    # ------------------- suppliers ---------------------
    # --------------------------------------------------- 

    try:
        logging.info("-" * 21)

        table = "suppliers"
        column = "----"
        issues = []

        step = "LOAD"
        logging.info(f"{layer} | {domain} | {step}    | {table}")
        sup = pd.read_parquet(SILVER_DIR / "erp" / "suppliers.parquet")

        step = "QUALITY"
        # --------- supplier_id 
        column = "supplier_id"
        inv_id = sup['supplier_id'].str.match(r'^S\d{4}$').sum()
        dup = sup['supplier_id'].duplicated().sum()
        null = sup['supplier_id'][sup['supplier_id'].isnull()].shape[0]
        
        check = 0
        if inv_id != sup.shape[0]:
            issues.append(f"{column} | invalid_format")
            check = check + 1
        if dup != 0:
            issues.append(f"{column} | duplicate_values")
            check = check + 1
        if null != 0:
            issues.append(f"{column} | null_values")
            check = check + 1
        if check == 0:
            issues.append(f"{column} | pass")

        # --------- supplier_name 
        column = "supplier_name"
        inv_name = sup['supplier_name'].str.match(r'^Supplier_([1-9][0-9]{0,2}|1000)$').sum()

        if inv_id != sup.shape[0]:
            issues.append(f"{column} | invalid_format")
        else:
            issues.append(f"{column} | pass")

        # --------- city
        column = "city"
        valid_cities = {'bangalore', 'hyderabad', 'pune', 'mumbai', 'delhi', None}
        inv_city = sup['city'].isin(valid_cities).sum()

        if inv_city != sup.shape[0]:
            issues.append(f"{column} | invalid_format")
        else:
            issues.append(f"{column} | pass")

        # --------- phone
        column = "phone"
        df = sup[sup['phone'].notnull()]['phone']
        inv_phone = df.str.match(r'^\+91\d{10}$')
        inv_phone= inv_phone.sum()

        if inv_phone != df.shape[0]:
            issues.append(f"{column} | invalid_format")
        else:
            issues.append(f"{column} | pass")

        # --------- email
        column = "email"
        df = sup[sup['email'].notnull()]['email']
        inv_email = df.str.match(r'^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$')
        inv_email= inv_email.sum()

        if inv_email != df.shape[0]:
            issues.append(f"{column} | invalid_format")
        else:
            issues.append(f"{column} | pass")

        for issue in issues:
            if " pass" in issue.split("|"):
                logging.info(f"{layer} | {domain} | {step} | {table} | {issue}")
            else:
                logging.warning(f"{layer} | {domain} | {step} | {table} | {issue}")

    except Exception as e:
        logging.exception(
            f"{layer} | {domain} | {step} | {table} | {column} |"
            f"error_type={type(e).__name__} message={e}"
        )

    # ---------------------------------------------------
    # ------------------ ingredients --------------------
    # --------------------------------------------------- 

    try:
        logging.info("-" * 21)

        table = "ingredients"
        column = "----"
        issues = []

        step = "LOAD"
        logging.info(f"{layer} | {domain} | {step}    | {table}")
        ing = pd.read_parquet(SILVER_DIR / "erp" / "ingredients.parquet")

        step = "QUALITY"
        # --------- ingredients_id
        column = "ingredients_id"
        inv_id = ing['ingredient_id'].str.match(r'^ING\d{3}$').sum()
        dup = ing['ingredient_id'].duplicated().sum()
        null = ing['ingredient_id'][ing['ingredient_id'].isnull()].shape[0]

        check = 0
        if inv_id != ing.shape[0]:
            issues.append(f"{column} | invalid_format")
            check = check + 1
        if dup != 0:
            issues.append(f"{column} | duplicate_values")
            check = check + 1
        if null != 0:
            issues.append(f"{column} | null_values")
            check = check + 1
        if check == 0:
            issues.append(f"{column} | pass")

        # --------- ingredient_name 
        column = "ingredient_name"
        inv_name = ing['ingredient_name'].str.match(r'^Ingredient_([1-9][0-9]{0,2}|100)$').sum()

        if inv_id != ing.shape[0]:
            issues.append(f"{column} | invalid_format")
        else:
            issues.append(f"{column} | pass")

        # --------- unit
        column = "unit"
        valid_unit = {'liter', 'kg', 'pcs', None}
        unit = ing['unit'].isin(valid_unit).sum()

        if unit != ing.shape[0]:
            issues.append(f"{column} | invalid_format")
        else:
            issues.append(f"{column} | pass")

        for issue in issues:
            if " pass" in issue.split("|"):
                logging.info(f"{layer} | {domain} | {step} | {table} | {issue}")
            else:
                logging.warning(f"{layer} | {domain} | {step} | {table} | {issue}")

    except Exception as e:
        logging.exception(
            f"{layer} | {domain} | {step} | {table} | {column} | "
            f"error_type={type(e).__name__} message={e}"
        )

    # ---------------------------------------------------
    # -------------- supplier_ingredients ---------------
    # --------------------------------------------------- 

    try:
        logging.info("-" * 21)

        table = "supplier_ingredients"
        column = "----"
        issues = []

        step = "LOAD"
        logging.info(f"{layer} | {domain} | {step}    | {table}")
        sup_ing = pd.read_parquet(SILVER_DIR / "erp" / "supplier_ingredients.parquet")

        step = "QUALITY"

        # --------- supplier_id
        column = "supplier_id"
        inv_fmt = sup_ing['supplier_id'].str.match(r'^S\d{4}$').sum()
        null = sup_ing['supplier_id'][sup_ing['supplier_id'].isnull()].sum()
        sup = pd.read_parquet(SILVER_DIR / "erp" / "suppliers.parquet")
        inv_id = sup_ing['supplier_id'].isin(sup['supplier_id']).sum()

        check = 0
        if inv_fmt != sup_ing.shape[0]:
            issues.append(f"{column} | invalid_format")
            check = check + 1
        if null != 0:
            issues.append(f"{column} | null_values")
            check = check + 1
        if inv_id != sup_ing.shape[0]:
            issues.append(f"{column} | invalid")
            check = check + 1
        if check == 0:
            issues.append(f"{column} | pass")

        # --------- ingredient_id
        column = "ingredient_id"
        inv_fmt = sup_ing['ingredient_id'].str.match(r'^ING\d{3}$').sum()
        null = sup_ing['ingredient_id'][sup_ing['ingredient_id'].isnull()].sum()
        ing = pd.read_parquet(SILVER_DIR / "erp" /"ingredients.parquet")
        inv_id = sup_ing['ingredient_id'].isin(ing['ingredient_id']).sum()

        check = 0
        if inv_fmt != sup_ing.shape[0]:
            issues.append(f"{column} | invalid_format")
            check = check + 1
        if null != 0:
            issues.append(f"{column} | null_values")
            check = check + 1
        if inv_id != sup_ing.shape[0]:
            issues.append(f"{column} | invalid")
            check = check + 1
        if check == 0:
            issues.append(f"{column} | pass")

        for issue in issues:
            if " pass" in issue.split("|"):
                logging.info(f"{layer} | {domain} | {step} | {table} | {issue}")
            else:
                logging.warning(f"{layer} | {domain} | {step} | {table} | {issue}")

    except Exception as e:
        logging.exception(
            f"{layer} | {domain} | {step} | {table} | {column} | "
            f"error_type={type(e).__name__} message={e}"
        )

    # ---------------------------------------------------
    # ------------------- menu_items --------------------
    # --------------------------------------------------- 

    try:
        logging.info("-" * 21)

        table = "menu_items"
        issues = []

        step = "LOAD"
        logging.info(f"{layer} | {domain} | {step}    | {table}")
        menu = pd.read_parquet(SILVER_DIR / "erp" / "menu_items.parquet")

        step = "QUALITY"

        # --------- item_id
        column = "item_id"
        inv_fmt = menu["item_id"].str.match(r'^I\d{4}$').sum()
        dup = menu["item_id"].duplicated().sum()
        null = menu["item_id"][menu["item_id"].isnull()].sum()

        check = 0
        if inv_fmt != menu.shape[0]:
            issues.append(f"{column} | invalid_format")
            check = check + 1
        if dup != 0:
            issues.append(f"{column} | duplicate_values")
            check = check + 1
        if null != 0:
            issues.append(f"{column} | null_values")
            check = check + 1
        if check == 0:
            issues.append(f"{column} | pass")

        # --------- item_name 
        column = "item_name"
        inv_fmt = menu["item_name"].str.match(r'^Menu_Item_([1-9][0-9]{0,2}|500)$').sum()

        if inv_fmt != menu.shape[0]:
            issues.append(f"{column} | invalid_format")
        else:
            issues.append(f"{column} | pass")

        # --------- category
        column = "category"
        valid_category = {'Starter', 'Dessert', 'Beverage', 'Main Course', None}
        category = menu['category'].isin(valid_category).sum()
        if category != menu.shape[0]:
            issues.append(f"{column} | invalid")
        else:
            issues.append(f"{column} | pass")

        # --------- cuisine
        column = "cuisine"
        valid_cuisine = {'Italian', 'Indian', 'Chinese', 'Fast Food', 'Continental', None}
        cuisine = menu['cuisine'].isin(valid_cuisine).sum()
        if cuisine != menu.shape[0]:
            issues.append(f"{column} | invalid")
        else:
            issues.append(f"{column} | pass")

        # --------- selling_price
        column = "selling_price"
        df = menu[menu['selling_price'].notna()]
        price = df[(df['selling_price']> 0) & (df['selling_price']< 600)].shape[0]
        if price != df.shape[0]:
            issues.append(f"{column} | invalid")
        else:
            issues.append(f"{column} | pass")

        for issue in issues:
            if " pass" in issue.split("|"):
                logging.info(f"{layer} | {domain} | {step} | {table} | {issue}")
            else:
                logging.warning(f"{layer} | {domain} | {step} | {table} | {issue}")

    except Exception as e:
        logging.exception(
            f"{layer} | {domain} | {step} | {table} | {column} | "
            f"error_type={type(e).__name__} message={e}"
        )

    # ---------------------------------------------------
    # --------------------- recipe ----------------------
    # --------------------------------------------------- 

    try:
        logging.info("-" * 21)

        table = "recipe"
        issues = []

        step = "LOAD"
        logging.info(f"{layer} | {domain} | {step}    | {table}")
        recp = pd.read_parquet(SILVER_DIR / "erp" / "recipe.parquet")

        step = "QUALITY"

        # --------- item_id
        column = "items_id"
        inv_fmt = recp['item_id'].str.match(r'^I\d{4}$').sum()
        null = recp['item_id'][recp['item_id'].isnull()].sum()
        menu = pd.read_parquet(SILVER_DIR / "erp" /"menu_items.parquet")
        inv_id = recp['item_id'].isin(menu['item_id']).sum()

        check = 0
        if inv_fmt != recp.shape[0]:
            issues.append(f"{column} | invalid_format")
            check = check + 1
        if null != 0:
            issues.append(f"{column} | null_values")
            check = check + 1
        if inv_id != recp.shape[0]:
            issues.append(f"{column} | invalid")
            check = check + 1
        if check == 0:
            issues.append(f"{column} | pass")

        # --------- ingredient_id
        column = "ingredient_id"
        inv_fmt = recp['ingredient_id'].str.match(r'^ING\d{3}$').sum()
        null = recp['ingredient_id'][recp['ingredient_id'].isnull()].sum()
        ing = pd.read_parquet(SILVER_DIR / "erp" /"ingredients.parquet")
        inv_id = recp['ingredient_id'].isin(ing['ingredient_id']).sum()

        check = 0
        if inv_fmt != recp.shape[0]:
            issues.append(f"{column} | invalid_format")
            check = check + 1
        if null != 0:
            issues.append(f"{column} | null_values")
            check = check + 1
        if inv_id != recp.shape[0]:
            issues.append(f"{column} | invalid")
            check = check + 1
        if check == 0:
            issues.append(f"{column} | pass")

        # --------- quantity_required
        column = "selling_price"
        df = recp[recp['quantity_required'].notna()]
        quantity = df[(df['quantity_required'] > 0) & (df['quantity_required'] < 1)].shape[0]
        if quantity != df.shape[0]:
            issues.append(f"{column} | invalid")
        else:
            issues.append(f"{column} | pass")

        for issue in issues:
            if " pass" in issue.split("|"):
                logging.info(f"{layer} | {domain} | {step} | {table} | {issue}")
            else:
                logging.warning(f"{layer} | {domain} | {step} | {table} | {issue}")

    except Exception as e:
        logging.exception(
            f"{layer} | {domain} | {step} | {table} | {column} | "
            f"error_type={type(e).__name__} message={e}"
        )

    # ---------------------------------------------------
    # ------------------- restaurants -------------------
    # --------------------------------------------------- 

    try:
        logging.info("-" * 21)

        table = "restaurants"
        issues = []

        step = "LOAD"
        logging.info(f"{layer} | {domain} | {step}    | {table}")
        res = pd.read_parquet(SILVER_DIR / "erp" / "restaurants.parquet")

        step = "QUALITY"

        # --------- restaurants_id
        column = "restaurant_id"
        inv_fmt = res["restaurant_id"].str.match(r'^R\d{3}$').sum()
        dup = res["restaurant_id"].duplicated().sum()
        null = res["restaurant_id"][res["restaurant_id"].isnull()].sum()

        check = 0
        if inv_fmt != res.shape[0]:
            issues.append(f"{column} | invalid_format")
            check = check + 1
        if dup != 0:
            issues.append(f"{column} | duplicate_values")
            check = check + 1
        if null != 0:
            issues.append(f"{column} | null_values")
            check = check + 1
        if check == 0:
            issues.append(f"{column} | pass")

        # --------- restaurants_name 
        column = "restaurant_name"
        inv_fmt = res["restaurant_name"].str.match(r'^Restaurant_([1-9][0-9]{0,2}|500)$').sum()

        if inv_fmt != res.shape[0]:
            issues.append(f"{column} | invalid_format")
        else:
            issues.append(f"{column} | pass")

        # --------- city
        column = "city"
        valid_city = {'Delhi', 'Mumbai', 'Bangalore', 'Pune', 'Hyderabad', None}
        city = res['city'].isin(valid_city).sum()
        if city != res.shape[0]:
            issues.append(f"{column} | invalid")
        else:
            issues.append(f"{column} | pass")

        # --------- restaurant_type
        column = "restaurant_type"
        valid_res_type = {'Fine Dining', 'Cafe', 'Casual Dining', 'Fast Food','Cloud Kitchen', None}
        res_type = res['restaurant_type'].isin(valid_res_type).sum()
        if res_type != res.shape[0]:
            issues.append(f"{column} | invalid")
        else:
            issues.append(f"{column} | pass")

        # --------- open_date
        column = "open_date"
        df = res[res['open_date'].notna()]
        min_date = datetime.datetime(2018,1,1)
        max_date = datetime.datetime.now().strftime("%Y-%m-%d")
        open_date = df['open_date'][(df['open_date'] > min_date) & (df['open_date'] < max_date)].count()
        if open_date != df.shape[0]:
            issues.append(f"{column} | invalid_date")
        else:
            issues.append(f"{column} | pass")

        for issue in issues:
            if " pass" in issue.split("|"):
                logging.info(f"{layer} | {domain} | {step} | {table} | {issue}")
            else:
                logging.warning(f"{layer} | {domain} | {step} | {table} | {issue}")

    except Exception as e:
        logging.exception(
            f"{layer} | {domain} | {step} | {table} | {column} | "
            f"error_type={type(e).__name__} message={e}"
        )



if __name__ == "__main__":
    test_run()