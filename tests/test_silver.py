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

    table = "suppliers"
    issues = []

    try:

        logging.info("-" * 21)

        table = "suppliers"
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

        # --------- city
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
            f"{layer} | {domain} | {step} | {table} | "
            f"error_type={type(e).__name__} message={e}"
        )

    # ---------------------------------------------------
    # ------------------ ingredients --------------------
    # --------------------------------------------------- 

    table = "suppliers"
    issues = []

    try:

        logging.info("-" * 21)

        table = "ingredients"
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
            f"{layer} | {domain} | {step} | {table} | "
            f"error_type={type(e).__name__} message={e}"
        )

if __name__ == "__main__":
    test_run()