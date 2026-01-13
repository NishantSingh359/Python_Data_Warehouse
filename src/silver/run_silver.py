
import datetime
import logging
import pandas as pd
import numpy as np
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[2]
RAW_DIR = BASE_DIR / "data" / "raw"

SILVER_DIR = BASE_DIR / "data" / "silver"
SILVER_DIR.mkdir(exist_ok=True)

LOG_DIR = BASE_DIR / "log"
LOG_DIR.mkdir(exist_ok=True)


logging.basicConfig(
    level= logging.INFO,
    filemode = 'w',
    filename = LOG_DIR / "silver.log",
    format = "%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S"
)

def run():

    layer = "SILVER"
    silver_time1 = datetime.datetime.now()
    logging.info(f"{layer} | LAYER_START")  

    # ===================================================
    # ===================== ERP =========================
    # ===================================================

    logging.info("-" * 21)
    domain = "ERP"
    erp_time1 = datetime.datetime.now()
    logging.info(f"{layer} | {domain} | DOMAIN_START")  

    # ---------------------------------------------------
    # ------------------- suppliers ---------------------
    # --------------------------------------------------- 

    try:
                    
        logging.info("-" * 21)

        time1 = datetime.datetime.now()
        table = "suppliers"

        step = "LOAD"
        logging.info(f"{layer} | {domain} | {step}  | {table}")
        sup = pd.read_csv(RAW_DIR / "erp" / "suppliers.csv.gz")

        step = "CLEAN"
        logging.info(f"{layer} | {domain} | {step} | {table} | started")
        supplier_id =   sup['supplier_id'].str.replace(r'\D', '', regex=True).replace({'':np.nan})
        supplier_id =   pd.to_numeric(supplier_id, errors= 'coerce').replace(0,np.nan).astype('Int16')
        supplier_id =   supplier_id.fillna(sup['supplier_name'].str.replace(r'\D', '', regex=True).replace({'':np.nan}))
        supplier_id =   ('S' + supplier_id.astype(str).str.zfill(4)).where(supplier_id.notnull(), np.nan)

        supplier_name = supplier_id.replace(r'\D','', regex=True).astype('Int16')
        supplier_name = ('Supplier_' + supplier_name.astype(str)).where(supplier_name.notnull(), np.nan)

        city =          sup['city'].astype(str).str.strip().str.lower().replace({'nan':np.nan})

        phone =         sup['phone'].str.replace('ext.99','').replace(r'\D','', regex=True).str.extract(r'(\d{10}$)')[0].astype('Int64').astype(str)
        phone =         ('+91' + phone).where(phone.str.len()==10, np.nan)

        email =         supplier_name.str.lower().str.replace('_','')+'@mail.com'

        suppliers = pd.DataFrame({
            'supplier_id':   supplier_id,
            'supplier_name': supplier_name,
            'city':          city,
            'phone':         phone,
            'email':         email
        })
        suppliers = suppliers.dropna(subset= 'supplier_id').drop_duplicates(subset= 'supplier_id').sort_values(by = 'supplier_id').reset_index().drop('index', axis = 1)

        before = sup.shape[0]
        after = suppliers.shape[0]
        drop = before-after
        drop_pct = round((before - after) / before * 100, 2)
        logging.info(f"{layer} | {domain} | {step} | {table} | rows_before={before} rows_after={after} dropped={drop} pct={drop_pct}")

        step = "SAVE"
        logging.info(f"{layer} | {domain} | {step}  | {table} | target=parquet")
        suppliers.to_parquet(SILVER_DIR / "erp" / "suppliers.parquet")

        step = "TIME"
        time2 = datetime.datetime.now()
        time = time2 - time1
        time = round(time.total_seconds(), 4)     
        logging.info(f"{layer} | {domain} | {step}  | {table} | duration_sec={time}")        

        step = "DQ"
        if drop_pct > 5:
            logging.warning(f"{layer} | {domain} | {step} | {table} | dropped_pct={drop_pct} threshold=5")

        logging.info("-" * 21)

    except Exception as e:
        logging.exception(
            f"{layer} | {domain} | {step} | {table} | "
            f"error_type={type(e).__name__} message={e}"
        )

    
    # ---------------------------------------------------
    # ------------------- ingredients -------------------
    # --------------------------------------------------- 

    try:
        time1 = datetime.datetime.now()
        table = "ingredients"

        step = "LOAD"
        logging.info(f"{layer} | {domain} | {step}  | {table}")
        ing = pd.read_csv(RAW_DIR / "erp" / "ingredients.csv.gz")

        step = "CLEAN"
        logging.info(f"{layer} | {domain} | {step} | {table} | started")
        ingredient_id =   ing['ingredient_id'].str.replace(r'\D', '', regex=True).replace({'':np.nan})
        ingredient_id =   pd.to_numeric(ingredient_id, errors='coerce').replace(0,np.nan).astype('Int16')
        ingredient_id =   ingredient_id.fillna(ing['ingredient_name'].str.replace(r'\D', '', regex=True).replace({'':np.nan}))
        ingredient_id =   ('ING' + ingredient_id.astype(str).str.zfill(3)).where(ingredient_id.notnull(), np.nan)

        ingredient_name = ingredient_id.str.replace(r'\D', '', regex=True).astype('Int16')
        ingredient_name = ('Ingredient_' + ingredient_name.astype(str)).where(ingredient_id.notnull(), np.nan)

        unit =            ing['unit'].astype(str).str.strip().str.lower().replace({'nan': np.nan})

        ingredients = pd.DataFrame({
            'ingredient_id':ingredient_id,
            'ingredient_name':ingredient_name,
            'unit':unit
        })
        ingredients = ingredients.dropna(subset= 'ingredient_id').drop_duplicates(subset= 'ingredient_id').sort_values(by = 'ingredient_id').reset_index().drop('index', axis = 1) 
        
        before = ing.shape[0]
        after = ingredients.shape[0]
        drop = before-after
        drop_pct = round((before - after) / before * 100, 2)
        logging.info(f"{layer} | {domain} | {step} | {table} | rows_before={before} rows_after={after} dropped={drop} pct={drop_pct}")

        step = "SAVE"
        logging.info(f"{layer} | {domain} | {step}  | {table} | target=parquet")
        ingredients.to_parquet(SILVER_DIR / "erp" / "ingredients.parquet")

        step = "TIME"
        time2 = datetime.datetime.now()
        time = time2 - time1
        time = round(time.total_seconds(), 4)
        logging.info(f"{layer} | {domain} | {step}  | {table} | duration_sec={time}")

        step = "DQ"
        if drop_pct > 5:
            logging.warning(f"{layer} | {domain} | {step} | {table} | dropped_pct={drop_pct} threshold=5")

        logging.info("-" * 21)

    except Exception as e:
        logging.exception(
            f"{layer} | {domain} | {step} | {table} | "
            f"error_type={type(e).__name__} message={e}"
        )


    # ---------------------------------------------------
    # -------------- supplier_ingredients ---------------
    # --------------------------------------------------- 

    try:
        time1 = datetime.datetime.now()
        table = "supplier_ingredients"

        step = "LOAD"
        logging.info(f"{layer} | {domain} | {step}  | {table}")
        sup_ing = pd.read_csv(RAW_DIR / "erp" / "supplier_ingredients.csv.gz")

        step = "CLEAN"
        logging.info(f"{layer} | {domain} | {step} | {table} | started")
        suppliers =     pd.read_parquet( SILVER_DIR / "erp" / "suppliers.parquet")
        supplier_id =   sup_ing['supplier_id'].str.replace(r'\D','',regex=True).replace({'':np.nan})
        supplier_id =   pd.to_numeric(supplier_id, errors='coerce').replace(0,np.nan).astype('Int16')
        supplier_id =   ('S' + supplier_id.astype(str).str.zfill(4)).where(supplier_id.notna(), np.nan)
        supplier_id =   supplier_id.where(supplier_id.isin(suppliers['supplier_id']), np.nan)

        ingredients =   pd.read_parquet(SILVER_DIR / "erp" / "ingredients.parquet")
        ingredient_id = sup_ing['ingredient_id'].str.replace(r'\D','',regex=True).replace({'':np.nan})
        ingredient_id = pd.to_numeric(ingredient_id,errors='coerce').replace(0,np.nan).astype('Int16')
        ingredient_id = ('ING' + ingredient_id.astype(str).str.zfill(3)).where(ingredient_id.notna(), np.nan)
        ingredient_id = ingredient_id.where(ingredient_id.isin(ingredients['ingredient_id']), np.nan)

        cost_price =    pd.to_numeric(sup_ing['cost_price'], errors='coerce')
        cost_price =    cost_price.where((cost_price > 0) & (cost_price < 5000), np.nan)

        supplier_ingredients = pd.DataFrame({
            'supplier_id':     supplier_id,
            'ingredient_id': ingredient_id,
            'cost_price':      cost_price
        })

        supplier_ingredients = supplier_ingredients.dropna(subset= ['supplier_id', 'ingredient_id']).sort_values(by = 'supplier_id').reset_index().drop('index', axis = 1)

        before = sup_ing.shape[0]
        after = supplier_ingredients.shape[0]
        drop = before-after
        drop_pct = round((before - after) / before * 100, 2)
        logging.info(f"{layer} | {domain} | {step} | {table} | rows_before={before} rows_after={after} dropped={drop} pct={drop_pct}")

        step = "SAVE"
        logging.info(f"{layer} | {domain} | {step}  | {table} | target=parquet")
        supplier_ingredients.to_parquet(SILVER_DIR / "erp" / "supplier_ingredients.parquet")

        step = "TIME"
        time2 = datetime.datetime.now()
        time = time2 - time1
        time = round(time.total_seconds(), 4)
        logging.info(f"{layer} | {domain} | {step}  | {table} | duration_sec={time}")

        step = "DQ"
        if drop_pct > 5:
            logging.warning(f"{layer} | {domain} | {step} | {table} | dropped_pct={drop_pct} threshold=5")
            
        logging.info("-" * 21)
            
    except Exception as e:
        logging.exception(
            f"{layer} | {domain} | {step} | {table} | "
            f"error_type={type(e).__name__} message={e}"
        )


    # ---------------------------------------------------
    # -------------------- menu_items -------------------
    # --------------------------------------------------- 

    try:
        time1 = datetime.datetime.now()
        table = "menu_items"

        step = "LOAD"
        logging.info(f"{layer} | {domain} | {step}  | {table}")
        menu = pd.read_csv(RAW_DIR / "erp" / "menu_items.csv.gz")

        step = "CLEAN"
        logging.info(f"{layer} | {domain} | {step} | {table} | started")

        item_id =    menu['item_id'].str.replace(r'\D', '', regex=True).replace({'':np.nan})
        item_id =    pd.to_numeric(item_id, errors='coerce').replace(0, np.nan).astype('Int16')
        item_id =    item_id.fillna(menu['item_name'].replace(r'\D', '', regex=True).replace({'':np.nan}))
        item_id =    ('I' + item_id.astype(str).str.zfill(4)).where(item_id.notnull(), np.nan)

        item_name = item_id.str.replace(r'\D','', regex=True).astype('Int16')
        item_name = ('Menu_Item_' + item_name.astype(str)).where(item_name.notnull(), np.nan)

        category =   menu['category'].str.strip().replace({'nan':np.nan}).str.lower()
        cuisine =    menu['cuisine'].str.strip().replace({'nan':np.nan}).str.lower()

        selling_price = menu['selling_price'].astype(float)
        selling_price = selling_price.where(selling_price > 0, np.nan)

        menu_items = pd.DataFrame({
            'item_id':       item_id,
            'item_name':     item_name,
            'category':      category,
            'cuisine':       cuisine,
            'selling_price': selling_price
        })

        menu_items = menu_items.dropna(subset= 'item_id').drop_duplicates(subset = 'item_id').sort_values( by = 'item_id').reset_index().drop('index', axis = 1)

        before = menu.shape[0]
        after = menu_items.shape[0]
        drop = before-after
        drop_pct = round((before - after) / before * 100, 2)
        logging.info(f"{layer} | {domain} | {step} | {table} | rows_before={before} rows_after={after} dropped={drop} pct={drop_pct}")

        step = "SAVE"
        logging.info(f"{layer} | {domain} | {step}  | {table} | target=parquet")
        menu_items.to_parquet(SILVER_DIR / "erp" / "menu_items.parquet")

        step = "TIME"
        time2 = datetime.datetime.now()
        time =  time2 - time1
        time = round(time.total_seconds(), 4)
        logging.info(f"{layer} | {domain} | {step}  | {table} | duration_sec={time}")

        step = "DQ"
        if drop_pct > 5:
            logging.warning(f"{layer} | {domain} | {step} | {table} | dropped_pct={drop_pct} threshold=5")
            
        logging.info("-" * 21)
            
    except Exception as e:
        logging.exception(
            f"{layer} | {domain} | {step} | {table} | "
            f"error_type={type(e).__name__} message={e}"
        )


    # ---------------------------------------------------
    # ---------------------- recipe ---------------------
    # --------------------------------------------------- 

    try:
        time1 = datetime.datetime.now()
        table = "recipe"

        step = "LOAD"
        logging.info(f"{layer} | {domain} | {step}  | {table}")
        recp = pd.read_csv(RAW_DIR / "erp" / "recipe.csv.gz")

        step = "CLEAN"
        logging.info(f"{layer} | {domain} | {step} | {table} | started")

        menu_items =        pd.read_parquet( SILVER_DIR / "erp" / "menu_items.parquet")
        item_id =           recp['item_id'].str.replace(r'\D','', regex=True).replace({'':np.nan})
        item_id =           pd.to_numeric(item_id, errors = 'coerce').replace(0,np.nan).astype('Int16')
        item_id =           ('I' + item_id.astype(str).str.zfill(4)).where(item_id.notnull(), np.nan)
        item_id =           item_id.where(item_id.isin(menu_items['item_id']), np.nan)

        ingredients =       pd.read_parquet( SILVER_DIR / "erp" / "ingredients.parquet")
        ingredient_id =     recp['ingredient_id'].str.replace(r'\D','', regex=True).replace({'':np.nan})
        ingredient_id =     pd.to_numeric(ingredient_id, errors='coerce').replace(0,np.nan).astype('Int16')
        ingredient_id =     ('ING' + ingredient_id.astype(str).str.zfill(3)).where(ingredient_id.notnull(), np.nan)
        ingredient_id =     ingredient_id.where(ingredient_id.isin(ingredients['ingredient_id']), np.nan)

        quantity_required = recp['quantity_required'].astype(str).str.strip().astype(float)
        quantity_required = quantity_required.where(quantity_required > 0, np.nan)

        recipe = pd.DataFrame({
            'item_id':item_id,
            'ingredient_id':ingredient_id,
            'quantity_required':quantity_required
        })

        recipe = recipe.dropna(subset = ['item_id', 'ingredient_id']).sort_values(by = 'item_id').reset_index().drop('index', axis = 1)

        before = recp.shape[0]
        after = recipe.shape[0]
        drop = before-after
        drop_pct = round((before - after) / before * 100, 2)
        logging.info(f"{layer} | {domain} | {step} | {table} | rows_before={before} rows_after={after} dropped={drop} pct={drop_pct}")

        step = "SAVE"
        logging.info(f"{layer} | {domain} | {step}  | {table} | target=parquet")
        recipe.to_parquet(SILVER_DIR / "erp" / "recipe.parquet")

        step = "TIME"
        time2 = datetime.datetime.now()
        time = time2 - time1
        time = round(time.total_seconds(), 4)
        logging.info(f"{layer} | {domain} | {step}  | {table} | duration_sec={time}")

        step = "DQ"
        if drop_pct > 5:
            logging.warning(f"{layer} | {domain} | {step} | {table} | dropped_pct={drop_pct} threshold=5")
            
        logging.info("-" * 21)

    except Exception as e:
        logging.exception(
            f"{layer} | {domain} | {step} | {table} | "
            f"error_type={type(e).__name__} message={e}"
        )


    # ---------------------------------------------------
    # ------------------- restaurents -------------------
    # --------------------------------------------------- 

    try:
        time1 = datetime.datetime.now()
        table = "restaurants"

        step = "LOAD"
        logging.info(f"{layer} | {domain} | {step}  | {table}")
        res = pd.read_csv(RAW_DIR / "erp" / "restaurants.csv.gz")

        step = "CLEAN"
        logging.info(f"{layer} | {domain} | {step} | {table} | started")

        restaurant_id =   res['restaurant_id'].str.replace(r'\D','',regex=True).replace({'':np.nan})
        restaurant_id =   pd.to_numeric(restaurant_id, errors='coerce').replace(0,np.nan).astype('Int16')
        restaurant_id =   restaurant_id.fillna(res['restaurant_name'].str.replace(r'\D','',regex=True).replace({'':np.nan}))
        restaurant_id =   ('R'+restaurant_id.astype(str).str.zfill(3)).where(restaurant_id.notnull(), np.nan)

        restaurant_name = restaurant_id.replace(r'\D','',regex=True).astype('Int16')
        restaurant_name = ('Restaurant_'+restaurant_name.astype(str)).where(restaurant_id.notnull(),np.nan)

        city =            res['city'].str.strip().str.lower()
        restaurant_type = res['restaurant_type'].str.strip().str.lower()
        open_date =       pd.to_datetime(res['open_date'], format= '%Y-%m-%d %H:%M:%S', errors= 'coerce')

        restaurants = pd.DataFrame({
            'restaurant_id':restaurant_id,
            'restaurant_name':restaurant_name,
            'city':city,
            'restaurant_type':restaurant_type,
            'open_date':open_date
        })

        restaurants = restaurants.drop_duplicates(subset= 'restaurant_id').sort_values(by = 'restaurant_id').reset_index().drop('index', axis = 1)

        before = res.shape[0]
        after = restaurants.shape[0]
        drop = before-after
        drop_pct = round((before - after) / before * 100, 2)
        logging.info(f"{layer} | {domain} | {step} | {table} | rows_before={before} rows_after={after} dropped={drop} pct={drop_pct}")

        step = "SAVE"
        logging.info(f"{layer} | {domain} | {step}  | {table} | target=parquet")
        restaurants.to_parquet(SILVER_DIR / "erp" / "restaurants.parquet")

        step = "TIME"
        time2 = datetime.datetime.now()
        time = time2 - time1
        time = round(time.total_seconds(), 4)
        logging.info(f"{layer} | {domain} | {step}  | {table} | duration_sec={time}")

        step = "DQ"        
        if drop_pct > 5:
            logging.warning(f"{layer} | {domain} | {step} | {table} | dropped_pct={drop_pct} threshold=5")
            
        logging.info("-" * 21)

    except Exception as e:
        logging.exception(
            f"{layer} | {domain} | {step} | {table} | "
            f"error_type={type(e).__name__} message={e}"
        )


    # ---------------------------------------------------
    # -------------------- inventory --------------------
    # --------------------------------------------------- 

    try:
        time1 = datetime.datetime.now()
        table = "inventory"

        step = "LOAD"
        logging.info(f"{layer} | {domain} | {step}  | {table}")
        inv = pd.read_csv(RAW_DIR / "erp" / "inventory.csv.gz")

        step = "CLEAN"
        logging.info(f"{layer} | {domain} | {step} | {table} | started")

        restaurants =     pd.read_parquet( SILVER_DIR / "erp" / "restaurants.parquet")
        restaurant_id =   inv['restaurant_id'].str.replace(r'\D', '', regex=True).replace({'':np.nan})
        restaurant_id =   pd.to_numeric(restaurant_id, errors='coerce').replace(0, np.nan).astype('Int16')
        restaurant_id =   ('R'+restaurant_id.astype(str).str.zfill(3)).where(restaurant_id.notnull(), np.nan)
        restaurant_id =   restaurant_id.where(restaurant_id.isin(restaurants['restaurant_id']), np.nan)

        ingredients =     pd.read_parquet( SILVER_DIR / "erp" / "ingredients.parquet")
        ingredient_id =   inv['ingredient_id'].replace(r'\D', '', regex=True).replace({'':np.nan})
        ingredient_id =   pd.to_numeric(ingredient_id, errors='coerce').replace(0, np.nan).astype('Int16')
        ingredient_id =   ('ING'+ingredient_id.astype(str).str.zfill(3)).where(ingredient_id.notna(), np.nan)
        ingredient_id =   ingredient_id.where(ingredient_id.isin(ingredients['ingredient_id']), np.nan)

        stock_qty =       inv['stock_qty'].astype(str).str.strip().astype(float)
        stock_qty =       stock_qty.where((stock_qty > 0) & (stock_qty < 1000), np.nan)

        reorder_level =   inv['reorder_level'].astype(str).str.strip().astype(float)
        reorder_level =   reorder_level.where(reorder_level > 0, np.nan)

        last_updated_at = inv['last_updated_at'].str.strip()
        last_updated_at = pd.to_datetime(last_updated_at, format= '%Y-%m-%d %H:%M:%S', errors= 'coerce')

        inventory = pd.DataFrame({
            'restaurant_id':restaurant_id,
            'ingredient_id':ingredient_id,
            'stock_qty':stock_qty,
            'reorder_level':reorder_level,
            'last_updated_at':last_updated_at
        })

        inventory = inventory.dropna(subset= 'restaurant_id').sort_values(by = 'restaurant_id').reset_index().drop('index', axis = 1)

        before = inv.shape[0]
        after = inventory.shape[0]
        drop = before-after
        drop_pct = round((before - after) / before * 100, 2)
        logging.info(f"{layer} | {domain} | {step} | {table} | rows_before={before} rows_after={after} dropped={drop} pct={drop_pct}")

        step = "SAVE"
        logging.info(f"{layer} | {domain} | {step}  | {table} | target=parquet")
        inventory.to_parquet(SILVER_DIR / "erp" / "inventory.parquet")

        step = "TIME"
        time2 = datetime.datetime.now()
        time = time2 - time1
        time = round(time.total_seconds(), 4)
        logging.info(f"{layer} | {domain} | {step}  | {table} | duration_sec={time}")

        step = "DQ"
        if drop_pct > 5:
            logging.warning(f"{layer} | {domain} | {step} | {table} | dropped_pct={drop_pct} threshold=5")
            
        logging.info("-" * 21)

    except Exception as e:
        logging.exception(
            f"{layer} | {domain} | {step} | {table} | "
            f"error_type={type(e).__name__} message={e}"
        )

    
    # ---------------------------------------------------
    # ---------------- delivery_partners ----------------
    # --------------------------------------------------- 

    try:
        time1 = datetime.datetime.now()
        table = "delivery_partners"

        step = "LOAD"
        logging.info(f"{layer} | {domain} | {step}  | {table}")
        del_part = pd.read_csv(RAW_DIR / "erp" / "delivery_partners.csv.gz")

        step = "CLEAN"
        logging.info(f"{layer} | {domain} | {step} | {table} | started")

        partner_id =   del_part['delivery_partner_id'].str.replace(r'\D', '', regex=True).replace({'':np.nan})
        partner_id =   pd.to_numeric(partner_id, errors='coerce').replace(0, np.nan).astype('Int16')
        partner_id =   partner_id.fillna(del_part['name'].str.replace(r'\D', '', regex=True).replace({'':np.nan}))
        partner_id =   ('D'+partner_id.astype(str).str.zfill(4).where(partner_id.notnull(), np.nan))

        
        name =         partner_id.str.replace(r'\D', '',regex=True).astype('Int16')
        name =         ('Rider_' + name.astype(str)).where(name.notnull(), np.nan)

        partner_type = del_part['partner_type'].str.strip().str.lower().replace({'nan':np.nan})
        vehicle_type = del_part['vehicle_type'].str.strip().str.lower().replace({'nan':np.nan})

        phone =         del_part['phone'].str.replace('ext.99','').replace(r'\D','', regex= True).str.extract(r'(\d{10}$)')[0].astype('Int64').astype(str)
        phone =         ('+91' + phone).where(phone.str.len() == 10, np.nan)

        delivery_partners = pd.DataFrame({
            'delivery_partner_id':partner_id,
            'name':name,
            'partner_type':partner_type,
            'vehicle_type':vehicle_type,
            'phone':phone
        })

        delivery_partners = delivery_partners.drop_duplicates(subset= 'delivery_partner_id').sort_values(by = 'delivery_partner_id').reset_index().drop('index', axis = 1)

        before = del_part.shape[0]
        after = delivery_partners.shape[0]
        drop = before-after
        drop_pct = round((before - after) / before * 100, 2)
        logging.info(f"{layer} | {domain} | {step} | {table} | rows_before={before} rows_after={after} dropped={drop} pct={drop_pct}")

        step = "SAVE"
        logging.info(f"{layer} | {domain} | {step}  | {table} | target=parquet")
        delivery_partners.to_parquet(SILVER_DIR / "erp" / "delivery_partners.parquet")

        step = "TIME"
        time2 = datetime.datetime.now()
        time = time2 - time1
        time = round(time.total_seconds(), 4)
        logging.info(f"{layer} | {domain} | {step}  | {table} | duration_sec={time}")

        step = "DQ"
        if drop_pct > 5:
            logging.warning(f"{layer} | {domain} | {step} | {table} | dropped_pct={drop_pct} threshold=5")
            
        logging.info("-" * 21)
            
    except Exception as e:
        logging.exception(
            f"{layer} | {domain} | {step} | {table} | "
            f"error_type={type(e).__name__} message={e}"
        )


    # ---------------------------------------------------
    # -------------------- employees --------------------
    # --------------------------------------------------- 

    try:
        time1 = datetime.datetime.now()
        table = "employees"

        step = "LOAD"
        logging.info(f"{layer} | {domain} | {step}  | {table}")
        emp = pd.read_csv(RAW_DIR / "erp" / "employees.csv.gz")

        step = "CLEAN"
        logging.info(f"{layer} | {domain} | {step} | {table} | started")

        emp_id =        emp['emp_id'].str.replace(r'\D', '', regex=True).astype('Int16')
        emp_id =        ('E' + emp_id.astype(str).str.zfill(5)).where(emp_id.notnull() == True, np.nan) 

        restaurants =   pd.read_parquet( SILVER_DIR / "erp" / "restaurants.parquet")
        restaurant_id = pd.to_numeric(emp['restaurant_id'].str.replace(r'\D', '', regex=True), errors= 'coerce')
        restaurant_id = ('R' + restaurant_id.astype('Int16').astype(str).str.zfill(3)).where(restaurant_id.notnull() == True, np.nan)
        restaurant_id = restaurant_id.where(restaurant_id.isin(restaurants['restaurant_id']), np.nan)

        role =          emp['role'].str.strip().replace({'nan':np.nan})

        hire_date =     emp['hire_date'].str.strip()
        hire_date =     pd.to_datetime(hire_date, format= '%Y-%m-%d %H:%M:%S', errors= 'coerce')

        salary =        pd.to_numeric(emp['salary'], errors= 'coerce').astype('Int64')
        salary =        salary.where(salary > 0, np.nan)

        employees = pd.DataFrame({
            'emp_id':        emp_id,
            'restaurant_id': restaurant_id,
            'role':          role,
            'hire_date':     hire_date,
            'salary':        salary
        })

        employees = employees.dropna(subset= 'emp_id').drop_duplicates(subset= 'emp_id').sort_values(by = 'emp_id').reset_index().drop('index', axis = 1)

        before = emp.shape[0]
        after = employees.shape[0]
        drop = before-after
        drop_pct = round((before - after) / before * 100, 2)
        logging.info(f"{layer} | {domain} | {step} | {table} | rows_before={before} rows_after={after} dropped={drop} pct={drop_pct}")

        step = "SAVE" 
        logging.info(f"{layer} | {domain} | {step}  | {table} | target=parquet")
        employees.to_parquet(SILVER_DIR / "erp" / "employees.parquet")

        step = "TIME"
        time2 = datetime.datetime.now()
        time =  time2 - time1
        time = round(time.total_seconds(), 4)
        logging.info(f"{layer} | {domain} | {step}  | {table} | duration_sec={time}")
        
        step = "DQ"
        if drop_pct > 5:
            logging.warning(f"{layer} | {domain} | {step} | {table} | dropped_pct={drop_pct} threshold=5")
            
        logging.info("-" * 21)

    except Exception as e:
        logging.exception(
            f"{layer} | {domain} | {step} | {table} | "
            f"error_type={type(e).__name__} message={e}"
        )

    erp_time2 = datetime.datetime.now()
    erp_time = erp_time2 - erp_time1
    erp_time = round(erp_time.total_seconds(), 4)
    logging.info(f"{layer} | {domain} | DOMAIN_END | duration_sec={erp_time}") 



    # ===================================================
    # ===================== CRM =========================
    # ===================================================

    logging.info("-" * 21)
    domain = "CRM"
    crm_time1 = datetime.datetime.now()
    logging.info(f"{layer} | {domain} | DOMAIN_START")

    # ---------------------------------------------------
    # -------------------- customers --------------------
    # --------------------------------------------------- 

    try:
                    
        logging.info("-" * 21)
        time1 = datetime.datetime.now()
        table = "customers"

        step = "LOAD"
        logging.info(f"{layer} | {domain} | {step}  | {table}")
        cust = pd.read_csv(RAW_DIR / "crm" / "customers.csv.gz")

        step = "CLEAN"
        logging.info(f"{layer} | {domain} | {step} | {table} | started")

        customer_id =   cust['customer_id'].str.replace(r'\D','', regex=True).replace({'':np.nan})
        customer_id =   customer_id.fillna(cust['customer_name'].str.replace(r'\D','',regex=True).replace({'':np.nan}))
        customer_id =   ('C' + customer_id.astype('Int32').astype(str).str.zfill(6)).where(customer_id.notnull(), np.nan)

        customer_name = customer_id.str.replace(r'\D','', regex=True).astype('Int32')
        customer_name = ('Customer_' + customer_name.astype(str)).where(customer_name.notnull(), np.nan)

        city =          cust['city'].str.strip().replace({'nan':np.nan}).str.lower()

        phone =         cust['phone'].str.replace('ext.99','').replace(r'\D','', regex=True).str.extract(r'(\d{10}$)')[0].astype('Int64').astype(str)
        phone =         ('+91' + phone).where(phone.str.len() == 10, np.nan)

        created_at =    cust['created_at'].str.strip()
        created_at =    pd.to_datetime(created_at, format= '%Y-%m-%d %H:%M:%S', errors='coerce')

        customers = pd.DataFrame({
            'customer_id':  customer_id,
            'customer_name':customer_name,
            'city':         city,
            'phone':        phone,
            'created_at':   created_at
        })

        customers = customers.dropna(subset= 'customer_id').drop_duplicates(subset= 'customer_id').sort_values(by = 'customer_id').reset_index().drop('index', axis=1)

        before = cust.shape[0]
        after = customers.shape[0]
        drop = before-after
        drop_pct = round((before - after) / before * 100, 2)
        logging.info(f"{layer} | {domain} | {step} | {table} | rows_before={before} rows_after={after} dropped={drop} pct={drop_pct}")

        step = "SAVE"
        logging.info(f"{layer} | {domain} | {step}  | {table} | target=parquet")
        customers.to_parquet(SILVER_DIR / "crm" / "customers.parquet")

        step = "TIME"
        time2 = datetime.datetime.now()
        time =  time2 - time1
        time = round(time.total_seconds(), 4)
        logging.info(f"{layer} | {domain} | {step}  | {table} | duration_sec={time}")

        step = "DQ"
        if drop_pct > 5:
            logging.warning(f"{layer} | {domain} | {step} | {table} | dropped_pct={drop_pct} threshold=5")
            
        logging.info("-" * 21)
            
    except Exception as e:
        logging.exception(
            f"{layer} | {domain} | {step} | {table} | "
            f"error_type={type(e).__name__} message={e}"
        )


    # ---------------------------------------------------
    # ---------------------- orders ---------------------
    # --------------------------------------------------- 

    try:
        time1 = datetime.datetime.now()
        table = "orders"

        step = "LOAD"
        logging.info(f"{layer} | {domain} | {step}  | {table}")
        orde = pd.read_csv(RAW_DIR / "crm" / "orders.csv.gz")

        step = "CLEAN"
        logging.info(f"{layer} | {domain} | {step} | {table} | started")

        order_id =       orde['order_id'].str.replace(r'\D', '', regex=True).replace({'':np.nan}).astype('Int32')
        order_id =       ('O' + order_id.astype(str).str.zfill(7)).where(order_id.notnull(), np.nan) 

        customers =      pd.read_parquet( SILVER_DIR / "crm" / "customers.parquet")
        customer_id =    orde['customer_id'].str.replace(r'\D', '', regex=True).replace({'':np.nan}).astype('Int32')
        customer_id =    ('C' + customer_id.astype(str).str.zfill(6)).where(customer_id.notnull(), np.nan)
        customer_id =    customer_id.where(customer_id.isin(customers['customer_id']), np.nan)

        restaurants =    pd.read_parquet( SILVER_DIR / "erp" / "restaurants.parquet")
        restaurant_id =  orde['restaurant_id'].str.replace(r'\D', '', regex=True).replace({'':np.nan}).astype('Int32')
        restaurant_id =  ('R' + restaurant_id.astype(str).str.zfill(3)).where(restaurant_id.notnull(), np.nan)
        restaurant_id =  restaurant_id.where(restaurant_id.isin(restaurants['restaurant_id']), np.nan)

        order_datetime = orde['order_datetime'].str.strip()
        order_datetime = pd.to_datetime(order_datetime, format= '%Y-%m-%d %H:%M:%S', errors='coerce')

        payment_mode =   orde['payment_mode'].str.strip().replace({'nan':np.nan}).str.lower()
        order_status =   orde['order_status'].str.strip().replace({'nan':np.nan}).str.lower()
        is_delivery =    orde['is_delivery'].str.strip().replace({'nan':np.nan}).str.title()

        partners =       pd.read_parquet( SILVER_DIR / "erp" / "delivery_partners.parquet")
        partner_id =     orde['delivery_partner_id'].str.replace(r'\D', '', regex=True).replace({'':np.nan}).astype('Int32')
        partner_id =     ('D' + partner_id.astype(str).str.zfill(4)).where(partner_id.notnull(), np.nan)
        partner_id =     partner_id.where(partner_id.isin(partners['delivery_partner_id']), np.nan)
        partner_id =     partner_id.mask((is_delivery == 'True') & (partner_id.isna()), 'UNKNOWN')

        orders = pd.DataFrame({
            'order_id':           order_id,
            'customer_id':        customer_id,
            'restaurant_id':      restaurant_id,
            'order_datetime':     order_datetime,
            'payment_mode':       payment_mode,
            'order_status':       order_status,
            'is_delivery':        is_delivery,
            'delivery_partner_id':partner_id
        })

        orders = orders.dropna(subset='order_id').drop_duplicates(subset='order_id').sort_values(by='order_id').reset_index().drop('index', axis=1)

        before = orde.shape[0]
        after = orders.shape[0]
        drop = before-after
        drop_pct = round((before - after) / before * 100, 2)
        logging.info(f"{layer} | {domain} | {step} | {table} | rows_before={before} rows_after={after} dropped={drop} pct={drop_pct}")

        step = "SAVE"
        logging.info(f"{layer} | {domain} | {step}  | {table} | target=parquet")
        orders.to_parquet(SILVER_DIR / "crm" / "orders.parquet")

        step = "TIME"
        time2 = datetime.datetime.now()
        time =  time2 - time1
        time = round(time.total_seconds(), 4)
        logging.info(f"{layer} | {domain} | {step}  | {table} | duration_sec={time}")

        step = "DQ"            
        if drop_pct > 5:
            logging.warning(f"{layer} | {domain} | {step} | {table} | dropped_pct={drop_pct} threshold=5")
            
        logging.info("-" * 21)

    except Exception as e:
        logging.exception(
            f"{layer} | {domain} | {step} | {table} | "
            f"error_type={type(e).__name__} message={e}"
        )

    # ---------------------------------------------------
    # ----------------- customer_reviews ----------------
    # --------------------------------------------------- 

    try:
        time1 = datetime.datetime.now()
        table = "customer_reviews"

        step = "LOAD"
        logging.info(f"{layer} | {domain} | {step}  | {table}")
        rev = pd.read_csv(RAW_DIR / "crm" / "customer_reviews.csv.gz")

        step = "CLEAN"
        logging.info(f"{layer} | {domain} | {step} | {table} | started")

        orders =      pd.read_parquet( SILVER_DIR / "crm" / "orders.parquet")
        order_id =    rev['order_id'].str.replace(r'\D','',regex=True).replace({'':np.nan})
        order_id =    order_id.fillna(rev['review_id'].str.replace(r'\D','',regex=True).replace({'':np.nan}))
        order_id =    ('O'+ order_id.astype('Int32').astype(str).str.zfill(7)).where(order_id.notnull(), np.nan)
        order_id =    order_id.where(order_id.isin(orders['order_id']), np.nan)

        review_id =   order_id.str.replace(r'\D','',regex=True).replace({'':np.nan})
        review_id =   ('RV_O'+review_id).where(review_id.notnull() == True, np.nan)

        rating =      pd.to_numeric(rev['rating'].str.strip().replace({'nan':np.nan}), errors= 'coerce').astype('Int16')
        review_text = rev['review_text'].str.strip().replace({'nan':np.nan,'':np.nan}).str.lower()

        created_at =  rev['created_at'].str.strip()
        created_at =  pd.to_datetime(created_at, format= '%Y-%m-%d %H:%M:%S.%f', errors='coerce')

        review = pd.DataFrame({
            'review_id':review_id,
            'order_id':order_id,
            'rating':rating,
            'review_text':review_text,
            'created_at':created_at
        })

        review = review.dropna(subset='review_id').drop_duplicates(subset='review_id').sort_values(by='review_id').reset_index().drop('index', axis=1)

        before = rev.shape[0]
        after = review.shape[0]
        drop = before-after
        drop_pct = round((before - after) / before * 100, 2)
        logging.info(f"{layer} | {domain} | {step} | {table} | rows_before={before} rows_after={after} dropped={drop} pct={drop_pct}")

        step = "SAVE"
        logging.info(f"{layer} | {domain} | {step}  | {table} | target=parquet")
        review.to_parquet(SILVER_DIR / "crm" / "customer_reviews.parquet")

        step = "TIME"
        time2 = datetime.datetime.now()
        time =  time2 - time1
        time = round(time.total_seconds(), 4)
        logging.info(f"{layer} | {domain} | {step}  | {table} | duration_sec={time}")

        step = "DQ"            
        if drop_pct > 5:
            logging.warning(f"{layer} | {domain} | {step} | {table} | dropped_pct={drop_pct} threshold=5")
            
        logging.info("-" * 21)

    except Exception as e:
        logging.exception(
            f"{layer} | {domain} | {step} | {table} | "
            f"error_type={type(e).__name__} message={e}"
        )


    # ---------------------------------------------------
    # ------------------- order_items -------------------
    # --------------------------------------------------- 

    try:
        time1 = datetime.datetime.now()
        table = "order_items"

        step = "LOAD"
        logging.info(f"{layer} | {domain} | {step}  | {table}")
        ord_itm = pd.read_csv(RAW_DIR / "crm" / "order_items.csv.gz")

        step = "CLEAN"
        logging.info(f"{layer} | {domain} | {step} | {table} | started")

        order_item_id = ord_itm['order_item_id'].str.replace(r'\D','', regex=True).replace({'':np.nan,'nan':np.nan})
        order_item_id = ('OI'+order_item_id.astype('Int32').astype(str).str.zfill(8)).where(order_item_id.notnull() == True, np.nan)

        orders =        pd.read_parquet( SILVER_DIR / "crm" / "orders.parquet")
        order_id =      ord_itm['order_id'].replace(r'\D','',regex=True).replace({'':np.nan,'nan':np.nan})
        order_id =      ('O'+order_id.astype('Int32').astype(str).str.zfill(7)).where(order_id.notnull() == True, np.nan)
        order_id =      order_id.where(order_id.isin(orders['order_id']), np.nan)

        menu =          pd.read_parquet( SILVER_DIR / "erp" / "menu_items.parquet")
        item_id =       ord_itm['item_id'].str.strip().replace(r'\D','',regex=True).replace({'':np.nan,'nan':np.nan})
        item_id =       ('I'+item_id.astype('Int32').astype(str).str.zfill(4)).where(item_id.notnull() == True, np.nan)
        item_id['item_id'] = item_id.where(item_id.isin(menu['item_id']), np.nan)

        # item_id mapping (dictionary)
        unit_price =  pd.to_numeric(ord_itm['unit_price'].str.strip().str.replace('-','').replace({'nan':np.nan}), errors= 'coerce')
        ord_itm['unit_price'] =  unit_price.where((unit_price > 0) & (unit_price < 1000), np.nan)

        price_counts =           menu.groupby('selling_price')['item_id'].nunique()
        unique_price_menu =      menu.loc[menu['selling_price'].isin(price_counts[price_counts == 1].index),['item_id', 'selling_price']]

        # Fill NaN item_id 
        merg =    ord_itm.merge(unique_price_menu, left_on='unit_price', right_on='selling_price', how='left', suffixes=('', '_menu'))
        item_id['item_id'] = merg['item_id'].fillna(merg['item_id_menu'])
        item_id = item_id['item_id']

        quantity =   pd.to_numeric(ord_itm['quantity'].str.strip().str.replace('-','').replace({'nan':np.nan}), errors= 'coerce').astype('Int32')
        quantity =   quantity.where((quantity > 0) & (quantity < 50), np.nan)
        merg =       ord_itm.merge(menu[['item_id', 'selling_price']], on='item_id', how='left')
        unit_price = merg['unit_price'].fillna(merg['selling_price'])
        line_total = pd.to_numeric(ord_itm['line_total'].str.strip().str.replace('-','').replace({'nan':np.nan}), errors= 'coerce')
        line_total = line_total.where((line_total > 0) & (line_total < 1000), np.nan)

        merg =       ord_itm.merge(menu[['item_id', 'selling_price']], on='item_id', how='left')
        unit_price = merg['unit_price'].fillna(merg['selling_price'])

        mask = unit_price.isna() & quantity.gt(0)
        unit_price.loc[mask] = (line_total.loc[mask] / quantity.loc[mask]).round(2).astype('float64')

        mask = quantity.isna() & unit_price.gt(0)
        tmp = (line_total.loc[mask] / unit_price.loc[mask]).round()
        quantity.loc[mask] = tmp.astype('Int32')

        line_total =  line_total.fillna(unit_price*quantity)

        order_items = pd.DataFrame({
            'order_item_id':order_item_id,
            'order_id':     order_id,
            'item_id':      item_id,
            'quantity':     quantity,
            'unit_price':   unit_price,
            'line_total':   line_total
        })

        mask = (order_items['unit_price'] * order_items['quantity']) != order_items['line_total']
        order_items.loc[mask,'line_total'] = (order_items.loc[mask,'quantity'] * order_items.loc[mask,'unit_price']).round(2).astype('float64')

        order_items = order_items.dropna(subset = 'order_id')
        order_items = order_items.dropna(subset='order_item_id').drop_duplicates(subset='order_item_id').sort_values(by='order_item_id').reset_index(drop=True)

        before = ord_itm.shape[0]
        after = order_items.shape[0]
        drop = before-after
        drop_pct = round((before - after) / before * 100, 2)
        logging.info(f"{layer} | {domain} | {step} | {table} | rows_before={before} rows_after={after} dropped={drop} pct={drop_pct}")

        step = "SAVE"
        logging.info(f"{layer} | {domain} | {step}  | {table} | target=parquet")
        order_items.to_parquet(SILVER_DIR / "crm" / "order_items.parquet")

        step = "TIME"
        time2 = datetime.datetime.now()
        time = time2 - time1
        time = round(time.total_seconds(), 4)
        logging.info(f"{layer} | {domain} | {step}  | {table} | duration_sec={time}")

        step = "DQ"          
        if drop_pct > 5:
            logging.warning(f"{layer} | {domain} | {step} | {table} | dropped_pct={drop_pct} threshold=5")
            
        logging.info("-" * 21)

    except Exception as e:
        logging.exception(
            f"{layer} | {domain} | {step} | {table}| "
            f"error_type={type(e).__name__} message={e}"
        )
    

    # ---------------------------------------------------
    # ------------------- kitchen_logs ------------------
    # --------------------------------------------------- 

    try:
        time1 = datetime.datetime.now()
        table = "kitchen_logs"

        step = "LOAD" 
        logging.info(f"{layer} | {domain} | {step}  | {table}")
        kic = pd.read_csv(RAW_DIR / "crm" / "kitchen_logs.csv.gz")

        step = "CLEAN"
        logging.info(f"{layer} | {domain} | {step} | {table} | started")

        order_item =     pd.read_parquet( SILVER_DIR / "crm" / "order_items.parquet")
        order_item_id =  kic['order_item_id'].str.replace(r'\D','',regex=True).replace({'':np.nan,'nan':np.nan})
        order_item_id =  pd.to_numeric(order_item_id, errors= 'coerce').astype('Int64')
        order_item_id =  ('OI'+order_item_id.astype(str).str.zfill(8)).where(order_item_id.notnull(),np.nan)
        order_item_id =  order_item_id.where(order_item_id.isin(order_item['order_item_id']), np.nan)

        emp =            pd.read_parquet( SILVER_DIR / "erp" / "employees.parquet")
        chef =           emp[emp['role'] == 'chef']['emp_id']
        chef_id =        kic['chef_id'].str.replace(r'\D','',regex=True).replace({'':np.nan,'nan':np.nan})
        chef_id =        pd.to_numeric(chef_id, errors= 'coerce').astype('Int64')
        chef_id =        ('E'+chef_id.astype(str).str.zfill(5)).where(chef_id.notnull(),np.nan)
        chef_id =        chef_id.where(chef_id.isin(chef), np.nan)

        started_at =     kic['started_at'].str.strip()
        started_at =     pd.to_datetime(started_at, format = '%Y-%m-%d %H:%M:%S', errors='coerce')

        completed_at =   kic['completed_at'].str.strip()
        completed_at =   pd.to_datetime(completed_at, format = '%Y-%m-%d %H:%M:%S', errors='coerce')

        status =         kic['status'].str.strip().replace({'nan':np.nan,'':np.nan}).str.lower()

        kitchen_logs = pd.DataFrame({
            'order_item_id':order_item_id,
            'chef_id':      chef_id,
            'started_at':   started_at,
            'completed_at': completed_at,
            'status':       status
        })

        kitchen_logs  = kitchen_logs.dropna(subset='order_item_id').drop_duplicates(subset='order_item_id').sort_values(by='order_item_id').reset_index(drop=True)
        kitchen_logs.insert(0,'kitchen_log_id','K'+(kitchen_logs.index+1).astype(str).str.zfill(8))

        before = kic.shape[0]
        after = kitchen_logs.shape[0]
        drop = before-after
        drop_pct = round((before - after) / before * 100, 2)
        logging.info(f"{layer} | {domain} | {step} | {table} | rows_before={before} rows_after={after} dropped={drop} pct={drop_pct}")

        step = "SAVE"
        logging.info(f"{layer} | {domain} | {step}  | {table} | target=parquet")
        kitchen_logs.to_parquet(SILVER_DIR / "crm" / "kitchen_logs.parquet")

        step = "TIME"
        time2 = datetime.datetime.now()
        time = time2 - time1
        time = round(time.total_seconds(), 4)
        logging.info(f"{layer} | {domain} | {step}  | {table} | duration_sec={time}")

        step = "DQ"
        if drop_pct > 5:
            logging.warning(f"{layer} | {domain} | {step} | {table} | dropped_pct={drop_pct} threshold=5")
            
        logging.info("-" * 21)

    except Exception as e:
        logging.exception(
            f"{layer} | {domain} | {step} | {table} | "
            f"error_type={type(e).__name__} message={e}"
        )

    crm_time2 = datetime.datetime.now()
    crm_time = crm_time2 - crm_time1
    crm_time = round(crm_time.total_seconds(), 4)
    logging.info(f"{layer} | {domain} | DOMAIN_END | duration_sec={crm_time}")
    logging.info("-" * 21)
    
    silver_time2 = datetime.datetime.now()
    silver_time = silver_time2 - silver_time1
    silver_time = round(silver_time.total_seconds(), 4)
    logging.info(f"{layer} | LAYER_END | duration_sec={silver_time}")

if __name__ == "__main__":
    run()