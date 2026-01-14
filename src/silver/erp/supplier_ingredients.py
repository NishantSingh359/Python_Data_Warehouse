import yaml
import numpy as np
import pandas as pd
from pathlib import Path
from base.base_silver_pipeline import BaseSilverPipeline

with open("src/silver/config/erp.yaml") as f:
    cfg = yaml.safe_load(f)
    path = cfg['tables']['supplier_ingredients']

class Supplier_IngredientsSilver(BaseSilverPipeline):

    def clean(self, sup_ing: pd.DataFrame) -> pd.DataFrame:

        suppliers =     pd.read_parquet(path['suppliers_path'])
        supplier_id =   sup_ing['supplier_id'].str.replace(r'\D','',regex=True).replace({'':np.nan})
        supplier_id =   pd.to_numeric(supplier_id, errors='coerce').replace(0,np.nan).astype('Int16')
        supplier_id =   ('S' + supplier_id.astype(str).str.zfill(4)).where(supplier_id.notna(), np.nan)
        supplier_id =   supplier_id.where(supplier_id.isin(suppliers['supplier_id']), np.nan)

        ingredients =   pd.read_parquet(path['ingredients_path'])
        ingredient_id = sup_ing['ingredient_id'].str.replace(r'\D','',regex=True).replace({'':np.nan})
        ingredient_id = pd.to_numeric(ingredient_id,errors='coerce').replace(0,np.nan).astype('Int16')
        ingredient_id = ('ING' + ingredient_id.astype(str).str.zfill(3)).where(ingredient_id.notna(), np.nan)
        ingredient_id = ingredient_id.where(ingredient_id.isin(ingredients['ingredient_id']), np.nan)

        cost_price =    pd.to_numeric(sup_ing['cost_price'], errors='coerce')
        cost_price =    cost_price.where((cost_price > 0) & (cost_price < 5000), np.nan)

        df = pd.DataFrame({
            'supplier_id':supplier_id,
            'ingredient_id':ingredient_id,
            'cost_price':cost_price
        })

        return (
            df
            .dropna(subset= ['supplier_id', 'ingredient_id'])
            .sort_values(by = 'supplier_id')
            .reset_index(drop=True)
        )
