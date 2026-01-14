import yaml
import numpy as np
import pandas as pd
from pathlib import Path
from base.base_silver_pipeline import BaseSilverPipeline

with open("src/silver/config/erp.yaml") as f:
    cfg = yaml.full_load(f)
    path = cfg['tables']['recipe']

class RecipeSilver(BaseSilverPipeline):

    def clean(self, recp: pd.DataFrame) -> pd.DataFrame:

        menu_items =        pd.read_parquet(path['menu_items_path'])
        item_id =           recp['item_id'].str.replace(r'\D','', regex=True).replace({'':np.nan})
        item_id =           pd.to_numeric(item_id, errors = 'coerce').replace(0,np.nan).astype('Int16')
        item_id =           ('I' + item_id.astype(str).str.zfill(4)).where(item_id.notnull(), np.nan)
        item_id =           item_id.where(item_id.isin(menu_items['item_id']), np.nan)

        ingredients =       pd.read_parquet(path['ingredients_path'])
        ingredient_id =     recp['ingredient_id'].str.replace(r'\D','', regex=True).replace({'':np.nan})
        ingredient_id =     pd.to_numeric(ingredient_id, errors='coerce').replace(0,np.nan).astype('Int16')
        ingredient_id =     ('ING' + ingredient_id.astype(str).str.zfill(3)).where(ingredient_id.notnull(), np.nan)
        ingredient_id =     ingredient_id.where(ingredient_id.isin(ingredients['ingredient_id']), np.nan)

        quantity_required = recp['quantity_required'].astype(str).str.strip().astype(float)
        quantity_required = quantity_required.where(quantity_required > 0, np.nan)

        df = pd.DataFrame({
            'item_id':item_id,
            'ingredient_id':ingredient_id,
            'quantity_required':quantity_required
        })

        return (
            df
            .dropna(subset = ['item_id', 'ingredient_id'])
            .sort_values(by = 'item_id')
            .reset_index(drop=True)
        )
