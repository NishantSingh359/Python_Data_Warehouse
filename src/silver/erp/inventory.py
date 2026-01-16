import yaml
import numpy as np
import pandas as pd
from pathlib import Path
from base.base_silver_pipeline import BaseSilverPipeline

with open("src/silver/config/erp.yaml") as f:
    cfg = yaml.full_load(f)
    path = cfg['tables']['inventory']

class InventorySilver(BaseSilverPipeline):

    def clean(self, df: pd.DataFrame) -> pd.DataFrame:

        restaurants =     pd.read_parquet(path['restaurants_path'])
        restaurant_id =   df['restaurant_id'].str.replace(r'\D', '', regex=True).replace({'':np.nan})
        restaurant_id =   pd.to_numeric(restaurant_id, errors='coerce').replace(0, np.nan).astype('Int16')
        restaurant_id =   ('R'+restaurant_id.astype(str).str.zfill(3)).where(restaurant_id.notnull(), np.nan)
        restaurant_id =   restaurant_id.where(restaurant_id.isin(restaurants['restaurant_id']), np.nan)

        ingredients =     pd.read_parquet(path['ingredients_path'])
        ingredient_id =   df['ingredient_id'].replace(r'\D', '', regex=True).replace({'':np.nan})
        ingredient_id =   pd.to_numeric(ingredient_id, errors='coerce').replace(0, np.nan).astype('Int16')
        ingredient_id =   ('ING'+ingredient_id.astype(str).str.zfill(3)).where(ingredient_id.notna(), np.nan)
        ingredient_id =   ingredient_id.where(ingredient_id.isin(ingredients['ingredient_id']), np.nan)

        stock_qty =       df['stock_qty'].astype(str).str.strip().astype(float)
        stock_qty =       stock_qty.where((stock_qty > 0) & (stock_qty < 1000), np.nan)

        reorder_level =   df['reorder_level'].astype(str).str.strip().astype(float)
        reorder_level =   reorder_level.where(reorder_level > 0, np.nan)

        last_updated_at = df['last_updated_at'].str.strip()
        last_updated_at = pd.to_datetime(last_updated_at, format= '%Y-%m-%d %H:%M:%S', errors= 'coerce')

        df = pd.DataFrame({
            'restaurant_id':restaurant_id,
            'ingredient_id':ingredient_id,
            'stock_qty':stock_qty,
            'reorder_level':reorder_level,
            'last_updated_at':last_updated_at
        })

        return (
            df
             .dropna(subset= 'restaurant_id')
             .sort_values(by = 'restaurant_id')
             .reset_index(drop=True)
        )
