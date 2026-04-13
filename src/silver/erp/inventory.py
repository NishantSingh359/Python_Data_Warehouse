import yaml
import numpy as np
import pandas as pd
from common.common import clean_id
from pathlib import Path
from base.base_silver_pipeline import BaseSilverPipeline

with open("src/silver/config/erp.yaml") as f:
    cfg = yaml.full_load(f)
    path = cfg['tables']['inventory']

class InventorySilver(BaseSilverPipeline):

    def clean(self, df: pd.DataFrame) -> pd.DataFrame:

        restaurants =     pd.read_parquet(path['restaurants_path'])
        restaurant_id =   clean_id(df['restaurant_id'], 'R', 3)
        restaurant_id =   restaurant_id.where(restaurant_id.isin(restaurants['restaurant_id']))

        ingredients =     pd.read_parquet(path['ingredients_path'])
        ingredient_id =   clean_id(df['ingredient_id'], 'ING', 3)
        ingredient_id =   ingredient_id.where(ingredient_id.isin(ingredients['ingredient_id']))

        stock_qty =       df['stock_qty'].where((df['stock_qty'] > 0) & (df['stock_qty'] < 1000), np.nan)

        reorder_level =   df['reorder_level'].where(df['reorder_level'] > 0, np.nan)

        last_updated_at = pd.to_datetime(df['last_updated_at'], format= '%Y-%m-%d %H:%M:%S', errors= 'coerce')
        last_updated_at = last_updated_at.where((last_updated_at > '2022-01-01') & (last_updated_at <= '2025-12-31'))

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
