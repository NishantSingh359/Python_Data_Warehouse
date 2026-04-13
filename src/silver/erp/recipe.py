import yaml
import numpy as np
import pandas as pd
from pathlib import Path
from common.common import clean_id
from base.base_silver_pipeline import BaseSilverPipeline

with open("src/silver/config/erp.yaml") as f:
    cfg = yaml.full_load(f)
    path = cfg['tables']['recipe']

class RecipeSilver(BaseSilverPipeline):

    def clean(self, df: pd.DataFrame) -> pd.DataFrame:

        item_id  = clean_id(df['item_id'], 'I', 4)

        ingredient_id = clean_id(df['ingredient_id'], 'ING', 3)

        quantity_required = df['quantity_required'].where((df['quantity_required'] > 0) & (df['quantity_required'] < 1))

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
