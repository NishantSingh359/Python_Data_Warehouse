import numpy as np
import pandas as pd
from common.common import clean_id
from base.base_silver_pipeline import BaseSilverPipeline

class IngredientsSilver(BaseSilverPipeline):

    def clean(self, df: pd.DataFrame) -> pd.DataFrame:

        ingredient_id =   clean_id(df['ingredient_id'], 'ING', 3)

        ingredient_name = df['ingredient_name'].str.strip().replace({'':np.nan})

        unit =            df['unit'].str.lower()

        df = pd.DataFrame({
            'ingredient_id':ingredient_id,
            'ingredient_name':ingredient_name,
            'unit':unit
        })

        return (
            df
            .dropna(subset=['ingredient_id'])
            .drop_duplicates('ingredient_id')
            .sort_values('ingredient_id')
            .reset_index(drop=True)
        )
