import numpy as np
import pandas as pd
from base.base_silver_pipeline import BaseSilverPipeline

class IngredientsSilver(BaseSilverPipeline):

    def clean(self, ing: pd.DataFrame) -> pd.DataFrame:

        ingredient_id =   ing['ingredient_id'].str.replace(r'\D', '', regex=True).replace({'':np.nan})
        ingredient_id =   pd.to_numeric(ingredient_id, errors='coerce').replace(0,np.nan).astype('Int16')
        ingredient_id =   ingredient_id.fillna(ing['ingredient_name'].str.replace(r'\D', '', regex=True).replace({'':np.nan}))
        ingredient_id =   ('ING' + ingredient_id.astype(str).str.zfill(3)).where(ingredient_id.notnull(), np.nan)

        ingredient_name = ingredient_id.str.replace(r'\D', '', regex=True).astype('Int16')
        ingredient_name = ('Ingredient_' + ingredient_name.astype(str)).where(ingredient_id.notnull(), np.nan)

        unit =            ing['unit'].astype(str).str.strip().str.lower().replace({'nan': np.nan})

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
