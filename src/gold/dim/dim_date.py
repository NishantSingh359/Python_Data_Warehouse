import pandas as pd
import datetime
from base.base_gold_pipeline import BaseGoldPipeline

class DimDate(BaseGoldPipeline):

    def build(self) -> pd.DataFrame:

        date_range = pd.date_range(
            start="2021-01-01",
            end="2025-01-31"
        )

        df = pd.DataFrame({"date": date_range})

        df["date_key"] = df["date"].dt.strftime("%Y%m%d").astype(int) # type: ignore
        df["day"] = df["date"].dt.day # type: ignore
        df["month"] = df["date"].dt.month # type: ignore
        df["month_name"] = df["date"].dt.month_name() # type: ignore
        df["quarter"] = df["date"].dt.quarter # type: ignore
        df["year"] = df["date"].dt.year # type: ignore
        df["week_of_year"] = df["date"].dt.isocalendar().week # type: ignore
        df["is_weekend"] = df["date"].dt.weekday >= 5 # type: ignore

        return df
