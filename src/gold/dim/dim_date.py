import pandas as pd
from base.base_gold_pipeline import BaseGoldPipeline

class DimDate(BaseGoldPipeline):

    def build(self) -> pd.DataFrame:

        date_range = pd.date_range(
            start="2023-01-01",
            end="2024-05-31"
        )

        df = pd.DataFrame({"date": date_range})

        df["date_key"] = df["date"].dt.strftime("%Y%m%d").astype(int)
        df["day"] = df["date"].dt.day
        df["month"] = df["date"].dt.month
        df["month_name"] = df["date"].dt.month_name()
        df["quarter"] = df["date"].dt.quarter
        df["year"] = df["date"].dt.year
        df["week_of_year"] = df["date"].dt.isocalendar().week
        df["is_weekend"] = df["date"].dt.weekday >= 5

        return df[
            ["date_key","date","day","month","month_name",
             "quarter","year","week_of_year","is_weekend"]
        ]
