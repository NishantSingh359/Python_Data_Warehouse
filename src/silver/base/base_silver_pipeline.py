import datetime
import logging
import pandas as pd
from abc import ABC, abstractmethod

class BaseSilverPipeline(ABC):

    def __init__(self, config: dict):
        self.layer = "SILVER"
        self.domain = config["domain"]
        self.table = config["table"]
        self.raw_path = config["raw_path"]
        self.silver_path = config["silver_path"]
        self.dq_threshold = config.get("dq_threshold", 5)

    def run(self):
        start = datetime.datetime.now()

        try:
            logging.info('-' * 21)
            
            self.log("LOAD")
            df_raw = self.load()

            self.log("CLEAN", "| started")
            df_clean = self.clean(df_raw)

            self.log_stats(df_raw, df_clean)

            self.log("SAVE", " | target=parquet")
            self.save(df_clean)

            self.log_time(start)

            self.run_dq(df_raw, df_clean)

        except Exception as e:
            logging.exception(
                f"{self.layer} | {self.domain} | {self.table} | "
                f"error_type={type(e).__name__} message={e}"
            )

    # ---------- Common helpers ----------

    def load(self) -> pd.DataFrame:
        return pd.read_csv(self.raw_path)

    def save(self, df: pd.DataFrame):
        df.to_parquet(self.silver_path)

    def log(self, step, msg=""):
        logging.info(
            f"{self.layer} | {self.domain} | {step} | {self.table} {msg}"
        )

    def log_stats(self, before_df, after_df):
        before = before_df.shape[0]
        after = after_df.shape[0]
        drop = before - after
        drop_pct = round(drop / before * 100, 2)

        logging.info(
            f"{self.layer} | {self.domain} | CLEAN | {self.table} | "
            f"rows_before={before} rows_after={after} dropped={drop} pct={drop_pct}"
        )

        self.drop_pct = drop_pct

    def run_dq(self, before_df, after_df):
        if self.drop_pct > self.dq_threshold:
            logging.warning(
                f"{self.layer} | {self.domain} | DQ | {self.table} | "
                f"dropped_pct={self.drop_pct} threshold={self.dq_threshold}"
            )

    def log_time(self, start):
        duration = round((datetime.datetime.now() - start).total_seconds(), 4)
        logging.info(
            f"{self.layer} | {self.domain} | TIME | {self.table} | duration_sec={duration}"
        )

    # ---------- Mandatory override ----------
    @abstractmethod
    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        pass
