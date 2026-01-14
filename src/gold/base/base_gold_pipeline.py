import datetime
import logging
import pandas as pd
from abc import ABC, abstractmethod

class BaseGoldPipeline(ABC):

    def __init__(self, config: dict):
        self.layer = "GOLD"
        self.domain = config["domain"]   # DIM / FACT
        self.table = config["table"]
        self.silver_path = config["silver_path"]
        self.gold_path = config["gold_path"]
    
    def run(self):
        start = datetime.datetime.now()

        try:
            logging.info('-' * 21)
            self.log("START")
            df = self.build()
            self.save(df)
            self.log_shape(df)
            self.log_time(start)

        except Exception as e:
            logging.exception(
                f"{self.layer} | {self.domain} | {self.table} | "
                f"error_type={type(e).__name__} message={e}"
            )

    # ---------- Common helpers ----------

    def save(self, df: pd.DataFrame):
        self.log("SAVE", "target=parquet")
        df.to_parquet(self.gold_path)

    def log(self, step, msg=""):
        logging.info(
            f"{self.layer} | {self.domain} | {step} | {self.table} {msg}"
        )

    def log_shape(self, df):
        logging.info(
            f"{self.layer} | {self.domain} | SHAPE | {self.table} | "
            f"rows={df.shape[0]} columns={df.shape[1]}"
        )

    def log_time(self, start):
        duration = round((datetime.datetime.now() - start).total_seconds(), 4)
        logging.info(
            f"{self.layer} | {self.domain} | TIME | {self.table} | duration_sec={duration}"
        )

    # ---------- Mandatory override ----------
    @abstractmethod
    def build(self) -> pd.DataFrame:
        pass
