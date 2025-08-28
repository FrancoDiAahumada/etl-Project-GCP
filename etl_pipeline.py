# etl_pipeline.py
import time
from typing import Any, Dict
from etl_medallion import run_etl  # importa tu pipeline de BigQuery

class CloudRunETL:
    def __init__(self) -> None:
        pass

    def run_etl_pipeline(self) -> Dict[str, Any]:
        start = time.time()
        run_etl()  # ejecuta tu pipeline medallion
        return {"duration_sec": round(time.time() - start, 2)}
