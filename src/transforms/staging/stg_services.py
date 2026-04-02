import duckdb
import pandas as pd
from pathlib import Path

RAW_DIR = Path("data/raw")
PROC_DIR = Path("data/processed")
PROC_DIR.mkdir(parents=True, exist_ok=True)

def stg_members(con):
    con.execute(f"CREATE OR REPLACE TABLE raw_members AS SELECT * FROM read_parquet('{RAW_DIR}/members.parquet')")
    