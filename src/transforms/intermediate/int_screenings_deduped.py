import duckdb
import pandas as pd
from pathlib import Path

PROC_DIR = Path("data/processed")


def int_screenings_deduped(con):
    con.execute(f"CREATE OR REPLACE TABLE stg_screenings AS SELECT * FROM read_parquet('{PROC_DIR}/stg_screenings.parquet')")
    return con.execute("""
        WITH ranked AS (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY screening_id
                    ORDER BY screening_date DESC NULLS LAST
                ) AS rn
            FROM stg_screenings
        )
        SELECT * EXCLUDE (rn) FROM ranked WHERE rn = 1
    """).df()


if __name__ == "__main__":
    con = duckdb.connect()
    df = int_screenings_deduped(con)
    df.to_parquet(PROC_DIR / "int_screenings_deduped.parquet", index=False)
    print(f"int_screenings_deduped: {len(df):,} rows")
    con.close()