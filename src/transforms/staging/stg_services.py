import duckdb
import pandas as pd
from pathlib import Path

RAW_DIR  = Path("data/raw")
PROC_DIR = Path("data/processed")
PROC_DIR.mkdir(parents=True, exist_ok=True)


def stg_services(con):
    con.execute(f"CREATE OR REPLACE TABLE raw_services AS SELECT * FROM read_parquet('{RAW_DIR}/services.parquet')")
    return con.execute("""
        SELECT
            service_id,
            member_id,
            screening_id,
            scn_region,
            county,
            TRY_CAST(service_date AS DATE)              AS service_date,
            service_type,
            COALESCE(service_subtype, 'General')        AS service_subtype,
            cbo_provider,
            CAST(days_to_service AS INTEGER)            AS days_to_service,
            CAST(within_30_days AS BOOLEAN)             AS within_30_days,
            service_outcome,
            CAST(follow_up_scheduled AS BOOLEAN)        AS follow_up_scheduled,
            CASE WHEN member_id    IS NULL THEN TRUE ELSE FALSE END AS null_member_flag,
            CASE WHEN service_date IS NULL THEN TRUE ELSE FALSE END AS null_date_flag
        FROM raw_services
    """).df()


if __name__ == "__main__":
    con = duckdb.connect()
    df = stg_services(con)
    df.to_parquet(PROC_DIR / "stg_services.parquet", index=False)
    print(f"stg_services: {len(df):,} rows")
    con.close()