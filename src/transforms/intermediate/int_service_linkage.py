import duckdb
import pandas as pd
from pathlib import Path

PROC_DIR = Path("data/processed")


def int_service_linkage(con):
    con.execute(f"CREATE OR REPLACE TABLE int_member_screenings AS SELECT * FROM read_parquet('{PROC_DIR}/int_member_screenings.parquet')")
    con.execute(f"CREATE OR REPLACE TABLE stg_services AS SELECT * FROM read_parquet('{PROC_DIR}/stg_services.parquet')")
    return con.execute("""
        WITH first_service AS (
            SELECT
                member_id,
                screening_id,
                MIN(service_date)      AS first_service_date,
                MIN(days_to_service)   AS days_to_first_service,
                COUNT(*)               AS total_services
            FROM stg_services
            GROUP BY member_id, screening_id
        )
        SELECT
            ms.screening_id,
            ms.member_id,
            ms.scn_region,
            ms.county,
            ms.race_ethnicity,
            ms.risk_tier,
            ms.screening_date,
            ms.hrsn_flag,
            ms.primary_need_type,
            ms.needs_count,
            fs.first_service_date,
            fs.days_to_first_service,
            COALESCE(fs.total_services, 0)              AS total_services,
            CASE WHEN ms.hrsn_flag = TRUE AND fs.first_service_date IS NULL
                 THEN TRUE ELSE FALSE END               AS no_service_flag,
            CASE WHEN ms.hrsn_flag = TRUE
                  AND (fs.first_service_date IS NULL
                       OR fs.days_to_first_service > 30)
                 THEN TRUE ELSE FALSE END               AS service_gap_flag
        FROM int_member_screenings ms
        LEFT JOIN first_service fs
            ON ms.member_id    = fs.member_id
           AND ms.screening_id = fs.screening_id
    """).df()


if __name__ == "__main__":
    con = duckdb.connect()
    df = int_service_linkage(con)
    df.to_parquet(PROC_DIR / "int_service_linkage.parquet", index=False)
    print(f"int_service_linkage: {len(df):,} rows")
    con.close()