import duckdb
import pandas as pd
from pathlib import Path

RAW_DIR  = Path("data/raw")
PROC_DIR = Path("data/processed")
PROC_DIR.mkdir(parents=True, exist_ok=True)


def stg_screenings(con):
    con.execute(f"CREATE OR REPLACE TABLE raw_screenings AS SELECT * FROM read_parquet('{RAW_DIR}/screenings.parquet')")
    return con.execute("""
        SELECT
            screening_id,
            member_id,
            scn_region,
            county,
            TRY_CAST(screening_date AS DATE)            AS screening_date,
            CASE
                WHEN screening_tool NOT IN ('AHC-HRSN','PRAPARE','WellRx','HealthLeads')
                THEN 'Unknown'
                ELSE screening_tool
            END                                         AS screening_tool,
            screener_role,
            CAST(hrsn_flag AS BOOLEAN)                  AS hrsn_flag,
            COALESCE(primary_need_type, 'Not Identified') AS primary_need_type,
            CAST(food_insecurity AS BOOLEAN)            AS food_insecurity,
            CAST(housing_instability AS BOOLEAN)        AS housing_instability,
            CAST(transportation_need AS BOOLEAN)        AS transportation_need,
            CAST(utility_need AS BOOLEAN)               AS utility_need,
            CAST(interpersonal_safety AS BOOLEAN)       AS interpersonal_safety,
            (CAST(food_insecurity AS INT)
             + CAST(housing_instability AS INT)
             + CAST(transportation_need AS INT)
             + CAST(utility_need AS INT)
             + CAST(interpersonal_safety AS INT))       AS needs_count,
            CASE WHEN screening_date IS NULL THEN TRUE ELSE FALSE END  AS null_date_flag,
            CASE WHEN member_id IS NULL     THEN TRUE ELSE FALSE END   AS null_member_flag,
            CASE WHEN screening_tool NOT IN ('AHC-HRSN','PRAPARE','WellRx','HealthLeads')
                 THEN TRUE ELSE FALSE END                              AS invalid_tool_flag
        FROM raw_screenings
    """).df()


if __name__ == "__main__":
    con = duckdb.connect()
    df = stg_screenings(con)
    df.to_parquet(PROC_DIR / "stg_screenings.parquet", index=False)
    print(f"stg_screenings: {len(df):,} rows")
    con.close()