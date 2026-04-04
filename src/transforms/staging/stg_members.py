import duckdb
import pandas as pd
from pathlib import Path

RAW_DIR  = Path("data/raw")
PROC_DIR = Path("data/processed")
PROC_DIR.mkdir(parents=True, exist_ok=True)


def stg_members(con):
    con.execute(f"CREATE OR REPLACE TABLE raw_members AS SELECT * FROM read_parquet('{RAW_DIR}/members.parquet')")
    return con.execute("""
        SELECT
            member_id,
            TRIM(first_name)                            AS first_name,
            TRIM(last_name)                             AS last_name,
            TRY_CAST(date_of_birth AS DATE)             AS date_of_birth,
            CAST(age AS INTEGER)                        AS age,
            COALESCE(gender, 'Unknown')                 AS gender,
            COALESCE(race_ethnicity, 'Unknown')         AS race_ethnicity,
            county,
            scn_region,
            zip_code,
            eligibility_group,
            risk_tier,
            TRY_CAST(enrollment_date AS DATE)           AS enrollment_date,
            managed_care_plan,
            primary_language,
            CASE WHEN age < 18  THEN TRUE ELSE FALSE END AS is_child,
            CASE WHEN age >= 65 THEN TRUE ELSE FALSE END AS is_senior,
            CASE WHEN member_id IS NULL THEN TRUE ELSE FALSE END AS member_id_null_flag
        FROM raw_members
    """).df()


if __name__ == "__main__":
    con = duckdb.connect()
    df = stg_members(con)
    df.to_parquet(PROC_DIR / "stg_members.parquet", index=False)
    print(f"stg_members: {len(df):,} rows")
    con.close()