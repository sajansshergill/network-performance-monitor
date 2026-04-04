import duckdb
import pandas as pd
from pathlib import Path

RAW_DIR  = Path("data/raw")
PROC_DIR = Path("data/processed")
PROC_DIR.mkdir(parents=True, exist_ok=True)


def stg_renewals(con):
    con.execute(f"CREATE OR REPLACE TABLE raw_renewals AS SELECT * FROM read_parquet('{RAW_DIR}/renewals.parquet')")
    return con.execute("""
        SELECT
            renewal_id,
            member_id,
            county,
            scn_region,
            risk_tier,
            eligibility_group,
            TRY_CAST(renewal_date AS DATE)              AS renewal_date,
            renewal_outcome,
            COALESCE(disenrollment_reason, 'N/A')       AS disenrollment_reason,
            CAST(outreach_attempts AS INTEGER)          AS outreach_attempts,
            CAST(language_barrier_flag AS BOOLEAN)      AS language_barrier_flag,
            CASE WHEN renewal_outcome = 'Disenrolled'
                 THEN TRUE ELSE FALSE END               AS is_disenrolled
        FROM raw_renewals
    """).df()


if __name__ == "__main__":
    con = duckdb.connect()
    df = stg_renewals(con)
    df.to_parquet(PROC_DIR / "stg_renewals.parquet", index=False)
    print(f"stg_renewals: {len(df):,} rows")
    con.close()