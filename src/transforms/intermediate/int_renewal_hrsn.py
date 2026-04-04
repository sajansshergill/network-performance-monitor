import duckdb
import pandas as pd
from pathlib import Path

PROC_DIR = Path("data/processed")


def int_renewal_hrsn(con):
    con.execute(f"CREATE OR REPLACE TABLE stg_renewals AS SELECT * FROM read_parquet('{PROC_DIR}/stg_renewals.parquet')")
    con.execute(f"CREATE OR REPLACE TABLE int_service_linkage AS SELECT * FROM read_parquet('{PROC_DIR}/int_service_linkage.parquet')")
    return con.execute("""
        SELECT
            r.renewal_id,
            r.member_id,
            r.county,
            r.scn_region,
            r.risk_tier,
            r.eligibility_group,
            r.renewal_date,
            r.renewal_outcome,
            r.is_disenrolled,
            r.disenrollment_reason,
            r.outreach_attempts,
            r.language_barrier_flag,
            sl.hrsn_flag,
            sl.primary_need_type,
            sl.needs_count,
            sl.service_gap_flag,
            sl.no_service_flag,
            CASE WHEN r.is_disenrolled = TRUE AND sl.hrsn_flag = TRUE
                 THEN TRUE ELSE FALSE END               AS lapsed_and_unmet_hrsn
        FROM stg_renewals r
        LEFT JOIN int_service_linkage sl USING (member_id)
    """).df()


if __name__ == "__main__":
    con = duckdb.connect()
    df = int_renewal_hrsn(con)
    df.to_parquet(PROC_DIR / "int_renewal_hrsn.parquet", index=False)
    print(f"int_renewal_hrsn: {len(df):,} rows")
    con.close()