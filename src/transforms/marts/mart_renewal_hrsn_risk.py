import duckdb
import pandas as pd
from pathlib import Path

PROC_DIR = Path("data/processed")


def mart_renewal_hrsn_risk(con):
    con.execute(f"CREATE OR REPLACE TABLE int_renewal_hrsn AS SELECT * FROM read_parquet('{PROC_DIR}/int_renewal_hrsn.parquet')")
    con.execute(f"CREATE OR REPLACE TABLE stg_members AS SELECT * FROM read_parquet('{PROC_DIR}/stg_members.parquet')")
    return con.execute("""
        WITH base AS (
            SELECT
                rh.member_id, rh.county, rh.scn_region, rh.risk_tier,
                rh.eligibility_group, rh.renewal_outcome, rh.is_disenrolled,
                rh.language_barrier_flag, rh.hrsn_flag, rh.lapsed_and_unmet_hrsn,
                m.race_ethnicity
            FROM int_renewal_hrsn rh
            LEFT JOIN stg_members m USING (member_id)
        )
        SELECT
            county,
            scn_region,
            race_ethnicity,
            COUNT(DISTINCT member_id)                                       AS total_members,
            SUM(CASE WHEN is_disenrolled        THEN 1 ELSE 0 END)         AS coverage_lapsed,
            SUM(CASE WHEN hrsn_flag             THEN 1 ELSE 0 END)         AS hrsn_positive,
            SUM(CASE WHEN lapsed_and_unmet_hrsn THEN 1 ELSE 0 END)         AS lapsed_with_unmet_hrsn,
            SUM(CASE WHEN language_barrier_flag AND is_disenrolled
                     THEN 1 ELSE 0 END)                                    AS lapsed_language_barrier,
            ROUND(SUM(CASE WHEN is_disenrolled THEN 1 ELSE 0 END)
                  * 100.0 / NULLIF(COUNT(DISTINCT member_id), 0), 2)       AS disenrollment_rate_pct,
            ROUND(SUM(CASE WHEN lapsed_and_unmet_hrsn THEN 1 ELSE 0 END)
                  * 100.0 / NULLIF(COUNT(DISTINCT member_id), 0), 2)       AS high_risk_rate_pct
        FROM base
        GROUP BY county, scn_region, race_ethnicity
        ORDER BY high_risk_rate_pct DESC NULLS LAST
    """).df()


if __name__ == "__main__":
    con = duckdb.connect()
    df = mart_renewal_hrsn_risk(con)
    df.to_parquet(PROC_DIR / "mart_renewal_hrsn_risk.parquet", index=False)
    print(df.head(10).to_string())
    con.close()