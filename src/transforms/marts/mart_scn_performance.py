import duckdb
import pandas as pd
from pathlib import Path

PROC_DIR = Path("data/processed")


def mart_scn_performance(con):
    con.execute(f"CREATE OR REPLACE TABLE stg_members AS SELECT * FROM read_parquet('{PROC_DIR}/stg_members.parquet')")
    con.execute(f"CREATE OR REPLACE TABLE int_member_screenings AS SELECT * FROM read_parquet('{PROC_DIR}/int_member_screenings.parquet')")
    return con.execute("""
        WITH region_totals AS (
            SELECT scn_region, COUNT(DISTINCT member_id) AS total_members
            FROM stg_members GROUP BY scn_region
        ),
        screening_summary AS (
            SELECT
                scn_region,
                COUNT(DISTINCT member_id)                    AS total_screened,
                COUNT(DISTINCT CASE
                    WHEN screening_date BETWEEN '2024-04-01' AND '2025-03-31'
                    THEN member_id END)                      AS screened_dy26,
                SUM(CASE WHEN hrsn_flag THEN 1 ELSE 0 END)  AS hrsn_positive,
                ROUND(AVG(needs_count), 2)                   AS avg_needs_count
            FROM int_member_screenings GROUP BY scn_region
        )
        SELECT
            rt.scn_region,
            rt.total_members,
            ss.total_screened,
            ss.screened_dy26,
            ss.hrsn_positive,
            ss.avg_needs_count,
            ROUND(ss.total_screened * 100.0 / NULLIF(rt.total_members, 0), 2)              AS screening_rate_pct,
            ROUND(ss.hrsn_positive  * 100.0 / NULLIF(ss.total_screened, 0), 2)             AS hrsn_prevalence_pct,
            ROUND(ss.total_screened * 100.0 / NULLIF(SUM(ss.total_screened) OVER(), 0), 2) AS pct_of_state_volume,
            CASE
                WHEN ss.screened_dy26 >= 0.25 * rt.total_members THEN 'On track'
                WHEN ss.screened_dy26 >= 0.15 * rt.total_members THEN 'At risk'
                ELSE 'Behind milestone'
            END AS dy26_milestone_status
        FROM region_totals rt
        LEFT JOIN screening_summary ss USING (scn_region)
        ORDER BY screening_rate_pct DESC NULLS LAST
    """).df()


if __name__ == "__main__":
    con = duckdb.connect()
    df = mart_scn_performance(con)
    df.to_parquet(PROC_DIR / "mart_scn_performance.parquet", index=False)
    print(df.to_string())
    con.close()