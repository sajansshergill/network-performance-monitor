import duckdb
import pandas as pd
from pathlib import Path

PROC_DIR = Path("data/processed")


def mart_service_gap_index(con):
    con.execute(f"CREATE OR REPLACE TABLE int_service_linkage AS SELECT * FROM read_parquet('{PROC_DIR}/int_service_linkage.parquet')")
    return con.execute("""
        SELECT
            county,
            scn_region,
            primary_need_type,
            COUNT(*)                                                    AS screened_with_need,
            SUM(CASE WHEN no_service_flag  THEN 1 ELSE 0 END)          AS no_service_count,
            SUM(CASE WHEN service_gap_flag THEN 1 ELSE 0 END)          AS gap_within_30d_count,
            ROUND(SUM(CASE WHEN service_gap_flag THEN 1 ELSE 0 END)
                  * 100.0 / NULLIF(COUNT(*), 0), 2)                    AS service_gap_rate_pct,
            ROUND(SUM(CASE WHEN no_service_flag  THEN 1 ELSE 0 END)
                  * 100.0 / NULLIF(COUNT(*), 0), 2)                    AS no_service_rate_pct,
            ROUND(AVG(days_to_first_service), 1)                       AS avg_days_to_service
        FROM int_service_linkage
        WHERE hrsn_flag = TRUE
          AND primary_need_type != 'Not Identified'
        GROUP BY county, scn_region, primary_need_type
        ORDER BY service_gap_rate_pct DESC NULLS LAST
    """).df()


if __name__ == "__main__":
    con = duckdb.connect()
    df = mart_service_gap_index(con)
    df.to_parquet(PROC_DIR / "mart_service_gap_index.parquet", index=False)
    print(df.head(10).to_string())
    con.close()