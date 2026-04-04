import duckdb
import pandas as pd
from pathlib import Path

PROC_DIR = Path("data/processed")


def int_member_screenings(con):
    con.execute(f"CREATE OR REPLACE TABLE int_screenings_deduped AS SELECT * FROM read_parquet('{PROC_DIR}/int_screenings_deduped.parquet')")
    con.execute(f"CREATE OR REPLACE TABLE stg_members AS SELECT * FROM read_parquet('{PROC_DIR}/stg_members.parquet')")
    return con.execute("""
        SELECT
            s.screening_id,
            s.member_id,
            m.race_ethnicity,
            m.age,
            m.is_child,
            m.is_senior,
            m.eligibility_group,
            m.risk_tier,
            m.managed_care_plan,
            m.primary_language,
            s.scn_region,
            s.county,
            s.screening_date,
            s.screening_tool,
            s.hrsn_flag,
            s.primary_need_type,
            s.needs_count,
            s.food_insecurity,
            s.housing_instability,
            s.transportation_need,
            s.utility_need,
            s.interpersonal_safety,
            ROW_NUMBER() OVER (
                PARTITION BY s.member_id ORDER BY s.screening_date ASC NULLS LAST
            ) AS screening_seq,
            LAG(s.screening_date) OVER (
                PARTITION BY s.member_id ORDER BY s.screening_date ASC
            ) AS prior_screening_date
        FROM int_screenings_deduped s
        INNER JOIN stg_members m USING (member_id)
        WHERE s.null_date_flag   = FALSE
          AND s.null_member_flag = FALSE
    """).df()


if __name__ == "__main__":
    con = duckdb.connect()
    df = int_member_screenings(con)
    df.to_parquet(PROC_DIR / "int_member_screenings.parquet", index=False)
    print(f"int_member_screenings: {len(df):,} rows")
    con.close()