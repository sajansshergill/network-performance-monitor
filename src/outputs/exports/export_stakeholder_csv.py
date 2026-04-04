"""Export mart parquet tables to CSV under outputs/ (stakeholder-ready)."""

from pathlib import Path

import pandas as pd

PROC_DIR = Path("data/processed")
OUT_DIR = Path("outputs")

MARTS = [
    ("mart_scn_performance.parquet", "mart_scn_performance.csv"),
    ("mart_service_gap_index.parquet", "mart_service_gap_index.csv"),
    ("mart_renewal_hrsn_risk.parquet", "mart_renewal_hrsn_risk.csv"),
]


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for pq_name, csv_name in MARTS:
        path = PROC_DIR / pq_name
        if not path.exists():
            print(f"Skip (missing): {path}")
            continue
        df = pd.read_parquet(path)
        out = OUT_DIR / csv_name
        df.to_csv(out, index=False)
        print(f"Wrote {out} ({len(df):,} rows)")


if __name__ == "__main__":
    main()
