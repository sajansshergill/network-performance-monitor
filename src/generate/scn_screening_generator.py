import random
import pandas as pd
from faker import Faker
from pathlib import Path
from datetime import date

fake = Faker("en_US")
random.seed(42)
Faker.seed(42)

NEED_TYPES    = ["Housing", "Nutrition", "Transportation", "Case Management"]
NEED_WEIGHTS  = [0.30, 0.28, 0.22, 0.20]
SCREENING_TOOLS  = ["AHC-HRSN", "PRAPARE", "WellRx", "HealthLeads"]
SCREENER_ROLES   = ["Community Health Worker", "Social Worker", "Nurse", "Care Coordinator"]


def generate_screenings(members_df: pd.DataFrame, screening_rate: float = 0.72) -> pd.DataFrame:
    print(f"Generating screening records (base rate: {screening_rate:.0%})...")
    screened_members = []

    for _, member in members_df.iterrows():
        rate = screening_rate
        if member["risk_tier"] == "High":
            rate = min(0.92, screening_rate + 0.15)
        elif member["risk_tier"] == "Very High":
            rate = min(0.97, screening_rate + 0.22)
        elif member["risk_tier"] == "Low":
            rate = max(0.40, screening_rate - 0.20)
        if random.random() < rate:
            screened_members.append(member)

    print(f"  ✓ {len(screened_members):,} members selected for screening")
    records = []

    for i, member in enumerate(screened_members):
        screening_date = fake.date_between(
            start_date=date(2024, 4, 1), end_date=date(2025, 3, 31)
        )
        hrsn_flag    = random.random() < 0.55
        primary_need = random.choices(NEED_TYPES, weights=NEED_WEIGHTS)[0] if hrsn_flag else None

        introduce_issue = random.random() < 0.08
        issue_type = random.choice(["null_need", "null_date", "invalid_tool"]) if introduce_issue else None

        records.append({
            "screening_id":      f"SCR{str(i + 1).zfill(8)}",
            "member_id":         member["member_id"],
            "scn_region":        member["scn_region"],
            "county":            member["county"],
            "screening_date":    None if issue_type == "null_date" else screening_date.isoformat(),
            "screening_tool":    "UNKNOWN_TOOL" if issue_type == "invalid_tool"
                                 else random.choice(SCREENING_TOOLS),
            "screener_role":     random.choice(SCREENER_ROLES),
            "hrsn_flag":         hrsn_flag,
            "primary_need_type": None if issue_type == "null_need" else primary_need,
            "food_insecurity":        random.random() < 0.30 if hrsn_flag else False,
            "housing_instability":    random.random() < 0.28 if hrsn_flag else False,
            "transportation_need":    random.random() < 0.22 if hrsn_flag else False,
            "utility_need":           random.random() < 0.15 if hrsn_flag else False,
            "interpersonal_safety":   random.random() < 0.10 if hrsn_flag else False,
            "needs_count":       None,
            "data_issue_flag":   issue_type is not None,
        })

    # Introduce ~2% duplicate screening_ids
    n_dupes = int(len(records) * 0.02)
    for idx in random.sample(range(len(records)), n_dupes):
        records.append(records[idx].copy())

    df = pd.DataFrame(records)
    print(f"  ✓ {len(df):,} records | HRSN rate: {df['hrsn_flag'].mean():.1%} | issue rate: {df['data_issue_flag'].mean():.1%}")
    return df


if __name__ == "__main__":
    members_df = pd.read_parquet("data/raw/members.parquet")
    df = generate_screenings(members_df)
    out = Path("data/raw/screenings.parquet")
    df.to_parquet(out, index=False)
    print(f"Saved → {out}")