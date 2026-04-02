import random
import pandas as pd
from faker import Faker
from pathlib import Path
from datetime import date

fake = Faker("en_US")
random.seed(42)
Faker.seed(42)

RENEWAL_OUTCOMES = ["Renewed", "Disenrolled", "Pending", "Transferred to Essential Plan"]
BASE_WEIGHTS     = [0.58, 0.22, 0.12, 0.08]

DISENROLLMENT_REASONS = [
    "Income over threshold", "Did not return paperwork", "Address not found",
    "Enrolled in employer coverage", "Moved out of state", "No response to renewal notices",
]


def generate_renewal_outcomes(members_df: pd.DataFrame) -> pd.DataFrame:
    print(f"Generating renewal outcomes for {len(members_df):,} members...")
    records = []

    for _, member in members_df.iterrows():
        weights = BASE_WEIGHTS.copy()
        if member["risk_tier"] == "Very High":
            weights = [0.45, 0.35, 0.12, 0.08]
        elif member["risk_tier"] == "High":
            weights = [0.52, 0.28, 0.12, 0.08]
        elif member["risk_tier"] == "Low":
            weights = [0.68, 0.14, 0.11, 0.07]

        outcome = random.choices(RENEWAL_OUTCOMES, weights=weights)[0]
        renewal_date = fake.date_between(
            start_date=date(2023, 6, 1), end_date=date(2025, 3, 31)
        ).isoformat()

        records.append({
            "renewal_id":            f"RNW{str(len(records) + 1).zfill(7)}",
            "member_id":             member["member_id"],
            "county":                member["county"],
            "scn_region":            member["scn_region"],
            "risk_tier":             member["risk_tier"],
            "eligibility_group":     member["eligibility_group"],
            "renewal_date":          renewal_date,
            "renewal_outcome":       outcome,
            "disenrollment_reason":  random.choice(DISENROLLMENT_REASONS) if outcome == "Disenrolled" else None,
            "outreach_attempts":     random.randint(0, 5) if outcome == "Disenrolled" else random.randint(0, 2),
            "language_barrier_flag": member["primary_language"] != "English" and random.random() < 0.35,
        })

    df = pd.DataFrame(records)
    print(f"  ✓ Renewed: {(df['renewal_outcome']=='Renewed').mean():.1%} | Disenrolled: {(df['renewal_outcome']=='Disenrolled').mean():.1%}")
    return df


if __name__ == "__main__":
    members_df = pd.read_parquet("data/raw/members.parquet")
    df = generate_renewal_outcomes(members_df)
    out = Path("data/raw/renewals.parquet")
    df.to_parquet(out, index=False)
    print(f"Saved → {out}")