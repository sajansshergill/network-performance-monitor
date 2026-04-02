import random
import pandas as pd
from faker import Faker
from pathlib import Path
from datetime import timedelta

fake = Faker("en_US")
random.seed(42)
Faker.seed(42)

SERVICE_TYPES = {
    "Housing":        ["Emergency shelter placement", "Rental assistance", "Housing navigation", "Utility assistance"],
    "Nutrition":      ["Food pantry referral", "SNAP enrollment", "Home meal delivery", "WIC referral"],
    "Transportation": ["Medical transport voucher", "Bus pass program", "Ride share coordination"],
    "Case Management":["Care coordination", "Benefits navigation", "Mental health referral", "Substance use referral"],
}

SERVICE_OUTCOMES = ["Completed", "Pending", "Declined", "Unable to reach", "Referred out"]
OUTCOME_WEIGHTS  = [0.48, 0.18, 0.12, 0.14, 0.08]

CBO_PROVIDERS = [
    "BronxWorks", "CAMBA", "FEGS Health & Human Services", "Henry Street Settlement",
    "Catholic Charities", "Urban Health Plan", "Community Healthcare Network",
    "Northwell Community Connect", "NYC Health + Hospitals Social Care",
    "Rochester Regional Health CBO", "Erie County HRSN Hub",
]


def generate_services(screenings_df: pd.DataFrame, service_rate: float = 0.62) -> pd.DataFrame:
    print(f"Generating service delivery records (base rate: {service_rate:.0%})...")
    hrsn_positive = screenings_df[screenings_df["hrsn_flag"] == True].copy()
    print(f"  ✓ {len(hrsn_positive):,} HRSN-positive members eligible for services")

    records = []
    for i, row in enumerate(hrsn_positive.itertuples(index=False)):
        rate = service_rate
        if row.scn_region in ["North Country", "Southern Tier"]:
            rate = max(0.30, service_rate - 0.25)
        elif row.scn_region in ["NYC-North", "NYC-South"]:
            rate = min(0.80, service_rate + 0.10)

        if random.random() > rate:
            continue

        screening_date = pd.to_datetime(row.screening_date)
        delay_days = random.choices(
            [random.randint(1, 14), random.randint(15, 30), random.randint(31, 90)],
            weights=[0.45, 0.30, 0.25]
        )[0]
        service_date = screening_date + timedelta(days=delay_days)
        need_type = row.primary_need_type if row.primary_need_type else random.choice(list(SERVICE_TYPES.keys()))

        records.append({
            "service_id":          f"SVC{str(i + 1).zfill(8)}",
            "member_id":           row.member_id,
            "screening_id":        row.screening_id,
            "scn_region":          row.scn_region,
            "county":              row.county,
            "service_date":        service_date.date().isoformat(),
            "service_type":        need_type,
            "service_subtype":     random.choice(SERVICE_TYPES.get(need_type, ["General service"])),
            "cbo_provider":        random.choice(CBO_PROVIDERS),
            "days_to_service":     delay_days,
            "within_30_days":      delay_days <= 30,
            "service_outcome":     random.choices(SERVICE_OUTCOMES, weights=OUTCOME_WEIGHTS)[0],
            "follow_up_scheduled": random.random() < 0.55,
        })

    df = pd.DataFrame(records)
    print(f"  ✓ {len(df):,} service records | within-30d: {df['within_30_days'].mean():.1%}")
    return df


if __name__ == "__main__":
    screenings_df = pd.read_parquet("data/raw/screenings.parquet")
    df = generate_services(screenings_df)
    out = Path("data/raw/services.parquet")
    df.to_parquet(out, index=False)
    print(f"Saved → {out}")