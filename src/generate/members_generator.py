import random
import pandas as pd
from faker import Faker
from pathlib import Path

fake = Faker("en_US")
random.seed(42)
Faker.seed(42)

NYS_COUNTIES = [
    "Bronx", "Brooklyn", "Manhattan", "Queens", "Staten Island",
    "Albany", "Buffalo", "Rochester", "Syracuse", "Yonkers",
    "Nassau", "Suffolk", "Westchester", "Orange", "Dutchess",
    "Ulster", "Sullivan", "Delaware", "Onondaga", "Erie",
]

SCN_REGIONS = [
    "NYC-North", "NYC-South", "NYC-East", "NYC-West",
    "Hudson Valley", "Capital Region", "Western NY",
    "Central NY", "Finger Lakes", "Southern Tier",
    "North Country", "Long Island",
]

COUNTY_TO_REGION = {
    "Bronx": "NYC-North", "Manhattan": "NYC-North",
    "Brooklyn": "NYC-South", "Staten Island": "NYC-South",
    "Queens": "NYC-East", "Yonkers": "NYC-West",
    "Nassau": "Long Island", "Suffolk": "Long Island",
    "Westchester": "Hudson Valley", "Orange": "Hudson Valley",
    "Dutchess": "Hudson Valley", "Ulster": "Hudson Valley",
    "Sullivan": "Hudson Valley", "Delaware": "Southern Tier",
    "Albany": "Capital Region", "Onondaga": "Central NY",
    "Erie": "Western NY", "Buffalo": "Western NY",
    "Rochester": "Finger Lakes", "Syracuse": "Central NY",
}

RACE_ETHNICITIES = [
    "Hispanic or Latino", "Black or African American",
    "White Non-Hispanic", "Asian or Pacific Islander",
    "American Indian or Alaska Native", "Multiracial", "Unknown",
]
RACE_WEIGHTS = [0.28, 0.24, 0.22, 0.12, 0.03, 0.06, 0.05]

RISK_TIERS = ["Low", "Medium", "High", "Very High"]
RISK_WEIGHTS = [0.40, 0.30, 0.20, 0.10]

ELIGIBILITY_GROUPS = [
    "Adult", "Child", "Pregnant", "Disabled",
    "Foster Care", "Juvenile Justice",
]
ELIGIBILITY_WEIGHTS = [0.38, 0.30, 0.08, 0.14, 0.05, 0.05]


def generate_members(n: int = 50_000) -> pd.DataFrame:
    print(f"Generating {n:,} member records...")
    counties = random.choices(NYS_COUNTIES, k=n)
    records = []

    for i in range(n):
        county = counties[i]
        scn_region = COUNTY_TO_REGION.get(county, random.choice(SCN_REGIONS))
        dob = fake.date_of_birth(minimum_age=0, maximum_age=85)
        age = (pd.Timestamp.today().date() - dob).days // 365

        records.append({
            "member_id":         f"MBR{str(i + 1).zfill(7)}",
            "first_name":        fake.first_name(),
            "last_name":         fake.last_name(),
            "date_of_birth":     dob.isoformat(),
            "age":               age,
            "gender":            random.choices(["Male", "Female", "Non-binary"], weights=[0.48, 0.49, 0.03])[0],
            "race_ethnicity":    random.choices(RACE_ETHNICITIES, weights=RACE_WEIGHTS)[0],
            "county":            county,
            "scn_region":        scn_region,
            "zip_code":          fake.zipcode(),
            "eligibility_group": random.choices(ELIGIBILITY_GROUPS, weights=ELIGIBILITY_WEIGHTS)[0],
            "risk_tier":         random.choices(RISK_TIERS, weights=RISK_WEIGHTS)[0],
            "enrollment_date":   fake.date_between(start_date="-5y", end_date="-6m").isoformat(),
            "managed_care_plan": random.choice(["HealthFirst", "MetroPlus", "Fidelis", "Molina", "Wellcare", "Affinity"]),
            "primary_language":  random.choices(
                ["English", "Spanish", "Chinese", "Russian", "Bengali", "Haitian Creole", "Arabic"],
                weights=[0.55, 0.22, 0.07, 0.05, 0.04, 0.04, 0.03]
            )[0],
        })

    df = pd.DataFrame(records)
    print(f"  ✓ {len(df):,} members | {df['county'].nunique()} counties | {df['scn_region'].nunique()} regions")
    return df


if __name__ == "__main__":
    out = Path("data/raw/members.parquet")
    out.parent.mkdir(parents=True, exist_ok=True)
    df = generate_members(50_000)
    df.to_parquet(out, index=False)
    print(f"Saved → {out}")