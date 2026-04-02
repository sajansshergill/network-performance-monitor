import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.generate.members_generator          import generate_members
from src.generate.scn_screening_generator    import generate_screenings
from src.generate.service_delivery_generator import generate_services
from src.generate.renewal_outcomes_generator import generate_renewal_outcomes

RAW_DIR = Path("data/raw")
RAW_DIR.mkdir(parents=True, exist_ok=True)


def main():
    start = time.time()
    print("=" * 60)
    print("NY HERO SCN — Data Generation")
    print("=" * 60)

    print("\n[1/4] Members")
    members_df = generate_members(50_000)
    members_df.to_parquet(RAW_DIR / "members.parquet", index=False)

    print("\n[2/4] Screenings")
    screenings_df = generate_screenings(members_df)
    screenings_df.to_parquet(RAW_DIR / "screenings.parquet", index=False)

    print("\n[3/4] Services")
    services_df = generate_services(screenings_df)
    services_df.to_parquet(RAW_DIR / "services.parquet", index=False)

    print("\n[4/4] Renewals")
    renewals_df = generate_renewal_outcomes(members_df)
    renewals_df.to_parquet(RAW_DIR / "renewals.parquet", index=False)

    print(f"\n{'='*60}")
    print(f"Done in {time.time()-start:.1f}s")
    print(f"  members:    {len(members_df):>8,}")
    print(f"  screenings: {len(screenings_df):>8,}")
    print(f"  services:   {len(services_df):>8,}")
    print(f"  renewals:   {len(renewals_df):>8,}")


if __name__ == "__main__":
    main()