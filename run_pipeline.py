#!/usr/bin/env python3
"""Generate raw data, then staging → intermediate → marts. Run from repo root."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def run(cmd: list[str]) -> None:
    print(f"\n→ {' '.join(cmd)}", flush=True)
    r = subprocess.run(cmd, cwd=ROOT)
    if r.returncode != 0:
        sys.exit(r.returncode)


def main() -> None:
    run([sys.executable, str(ROOT / "src" / "generate" / "run_generate.py")])

    staging = [
        "src/transforms/staging/stg_members.py",
        "src/transforms/staging/stg_screenings.py",
        "src/transforms/staging/stg_services.py",
        "src/transforms/staging/stg_renewals.py",
    ]
    for script in staging:
        run([sys.executable, str(ROOT / script)])

    intermediate = [
        "src/transforms/intermediate/int_screenings_deduped.py",
        "src/transforms/intermediate/int_member_screenings.py",
        "src/transforms/intermediate/int_service_linkage.py",
        "src/transforms/intermediate/int_renewal_hrsn.py",
    ]
    for script in intermediate:
        run([sys.executable, str(ROOT / script)])

    marts = [
        "src/transforms/marts/mart_scn_performance.py",
        "src/transforms/marts/mart_service_gap_index.py",
        "src/transforms/marts/mart_renewal_hrsn_risk.py",
    ]
    for script in marts:
        run([sys.executable, str(ROOT / script)])

    run([sys.executable, str(ROOT / "src" / "outputs" / "exports" / "export_stakeholder_csv.py")])

    print("\n" + "=" * 60)
    print("Pipeline finished successfully.")
    print("=" * 60)


if __name__ == "__main__":
    main()
