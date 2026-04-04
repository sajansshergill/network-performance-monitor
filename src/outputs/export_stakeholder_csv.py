"""Entry point for `python src/outputs/export_stakeholder_csv.py` — delegates to exports/."""

from pathlib import Path
import runpy

runpy.run_path(
    str(Path(__file__).resolve().parent / "exports" / "export_stakeholder_csv.py"),
    run_name="__main__",
)
