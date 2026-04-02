# ny-hero-scn-performance-monitor

> Python-first data engineering pipeline for monitoring Social Care Network (SCN) performance across New York State's HERO initiative — built to mirror the statewide data infrastructure UHF is actively constructing under the 2024 NYS 1115 Medicaid Waiver Amendment.

---

## Context

In January 2025, the New York State Department of Health named United Hospital Fund (UHF) as the lead coordinator of the state's Health Equity Regional Organization (HERO) — a core initiative under the $7.5B NYS 1115 Medicaid Waiver Amendment. UHF's mandate is to bridge public health, social services, and healthcare delivery by building the data infrastructure that tracks whether Medicaid members with social needs are actually being reached and served.

At the center of this infrastructure are Social Care Networks (SCNs) — regional entities responsible for screening Medicaid members for Health-Related Social Needs (HRSN), connecting them to services (housing, nutrition, transportation, case management), and reporting outcomes back to the state.

This project builds the data pipeline that UHF's data team would need in year one: ingest multi-region SCN reporting data, clean and transform it, detect service gaps, and produce policy-ready outputs for state stakeholders.

---

## Problem Statement

SCNs across NYS submit screening and service delivery data at different cadences, with inconsistent field completeness and varying service mix. Without a centralized transformation layer, it is impossible to answer the questions that matter to policymakers:

- Which regions are hitting their HRSN screening milestones?
- Where are members being screened but receiving no follow-up service?
- Which need types — housing, food, transportation — are most underserved and in which counties?
- Among members who lost Medicaid coverage during the renewal unwind, how many also have unmet HRSN?

This pipeline answers all four.

---

## Architecture

```
Multi-Region SCN Sources
        │
        ▼
┌──────────────────────────────────────────┐
│  Layer 1: Synthetic Data Generation      │  ← Python (Faker, pandas)
│  src/generate/                           │
│  · SCN screening records (12 regions)    │
│  · HRSN service delivery logs            │
│  · Member demographics + risk tiers      │
│  · Medicaid renewal outcomes (unwind)    │
└──────────────────────┬───────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────┐
│  Layer 2: SQL Transformation (DuckDB)    │
│  src/sql/                                │
│  staging/     → type casting, nulls      │
│  intermediate/→ deduplication, joins     │
│  marts/       → performance metrics      │
└──────────────────────┬───────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────┐
│  Layer 3: Data Quality Automation        │  ← Great Expectations + pytest
│  src/quality/                            │
│  · Null checks per SCN region            │
│  · Referential integrity validation      │
│  · Screening milestone threshold alerts  │
└──────────────────────┬───────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────┐
│  Layer 4: Policy-Ready Outputs           │  ← Python (Plotly, Jinja2) + Streamlit
│  src/outputs/                            │
│  · SCN region comparison dashboard       │
│  · Service gap index by county           │
│  · Python HTML briefing export           │
│  · One-click stakeholder CSV             │
└──────────────────────────────────────────┘
```

---

## Tech Stack

| Component | Tool | Purpose |
|---|---|---|
| Data generation | Python, Faker, pandas | Synthetic multi-region SCN + member dataset |
| Analytical SQL | DuckDB (via Python) | In-process OLAP — CTEs, window functions, query optimization |
| Transformations | pandas, DuckDB Python API | DataFrame-level cleaning and metric computation |
| Data quality | Great Expectations, pytest | Automated validation; HTML data docs |
| Visualization | Plotly, matplotlib | Equity charts and service gap plots |
| Briefing export | Jinja2 + WeasyPrint | Python-rendered HTML → PDF policy summary |
| Dashboard | Streamlit | Regional SCN performance dashboard |
| Orchestration | Python scripts + GitHub Actions CI | End-to-end pipeline execution |
| Containerization | Docker | Reproducible environments |
| Versioning | Git + GitHub | Modular codebase versioning, PR-based review |

---

## Repository Structure

```
ny-hero-scn-performance-monitor/
├── src/
│   ├── generate/
│   │   ├── scn_screening_generator.py      # HRSN screening records by region
│   │   ├── service_delivery_generator.py   # Service logs: housing, food, transport
│   │   ├── members_generator.py            # Demographics, risk tier, county
│   │   └── renewal_outcomes_generator.py   # Medicaid unwind renewal status
│   ├── transforms/
│   │   ├── staging/
│   │   │   ├── stg_screenings.py           # Raw → typed, null coalescing
│   │   │   ├── stg_services.py
│   │   │   ├── stg_members.py
│   │   │   └── stg_renewals.py
│   │   ├── intermediate/
│   │   │   ├── int_screenings_deduped.py   # Window-function deduplication via DuckDB
│   │   │   ├── int_member_screenings.py    # Members joined to screening history
│   │   │   ├── int_service_linkage.py      # Screening → service follow-up linkage
│   │   │   └── int_renewal_hrsn.py         # Members with lapsed coverage + HRSN
│   │   └── marts/
│   │       ├── mart_scn_performance.py     # Region-level milestone tracking
│   │       ├── mart_service_gap_index.py   # Unmet need by county + need type
│   │       └── mart_renewal_hrsn_risk.py   # High-risk intersection: lapsed + HRSN
│   ├── quality/
│   │   ├── expectations/
│   │   │   ├── screenings_suite.json
│   │   │   └── services_suite.json
│   │   └── run_quality_checks.py
│   ├── outputs/
│   │   ├── briefing/
│   │   │   ├── generate_briefing.py        # Renders HTML → PDF via Jinja2 + WeasyPrint
│   │   │   └── template.html               # Jinja2 HTML template for policy summary
│   │   └── exports/
│   │       └── export_stakeholder_csv.py   # One-click mart → CSV export
│   └── dashboard/
│       └── app.py                          # Streamlit regional performance view
├── tests/
│   ├── test_staging.py
│   ├── test_intermediate.py
│   └── test_marts.py
├── docs/
│   ├── data_dictionary.md
│   └── metric_definitions.md
├── .github/
│   └── workflows/
│       └── pipeline.yml
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

## Python Highlights

**Screening milestone tracking — DuckDB called from Python with window functions:**
```python
import duckdb
import pandas as pd

def compute_scn_performance(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    return con.execute("""
        WITH screening_milestones AS (
            SELECT
                scn_region,
                COUNT(DISTINCT member_id)                                       AS total_screened,
                COUNT(DISTINCT CASE
                    WHEN screening_date BETWEEN '2024-04-01' AND '2025-03-31'
                    THEN member_id END)                                         AS screened_dy26,
                SUM(CASE WHEN hrsn_flag = TRUE THEN 1 ELSE 0 END)              AS members_with_need,
                ROUND(
                    COUNT(DISTINCT member_id) * 100.0
                    / NULLIF(SUM(COUNT(DISTINCT member_id)) OVER (), 0), 2
                )                                                               AS pct_of_state_total
            FROM int_member_screenings
            GROUP BY scn_region
        )
        SELECT
            *,
            CASE
                WHEN screened_dy26 >= 0.25 * total_screened THEN 'On track'
                WHEN screened_dy26 >= 0.15 * total_screened THEN 'At risk'
                ELSE 'Behind milestone'
            END AS dy26_milestone_status
        FROM screening_milestones
        ORDER BY pct_of_state_total DESC
    """).df()
```

**Service gap index — pandas post-processing on DuckDB mart output:**
```python
import duckdb
import pandas as pd

def compute_service_gap_index(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    df = con.execute("""
        WITH screened_with_need AS (
            SELECT
                s.member_id,
                s.county,
                s.primary_need_type,
                s.screening_date,
                MIN(sv.service_date) AS first_service_date
            FROM int_member_screenings s
            LEFT JOIN int_service_linkage sv
                ON s.member_id = sv.member_id
                AND sv.service_date >= s.screening_date
            WHERE s.hrsn_flag = TRUE
            GROUP BY 1, 2, 3, 4
        )
        SELECT
            county,
            primary_need_type,
            COUNT(*)                                                    AS screened_with_need,
            SUM(CASE
                WHEN first_service_date IS NULL
                  OR DATEDIFF('day', screening_date, first_service_date) > 30
                THEN 1 ELSE 0 END)                                      AS unmet_within_30d
        FROM screened_with_need
        GROUP BY county, primary_need_type
    """).df()

    df["service_gap_rate_pct"] = (
        df["unmet_within_30d"] / df["screened_with_need"].replace(0, pd.NA) * 100
    ).round(2)

    return df.sort_values("service_gap_rate_pct", ascending=False)
```

**High-risk intersection — members with lapsed Medicaid coverage AND unmet HRSN:**
```python
def compute_renewal_hrsn_risk(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    df = con.execute("""
        SELECT
            m.county,
            m.race_ethnicity,
            COUNT(DISTINCT m.member_id)                                 AS total_members,
            SUM(CASE WHEN r.renewal_outcome = 'disenrolled'
                THEN 1 ELSE 0 END)                                      AS coverage_lapsed,
            SUM(CASE WHEN s.hrsn_flag = TRUE
                AND r.renewal_outcome = 'disenrolled'
                THEN 1 ELSE 0 END)                                      AS lapsed_with_unmet_hrsn
        FROM stg_members m
        LEFT JOIN int_renewal_hrsn r   USING (member_id)
        LEFT JOIN int_member_screenings s USING (member_id)
        GROUP BY m.county, m.race_ethnicity
    """).df()

    df["high_risk_rate_pct"] = (
        df["lapsed_with_unmet_hrsn"] / df["total_members"].replace(0, pd.NA) * 100
    ).round(2)

    return df.sort_values("high_risk_rate_pct", ascending=False)
```

**Python briefing export — Jinja2 HTML template rendered to PDF:**
```python
from jinja2 import Environment, FileSystemLoader
from weasyprint import HTML
import pandas as pd

def generate_briefing(scn_df: pd.DataFrame, gap_df: pd.DataFrame, risk_df: pd.DataFrame):
    env = Environment(loader=FileSystemLoader("src/outputs/briefing"))
    template = env.get_template("template.html")

    html_content = template.render(
        scn_performance=scn_df.to_dict(orient="records"),
        top_gap_counties=gap_df.head(10).to_dict(orient="records"),
        high_risk_members=risk_df.head(10).to_dict(orient="records"),
    )

    HTML(string=html_content).write_pdf("outputs/hero_scn_briefing.pdf")
    print("Briefing exported → outputs/hero_scn_briefing.pdf")
```

---

## Data Quality Checks

Automated Great Expectations suites run after each transformation layer. Pipeline halts on any suite failure.

| Check | Layer | Threshold |
|---|---|---|
| Null rate — `member_id` | Staging | 0% |
| Null rate — `scn_region` | Staging | 0% |
| Valid `primary_need_type` values | Staging | Housing, Nutrition, Transportation, Case Management only |
| Duplicate `screening_id` count | Intermediate | 0 |
| Referential integrity — screenings → members | Intermediate | 100% |
| SCN screening milestone flag completeness | Marts | 100% |
| Service gap rate anomaly (> 80% in any county) | Marts | Warning alert |

HTML data docs are auto-generated to `/docs/ge_reports/` on each pipeline run.

---

## Metrics Computed

**SCN Region Performance**

| Metric | Description |
|---|---|
| Screening rate by region | Share of eligible Medicaid members screened, vs. DY26 milestone target |
| HRSN prevalence by region | Share of screened members flagging at least one social need |
| Milestone status | On track / At risk / Behind — based on NYS DY26 25% screening target |
| Regional share of state volume | Each SCN's contribution to statewide screening totals |

**Service Gap Index**

| Metric | Description |
|---|---|
| 30-day service gap rate | Members with unmet HRSN who received no follow-up service within 30 days |
| Gap rate by need type | Broken out for housing, nutrition, transportation, case management |
| Gap rate by county | Geographic view for regional policy targeting |

**Renewal × HRSN Risk**

| Metric | Description |
|---|---|
| Coverage lapse rate | Share of members disenrolled during Medicaid unwind, by county |
| High-risk intersection rate | Members with both lapsed coverage and unmet HRSN — the population most likely to fall through the system |
| Stratification | By race/ethnicity and county for equity-focused reporting |

---

## Python Briefing Export

The `generate_briefing.py` script reads mart-layer DataFrames, renders them through a Jinja2 HTML template, and exports a formatted PDF policy summary — no dashboard login required. Designed for analysts to drop into stakeholder meetings or state reporting packages.

```python
# Run from project root
python src/outputs/briefing/generate_briefing.py

# Output: outputs/hero_scn_briefing.pdf
```

The Jinja2 template (`template.html`) renders three sections automatically: SCN milestone status table, top-10 service gap counties chart (Plotly static export), and the high-risk member intersection summary by race/ethnicity and county.

---

## Getting Started

**Clone and run with Docker:**
```bash
git clone https://github.com/sajanshergill/ny-hero-scn-performance-monitor.git
cd ny-hero-scn-performance-monitor

docker-compose up --build
```

**Run locally:**
```bash
pip install -r requirements.txt

# Step 1: Generate synthetic data
python src/generate/scn_screening_generator.py
python src/generate/service_delivery_generator.py
python src/generate/members_generator.py
python src/generate/renewal_outcomes_generator.py

# Step 2: Run transformations (staging → intermediate → marts)
python src/transforms/staging/stg_screenings.py
python src/transforms/intermediate/int_member_screenings.py
python src/transforms/marts/mart_scn_performance.py
python src/transforms/marts/mart_service_gap_index.py
python src/transforms/marts/mart_renewal_hrsn_risk.py

# Step 3: Run quality checks
python src/quality/run_quality_checks.py

# Step 4: Export stakeholder CSVs
python src/outputs/exports/export_stakeholder_csv.py

# Step 5: Generate PDF briefing
python src/outputs/briefing/generate_briefing.py

# Step 6: Launch dashboard
streamlit run src/dashboard/app.py
```

---

## CI/CD

GitHub Actions runs the full pipeline on every push to `main`:

1. Build Docker environment
2. Generate synthetic data (all four source tables)
3. Execute Python transformation layers — staging → intermediate → marts
4. Run Great Expectations quality suites — fail pipeline on any threshold breach
5. Run pytest unit tests across all transform layers
6. Export stakeholder CSVs
7. Generate PDF briefing via Jinja2 + WeasyPrint
8. Publish Great Expectations HTML data docs to GitHub Pages

---

## Documentation

- [`docs/data_dictionary.md`](docs/data_dictionary.md) — field-level definitions for all source tables, valid value enumerations, and SCN region codes mapped to NYS geography
- [`docs/metric_definitions.md`](docs/metric_definitions.md) — precise computation logic for all metrics, denominator choices, DY26 milestone target source, and edge case handling

---

## Relevance to UHF's HERO Work

This project is built directly around UHF's current operational mandate as HERO coordinator.

| UHF / HERO priority | How this project addresses it |
|---|---|
| Statewide SCN performance tracking | `mart_scn_performance.sql` computes DY26 milestone status for all 12 simulated regions |
| Identifying service gaps for high-need members | `mart_service_gap_index.sql` flags unmet HRSN with no 30-day service follow-up, by county and need type |
| Medicaid renewal unwind risk | `mart_renewal_hrsn_risk.sql` surfaces the high-risk intersection of coverage lapse and unmet HRSN |
| Policy-ready stakeholder reporting | `generate_briefing.py` renders a Jinja2 HTML → PDF summary with no dashboard dependency |
| Equity-first metric design | All mart outputs are stratified by race/ethnicity and county by default |
| Reproducible, auditable infrastructure | Modular Python modules, Git versioning, Great Expectations, and Docker ensure full reproducibility |

---

## Author

**Sajan Singh Shergill**  
MS Data Science, Pace University (May 2026)  
[linkedin.com/in/sajanshergill](https://linkedin.com/in/sajanshergill) · [sajansshergill.github.io](https://sajansshergill.github.io)
