# Enpal dbt Assessment

[![Docs Build](https://github.com/orustra-eng/dbt-enpal-submission-clean/actions/workflows/dbt-docs.yml/badge.svg?branch=main)](https://github.com/orustra-eng/dbt-enpal-submission-clean/actions/workflows/dbt-docs.yml)
[![dbt Docs (GitHub Pages)](https://img.shields.io/badge/dbt%20Docs-View%20site-blue)](https://orustra-eng.github.io/dbt-enpal-submission-clean/)

**Docs:** https://orustra-eng.github.io/dbt-enpal-submission-clean/  
**Workflow:** https://github.com/orustra-eng/dbt-enpal-submission-clean/actions/workflows/dbt-docs.yml


## What’s inside
- Staging models (`models/staging`)
- Intermediate models (`models/intermediate`)
- Reporting model (`models/reporting/rep_sales_funnel_monthly.sql`)
- Seeds (`seeds/`): `map_stage_to_funnel.csv`, `map_activity_to_funnel.csv`, etc.
- Linting config: `.sqlfluff`

## Prereqs
- Python 3.11+
- dbt-core + dbt-postgres
- (Optional) sqlfluff

## Setup
```bash
python -m venv .venv
. .venv/Scripts/Activate.ps1  # Windows PowerShell
pip install -r requirements.txt  # or: pip install dbt-postgres sqlfluff


