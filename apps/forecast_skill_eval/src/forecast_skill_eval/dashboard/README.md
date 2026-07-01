# Forecast Skill Evaluation — Station Explorer Dashboard

A local, read-only Streamlit dashboard for exploring contingency-based skill
metrics by station.

## What it does

Reads a `contingency_metrics.csv` produced by `forecast_skill_eval` and lets
you filter by horizon, event type, season, regime, norm provenance, lead, and
model. The main panel shows:

- a per-station metrics table (POD, FAR, HSS, PSS, CI bounds)
- a ranked bar chart for any metric, with the POOLED value as a reference line
- side-by-side model comparison charts when multiple models are selected

The dashboard is **read-only**: it never writes files, never exports data.

## Run

From the repository root:

```
uv run --project apps/forecast_skill_eval --with streamlit streamlit run apps/forecast_skill_eval/src/forecast_skill_eval/dashboard/app.py
```

`streamlit` is a dev-only tool (not a pinned runtime dep), so it is supplied
ad hoc via `--with`.

To point at a different CSV, set the environment variable before the command:

```
SKILL_EVAL_METRICS_CSV=/path/to/contingency_metrics.csv \
    uv run --project apps/forecast_skill_eval --with streamlit \
    streamlit run apps/forecast_skill_eval/src/forecast_skill_eval/dashboard/app.py
```
