# Forecast Dashboard Dev Tools

Developer utilities for inspecting forecast data and skill metrics from the
SAPPHIRE API. These scripts are **not** part of the operational pipeline — they
are for ad-hoc exploration, debugging, and quality checks.

## Prerequisites

All scripts assume the SAPPHIRE services are running locally
(`http://localhost:8000/api`). Run from the `apps/forecast_dashboard` directory
so that `uv` picks up the correct `pyproject.toml`.

## Scripts

### `fetch_data.py` — shared API client

Lightweight data-fetching layer used by all other scripts. Provides:

- `fetch_forecasts()` — ML/ensemble forecasts from the postprocessing API
- `fetch_lr_forecasts()` — linear regression forecasts
- `fetch_skill_metrics()` — skill metrics (s/σ, accuracy, MAE, NSE)
- `insert_gap_nans()` — helper to break matplotlib lines across data gaps

Not intended to be run directly.

### `inspect_forecasts.py` — CLI forecast plots

Produces matplotlib plots of forecast time series and model comparisons.

```bash
cd apps/forecast_dashboard

# Save plots to a directory
uv run python dev_code/inspect_forecasts.py \
    --station 15102 --horizon pentad \
    --start-date 2025-06-01 --end-date 2026-03-01 \
    --models LR TFT NE \
    --output-dir /tmp/forecast_plots/

# Interactive display
uv run python dev_code/inspect_forecasts.py \
    --station 15102 --horizon pentad --show
```

### `inspect_skill.py` — CLI skill metric plots

Line plots of skill metrics over pentad/decad periods with threshold reference
lines.

```bash
cd apps/forecast_dashboard

uv run python dev_code/inspect_skill.py \
    --station 15102 --horizon pentad \
    --metric sdivsigma accuracy --show
```

### `explore_forecasts.py` — interactive forecast notebook (marimo)

Interactive browser-based exploration of forecast data with model filtering and
quantile bands.

```bash
cd apps/forecast_dashboard
uv run marimo edit dev_code/explore_forecasts.py
```

### `explore_skill.py` — interactive skill metrics notebook (marimo)

Interactive exploration of skill metrics with selectable metrics, heatmaps, and
raw data tables.

```bash
cd apps/forecast_dashboard
uv run marimo edit dev_code/explore_skill.py
```
