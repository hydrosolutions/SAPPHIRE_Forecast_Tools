# Forecast Skill Evaluation

Read-only forecast-skill analysis for SAPPHIRE low-flow forecasts. The CLI builds
forecast/observed pairs across configured horizons, computes contingency metrics,
builds baseline comparisons, and writes tidy run artifacts.

## Run the CLI

From the repository root:

```bash
cd apps/forecast_skill_eval
SAPPHIRE_TEST_ENV=True uv run python -m forecast_skill_eval.cli \
  --threshold 0.80 \
  --horizons day pentad decade month quarter season \
  --stations 19999 \
  --models model-a LR LR_Base \
  --start-date 2020-01-01 \
  --end-date 2024-12-31 \
  --output-dir artifacts \
  --min-years 10 \
  --operational-start 2024-01-01
```

Optional arguments:

- `--run-id fixed-name` sets the artifact directory name. Without it, the CLI
  uses the current timestamp for file naming only.
- `--provenance horizon=source` overrides the norm provenance mapping. Repeat
  the flag for multiple horizons, for example `--provenance decade=official`.
- `--models` and `--stations` accept either space-separated values or comma
  separated values.

If `sapphire-api-client` is not importable, the CLI prints a skip message and
does not construct API clients.

## Artifacts

Each run writes to:

```text
<output_dir>/<run_id>/
```

Files:

- `pairs.csv` and, when a parquet engine is available, `pairs.parquet`: one row
  per scored forecast/observed pair with forecast value, observed value, norm,
  norm provenance, forecast regime, classes, and contingency cell.
- `contingency_metrics.csv` and optional `.parquet`: tidy station and pooled
  contingency counts plus POD, FAR, POFD, CSI, frequency bias, HSS, PSS, and
  Wilson intervals. Rows are emitted for `regime=all` plus each scored regime.
- `baselines.csv` and optional `.parquet`: climatology and operational proxy
  baseline rows on matched samples where available, also tagged by regime.
- `exclusion_ledger.csv` and optional `.parquet`: excluded candidates by stage,
  reason, station, period key, and year.
- `run_config.json`: the captured-once resolved parameters for threshold,
  horizons, filters, date range, output directory, provenance mapping,
  `min_years`, and `operational_start`.
- `summary.md`: per-horizon coverage and skipped horizons, ledger totals by
  `(stage, reason)`, regime source, headline pooled base rate/POD/FAR/FN/HSS/PSS
  per model/regime/provenance with undefined flags, per-station POD
  min/median/max for each pooled line, and norm provenance breakdown.

## Local Database Caveats

- Pentad and decade norms use calculated norms in local runs unless the
  configured provenance mapping points elsewhere.
- Month, quarter, and season observed truth is derived from daily runoff.
- Rolling-window long-term forecasts are excluded; only calendar-aligned
  month, quarter, and season forecasts are scored.
- Forecast `flag=2` rows are excluded as forecast error flags. If flags do not
  meaningfully separate operational and hindcast rows for a horizon, regime is
  derived from the issue date and `operational_start`.
- `DAY` is exploratory and can be thin, so interpret day-horizon coverage and
  pooled metrics cautiously.
