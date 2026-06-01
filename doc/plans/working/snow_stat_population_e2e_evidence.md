# Snow Stat Population — Phase 3 End-to-End Evidence

**Date produced:** 2026-06-01
**Local stack:** http://localhost:8000
**Branch:** develop_dashboard_snow_display
**Upstream commits:** P0 `97c889c`, P1 `a294540`, P2 `b1582c7`, RoF-fix `90a574f`, JSON-safe fix `2793b62`

---

## 1. Stack Health Check

Python `urllib.request` GET on `/health` returned HTTP `200`.

Response prefix:

```json
{"status":"healthy","timestamp":"2026-06-01T16:16:51.199657","services":{"preprocessing":"http://preprocessing-api:8002", ...}}
```

Note: sandboxed localhost HTTP was blocked with `Operation not permitted`, so the health check and API verification were run with explicit localhost access.

## 2. Recalc Invocation

Script inspection:
- `recalculate_snow_norms.py` exposes no CLI arguments for `--year`, `--variables`, or `--snow-type`.
- Target year defaults to `date.today().year`, so this run targeted `2026`.
- Variables are read from `ieasyhydroforecast_SNOW_VARS`; the loaded env produced `['SWE', 'HS', 'ROF']`.
- The script cannot scope to a single snow type from the command line.

Exact command, run from `apps/preprocessing_gateway/`:

```bash
SECONDS=0
ieasyhydroforecast_env_file_path="$HOME/Documents/GitHub/kyg_data_forecast_tools/config/.env_bea_kghm" uv run python recalculate_snow_norms.py 2>&1 | tee /tmp/snow_stat_recalc_p3.log
cmd_status=${pipestatus[1]}
printf 'Wall-clock duration: %s seconds\n' "$SECONDS"
exit "$cmd_status"
```

- **Target year:** `2026`
- **Wall-clock duration:** `954 seconds` (about 15.9 minutes)
- **Exit code:** `0`
- **Write summary:** 240 `Wrote N stat+norm records` log lines, 0 `Failed to write` lines, 0 `ERROR` lines.

Per-variable summary lines:

```text
2026-06-01 18:29:13,462 - INFO - Computed stats for SWE: 21651 DOY rows, 19242 with populated stats, 2409 with NaN stats
2026-06-01 18:29:53,604 - INFO - Computed stats for HS: 21583 DOY rows, 20478 with populated stats, 1105 with NaN stats
2026-06-01 18:30:33,737 - INFO - Computed stats for ROF: 21239 DOY rows, 19894 with populated stats, 1345 with NaN stats
```

Log tail, station codes redacted:

```text
2026-06-01 18:42:41,925 - INFO - Wrote 365 stat+norm records for ROF/<redacted> (year 2026)
2026-06-01 18:42:42,035 - INFO - Posting batch 1/1 (365 records)
2026-06-01 18:42:42,075 - INFO - Successfully posted 365 records to /snow/
2026-06-01 18:42:42,075 - INFO - Wrote 365 stat+norm records for ROF/<redacted> (year 2026)
2026-06-01 18:42:42,164 - INFO - Posting batch 1/1 (365 records)
2026-06-01 18:42:42,204 - INFO - Successfully posted 365 records to /snow/
2026-06-01 18:42:42,204 - INFO - Wrote 365 stat+norm records for ROF/<redacted> (year 2026)
2026-06-01 18:42:42,310 - INFO - Posting batch 1/1 (365 records)
2026-06-01 18:42:42,349 - INFO - Successfully posted 365 records to /snow/
2026-06-01 18:42:42,349 - INFO - Wrote 365 stat+norm records for ROF/<redacted> (year 2026)
2026-06-01 18:42:42,437 - INFO - Posting batch 1/1 (365 records)
2026-06-01 18:42:42,475 - INFO - Successfully posted 365 records to /snow/
2026-06-01 18:42:42,476 - INFO - Wrote 365 stat+norm records for ROF/<redacted> (year 2026)
2026-06-01 18:42:42,569 - INFO - Posting batch 1/1 (365 records)
2026-06-01 18:42:42,609 - INFO - Successfully posted 365 records to /snow/
2026-06-01 18:42:42,609 - INFO - Wrote 365 stat+norm records for ROF/<redacted> (year 2026)
2026-06-01 18:42:42,611 - INFO - Snow norm recalculation completed successfully
Snow norm recalculation completed successfully
```

NaN-stats rows are expected for code/DOY combinations with fewer than `n_years_min = 5` years of history. Those rows correctly remain null for climatology band fields.

## 3. Per-Snow-Type Non-Null Counts

Query: `GET /api/preprocessing/snow/?snow_type={X}&start_date=2025-01-01&end_date=2026-12-31&limit=100000`.

| Snow type | Rows  | mean  | min   | max   | q05   | q25   | q50   | q75   | q95   | previous | current | count | std   |
|-----------|-------|-------|-------|-------|-------|-------|-------|-------|-------|----------|---------|-------|-------|
| HS        | 48352 | 20440 | 20440 | 20440 | 20440 | 20440 | 20440 | 20440 | 20440 | 19152    | 8801    | 21527 | 20440 |
| ROF       | 51061 | 19855 | 19855 | 19855 | 19855 | 19855 | 19855 | 19855 | 19855 | 21861    | 8801    | 21183 | 19855 |
| SWE       | 51061 | 19208 | 19208 | 19208 | 19208 | 19208 | 19208 | 19208 | 19208 | 21861    | 8801    | 21595 | 19208 |

All ten dashboard stat fields have non-zero non-null counts for every snow type. `current` is lower because the 2026 current-year window only extends through 2026-06-01; future 2026 dates have no observed current values yet.

Fully-populated rows:

| Snow type | Codes with at least one fully-populated row | Fully-populated rows |
|-----------|---------------------------------------------|----------------------|
| HS        | 56                                          | 7672                 |
| ROF       | 56                                          | 7543                 |
| SWE       | 53                                          | 7261                 |

“Fully populated” means all ten dashboard fields `mean`, `min`, `max`, `q05`, `q25`, `q50`, `q75`, `q95`, `previous`, and `current` are non-null on the same row.

## 4. Example Fully-Populated Station

A station with all ten dashboard stat fields non-null on at least one date:

- **Snow type:** `HS`
- **Code:** `<redacted>`
- **Example date:** `2026-02-08`
- **Values:**
  - `mean = 0.20933333333333334`
  - `min = 0.15`
  - `max = 0.35`
  - `q05 = 0.15`
  - `q25 = 0.17`
  - `q50 = 0.21`
  - `q75 = 0.23`
  - `q95 = 0.27299999999999985`
  - `previous = 0.15`
  - `current = 0.15`

## 5. DECISION

**READY: YES**

Justification:
- Recalc exited with status `0`.
- For HS, `mean`, `min`, `max`, `q05`, `q95`, and `current` all have non-null counts greater than `0`.
- At least one `(snow_type, code)` example row has all ten dashboard fields non-null on at least one date; HS has 56 such codes and 7672 such rows.

## 6. Implications For The Dashboard Plan

The upstream dashboard plan (`develop_dashboard_snow_display`) can treat the write-side snow-stat population gate as satisfied after this work merges. The `/snow/` API now returns non-null dashboard stat fields for HS, ROF, and SWE, with a concrete fully-populated HS example.

Rows that still have null climatology stats are expected where historical coverage is below the five-year threshold. The dashboard should represent those as insufficient history, not as a write-side failure.
