# INFRA-015: Audit pentad/decade boundary date convention across modules

**Status**: Complete — all actions resolved
**Module**: infra (cross-module)
**Priority**: Mid
**Labels**: `audit`, `date-handling`, `cross-module`

---

## Summary

Verify that the pentad/decade boundary date convention is consistent across all
modules: forecasts are **triggered on the last day of the outgoing period**, and
the period label stored refers to the **upcoming period** (the one being forecast).

## Context

The operational convention is:

| Trigger day (issue date)  | Pentad being forecast | Decade being forecast |
|---------------------------|----------------------|----------------------|
| Last day of month         | 1st (next month)     | 1st (next month)     |
| 5th                       | 2nd                  | —                    |
| 10th                      | 3rd                  | 2nd                  |
| 15th                      | 4th                  | —                    |
| 20th                      | 5th                  | 3rd                  |
| 25th                      | 6th                  | —                    |

The trigger day is the **last day of the previous pentad/decade**, one day before
the start of the period being forecast. The forecast covers the *next* period.

## Findings

### 1. Trigger day lists — CONSISTENT everywhere

All modules use identical boundary day sets:

- **Pentad**: `{5, 10, 15, 20, 25, last_day_of_month}`
- **Decade**: `{10, 20, last_day_of_month}`

Locations verified:
- `iEasyHydroForecast/forecast_library.py:594-678` — `add_pentad_issue_date()`, `add_decad_issue_date()`
- `iEasyHydroForecast/setup_library.py:4655-4677` — `ForecastFlags.from_forecast_date_get_flags()`
- `linear_regression/linear_regression.py:360-414` — `get_forecast_days_for_month()`
- `postprocessing_forecasts/postprocessing_operational.py:54-63` — `is_pentad_boundary()`, `is_decad_boundary()`
- `machine_learning/reaggregate_day_to_periods.py:104-111` — `is_pentad_boundary()`, `is_decad_boundary()`
- `validate_pipeline/validate_pipeline.py:103-136` — `is_pentad_forecast_day()`, `is_decad_forecast_day()`
- `machine_learning/scr/utils_ml_forecast.py:430-465` — `save_pentad_forecast()`, `save_decadal_forecast()` (dead code, never called)

### 2. Period labeling — Two-layer convention (CORRECT but subtle)

The `tag_library.py` functions use `(day - 1) // 5 + 1` which assigns day 5 to
pentad 1 (the *closing* period). Downstream code in postprocessing and
setup_library applies a `+1 day` offset before calling these functions:

```python
# postprocessing_forecasts/src/data_reader.py:1748-1753
offset_dates = forecasts["date"] + pd.Timedelta(days=1)
forecasts[period_in_month_col] = offset_dates.apply(get_period)
forecasts[period_col] = offset_dates.apply(get_period_in_year)
```

This correctly converts:
- `date=Jan 5` → `date+1=Jan 6` → `get_pentad(Jan 6) = 2` (upcoming period)
- `date=Jan 31` → `date+1=Feb 1` → `get_pentad(Feb 1) = 1` (1st pentad of Feb)

The +1 shift appears consistently in:
- `data_reader.py:1748-1753` (`_normalize_lr_forecasts`)
- `data_reader.py:1883-1885` (`_normalize_ml_forecasts`)
- `setup_library.py` at ~10 locations (lines 1879, 2225, 2296, 2398, 2469, 2649, 2755, 2962, 3092)
- `forecast_library.py:4484-4487`

### 3. `run_locally.sh` — No day-of-month gating in the shell

The `daily` target runs PENTAD and DECAD modes **every day** (Phase 3 loops
`for mode in PENTAD DECAD`). Day-of-month gating happens inside Python:
`ForecastFlags.from_forecast_date_get_flags()` checks if `today.day` is in the
boundary list. The cron schedule (`0 4 * * *`) also fires every day.

Long-term forecasting uses a separate mechanism: `is_lt_issue_window()` checks
±5 days from configurable issue days (default: 10, 25).

### 4. Potential concerns (non-blocking)

| Item | Severity | Detail |
|------|----------|--------|
| `linear_regression.py:745,846` — `forecast_pentad_of_year = get_pentad_in_year(current_day)` | **HIGH — tracked as LR-008** | Uses raw convention WITHOUT the +1 day shift. This value flows directly into the API write via `forecast_library.py:3274-3278` as `horizon_in_year`. On boundary days, LR tags to the wrong pentad. Fix: `current_day + timedelta(days=1)`. |
| ML `calculate_pentad_from_date()` uses raw convention | **None** — dead code | `save_pentad_forecast()` / `save_decadal_forecast()` are never called. |
| ML `make_forecast.py` uses `datetime.now()` inline | **Low** — tracked in INFRA-004 | Not a boundary shift issue, but violates Forecast Date Rule. |
| `reset_forecast_run_date/rerun_forecast.py` uses days 9/19 | **None** — intentional | Deliberately sets rerun file date to day before boundary so the pipeline re-triggers. Legacy code. |

### 5. `forecast_library.get_issue_date_from_pentad()` — inverse mapping

Lines 4308-4349: given pentad_in_year, returns the issue (trigger) date.
Confirms the convention:
- pentad 1 → last day of previous month
- pentad 2 → day 5
- pentad 3 → day 10
- pentad 4 → day 15
- pentad 5 → day 20
- pentad 6 → day 25

This is consistent with the forward mapping.

---

## Conclusion

**One production bug found — tracked as LR-008.** The boundary dates are
consistent across all modules EXCEPT the linear regression pipeline, where
`linear_regression.py` lines 745 and 846 pass `current_day` to
`get_pentad_in_year` / `get_decad_in_year` instead of `current_day + 1 day`.
This causes LR forecasts to be tagged to the wrong pentad/decad on every
boundary day, preventing EM/NE combined forecast creation. See
`high_prio_gi_draft_lr_pentad_horizon_offset.md` (LR-008) for the fix plan.

The two-layer convention (raw `get_pentad` + downstream `+1 day` shift) is
otherwise deliberate and correct. The documentation gap remains — this
convention should be written down.

---

## Recommended Actions

1. ~~**Document the convention**~~ — **DONE.** Added "Boundary Dates and the
   +1 Day Shift" subsection to `doc/data_flow_short_term.md`.
2. ~~**Verify `forecast_horizon_int` in LR API writes**~~ — **RESOLVED:
   confirmed broken, tracked as LR-008.** The value stored uses
   `get_pentad_in_year(current_day)` without the +1 shift. LR-008 fixes this.
3. ~~**Remove dead code**~~ — **DONE.** Removed `save_pentad_forecast()` and
   `save_decadal_forecast()` from `utils_ml_forecast.py` (zero callers).

---

## References

- INFRA-004: Forecast Date Rule (`high_prio_gi_draft_infra_forecast_date_rule.md`)
- `doc/data_flow_short_term.md` — pipeline data flow
- CLAUDE.md "The Forecast Date Rule" section
- LR-008: LR pentad horizon offset (`high_prio_gi_draft_lr_pentad_horizon_offset.md`)

---

*Created: 2026-03-25 — Initial investigation complete.*
*Updated: 2026-03-25 — LR-008 found; downgraded to mid priority; conclusion corrected.*
*Updated: 2026-03-27 — Actions 1 and 3 implemented. Issue complete.*
