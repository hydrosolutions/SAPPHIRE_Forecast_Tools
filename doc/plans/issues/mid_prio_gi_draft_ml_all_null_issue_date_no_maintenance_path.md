## A whole-station-null ML issue date is invisible to both maintenance paths (ML-020)

**Status**: Draft (2026-08-18) — **investigation**, cause not yet established
**Module**: `apps/machine_learning` (`recalculate_nan_forecasts.py`, `fill_ml_gaps.py`);
observed data in the postprocessing `forecasts` DAY archive
**Priority**: **Medium** (owner to confirm) — no maintenance path can reach the condition, and
a starved DAY archive propagates into period aggregation. Rating is provisional because the
cause is unknown and the observation is from one local environment.
**Labels**: `machine_learning`, `data-quality`, `maintenance-coverage`
**Found**: 2026-08-18, local kghm (kyg) end-to-end review on `maxat_sapphire_2` @ `a304ffb0`.
**Related**: **ML-019** (the recalc reports success regardless), **ML-015** (TFT-only all-NaN
split — *not* this: the pattern here is model-independent), **ML-017** (a missing ERA5 day
cascades NaNs to *all* stations — also not this, see below), **PP-045**/**PP-060** (period
aggregation consumes this archive).

---

## Observation

DAY forecasts on the local kyg development database, read after the full 2026-08-18 pipeline
run (all three models — TFT, TiDE, TSMixer — carry the identical pattern):

| Issue date | Stations | Rows per station | Fully-null stations | Complete stations |
|---|---|---|---|---|
| 2026-08-16 | 71 | **6 for 65 of them**, 11 for 6 | **50** | 21 |
| 2026-08-18 | 55 | 11 | 3 | 52 |

> **Row-count correction, made before filing.** An earlier draft read "6 rows instead of 11" as a
> **truncated horizon**. It is not. `make_forecast.py:532-535` sets `forecast_horizon = 6` for
> `PENTAD` and `11` for `DECAD`, and that value is passed straight to the predictor (`:795`,
> `:803`) — so **6 and 11 are simply the two modes' horizons**. The row-count difference between
> 08-16 and 08-18 says the two days were produced in different prediction modes; it is not
> evidence of degradation. The claim is withdrawn. What follows is what survives it.

The surviving anomaly is the **nulls**: 50 of 71 stations on 2026-08-16 are null across their
entire horizon. On both dates every station is either fully populated or fully null — **zero
partial stations**. That is the signature of a per-station input failure, not of a model degrading
at longer lead times, and it rules out a lead-dependent explanation.

The station count also differs between the two dates (71 vs 55), consistent with the mode
difference but not separately checked. Recorded as an observation, not explained.

**What it is not.** The null set is byte-identical across TFT, TiDE and TSMixer, so this is not
ML-015's TFT-only split. And meteo continuity was verified the same day — 79 consecutive days
of T and P with zero interior gaps and zero all-null days — so ML-017's ERA5-gap cascade is not
the mechanism either. The cause is **not established**; it needs the 08-16 run's own logs,
which is where INFRA-029 bites (the postprocessing side of that day's record is WARNING-only).

## Why no maintenance path reaches it

This is the part that is provable from code, independent of the cause.

**`recalculate_nan_forecasts.py` never looks at 08-16.** Its window is derived from the rows it
finds flagged as recalculable:

```python
nan_values = forecast_code[forecast_code["flag"].isin([1, 2])]     # :283
min_missing_date = nan_values["forecast_date"].min()               # :286
max_missing_date = nan_values["forecast_date"].max()               # :287
```

On the observed run that produced `Min missing date: 2026-08-17, Max missing date: 2026-08-18`.
The 08-16 rows are outside that window — they exist, so the only explanation left is that they
carry a flag other than `1`/`2`. **That is an inference, not a measurement**: the flag distribution
on 08-16 was never read (see the investigation list). Either way, the hindcast was not asked for
that date.

**`fill_ml_gaps.py` cannot see it either.** It detects only *interior* gaps between consecutive
issue dates:

```python
limit_day_gap = 1                                                   # :198
for i in range(1, len(forecast_dates)):
    if (forecast_dates[i] - forecast_dates[i - 1]).days > limit_day_gap:   # :278-279
```

2026-08-16 **is present**, so no date-to-date gap exists. A date that is present but all-null is
not a gap by this definition, and the module correctly reports nothing to fill.

So the two maintenance tools between them cover "NaN rows flagged for recalculation" and "issue
dates missing entirely", and neither covers "issue date present, rows present, values all null" —
the condition actually observed. This is the durable part of this issue and it holds independently
of the withdrawn row-count reading.

## Investigation before implementation

This issue should not be implemented from its current evidence. Establish first:

- [ ] ~~Is 11 rows per station the DAY invariant?~~ — **answered before filing**: 6 is PENTAD's
      horizon and 11 is DECAD's (`make_forecast.py:532-535`). Remaining sub-question: confirm which
      mode ran on 08-16, since that also explains the 71-vs-55 station count.
- [ ] **What do the 08-16 rows' `flag` values look like?** This is now the first real unknown — That decides whether the recalc
      *could* have reached them with a wider window, or whether they are unflagged.
- [ ] **How often does this shape occur?** One date on one local environment is not a rate.
      Query a longer window on a deployed environment before rating this.
- [ ] **What does 08-16's ML run log say?** Blocked in practice by INFRA-029 for the
      postprocessing side; the ML modules' own logs are readable.
- [ ] **Did the 18 rows written on 08-17 help?** No before-snapshot was taken for that date, so
      the effect is unknown — do not assume either way.

## Candidate directions (do not choose before the investigation)

- **(a) Detection only** — a maintenance-time check that reports issue dates whose row count or
  non-null fraction falls below the expected shape. Report, do not write; matches the
  detect-and-report direction taken for PP-045.
- **(b) Widen the recalc window** to a lookback rather than deriving it from flagged rows.
  Cheap, but it hindcasts dates that may be fine, and the cost is a hindcast run per date.
- **(c) Teach `fill_ml_gaps` a "degraded date" notion** — a date present but with an all-null or
  mostly-null value column. Closest to the real gap, and the largest change. Note the expected
  **row** count is now known to be mode-dependent (6 vs 11), so any such check must key on the
  null fraction, not on a fixed row count — an earlier reading of this issue would have built
  exactly the wrong test.

## Out of scope

- The reporting defect in `recalculate_nan_forecasts` — that is **ML-019**.
- Period aggregation's handling of missing DAY inputs — PP-045 and PP-060.
- Any change to the operational forecast path.

## Acceptance criteria (for the investigation)

- [ ] The `flag` distribution on an all-null date is recorded.
- [ ] The mode that produced 2026-08-16 is identified, closing the 71-vs-55 station-count question.
- [ ] The frequency of the shape on a deployed environment is measured over ≥ 90 days.
- [ ] A direction is chosen, with the owner's rating, before any code is written.
