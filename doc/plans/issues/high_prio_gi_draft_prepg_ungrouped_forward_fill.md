## Whole-frame `ffill` on three forcing paths carries values across station, and on one path ensemble-member, boundaries (PREPG-013)

**Status**: Draft (2026-08-18) — split out of PREPG-011, which conflated it with an identity defect
**Module**: `apps/preprocessing_gateway` (`Quantile_Mapping_OP.py`, `get_era5_reanalysis_data.py`)
**Priority**: **High for the control path** (*raised from Medium 2026-08-18 on measured
evidence*), Medium for ERA5, Low for the ensemble path.

The control path is the live one: its frames carry **127 stations (kyg)** / **35 (taj)** stacked
in long format, and the fill has fired on **15 distinct run dates between 2026-02-13 and
2026-08-17**. The ensemble path's frames are single-station, so its fill can only cross *member*
boundaries. See § Measured exposure.
**Labels**: `preprocessing_gateway`, `data-integrity`, `forcing`, `silent-corruption`
**Found**: 2026-08-18, out-of-loop review.
**Related**: PREPG-011 (ensemble station identity — a *prerequisite* for keying the ensemble fill
correctly, but a different defect). PREPG-010 (same path, transport).

---

## The pattern, at three sites

```python
if P_ensemble.isnull().values.any():
    print(f"Nan values in P data (ensemble) for HRU {code_ens}")
    print("Take Last Observation")
    P_ensemble = P_ensemble.ffill()      # whole frame — no grouping
```

| Site | Path | Boundaries crossed **in practice** | Stations per frame |
|---|---|---|---|
| `Quantile_Mapping_OP.py:757`, `:762` | **control member** | **station** | **127** (kyg), 35 (taj) |
| `get_era5_reanalysis_data.py:179` | ERA5 reanalysis | station | (not inventoried) |
| `Quantile_Mapping_OP.py:899`, `:904` | ensemble | ensemble **member** only | 1 — see below |

The ensemble row is narrower than the first draft claimed: every real ensemble file is
single-feature (§ Measured exposure), so that fill crosses member boundaries but **not** station
boundaries.

Rows are ordered by identity before the merge (`Quantile_Mapping_OP.py:225`), so a **leading**
gap in one group inherits the previous group's last value. Ensemble members are independent
realisations and stations are different catchments — this is fabrication, not interpolation.

## Phase-0 inventory — RESOLVED 2026-08-18

Measured on local dev copies of the `kyg`/`taj` data repos (`intermediate_data/data_gateway`),
holding the **2026-08-17 operational downloads**, plus the three deployment env files
(`~/Documents/GitHub/<org>_data_forecast_tools/config/`). Counts only; no station identifiers
reproduced here.

**Configured bundles / ensemble HRUs**

| Org | `HRU_CONTROL_MEMBER` | `HRU_ENSEMBLE` |
|---|---|---|
| kyg | **1** | 2 |
| taj | **1** | 1 |
| uzb | unset | unset |

**Real DG file shapes** (fields = date column + one column per feature)

| File class | kyg | taj |
|---|---|---|
| Ensemble `ECMWFIFS_*_ENS*_HRU*` | **2 fields — all 200 files** | none present |
| Control `Operational_HRU_*` | **128 fields (127 stations)** | **36 fields (35 stations)** |
| Snow `SnowOperational_HRU_*` | 65 and 443 fields | 19 and 158 fields |

**Two conclusions, and they point in opposite directions:**

1. **Every ensemble file is single-feature.** The multi-feature shape that the ensemble identity
   defect requires **does not occur on any current deployment**.
2. **The control files are strongly multi-station** — 127 and 35 stations respectively — and the
   control frame is *long* (`["date","P","code"]`, stations stacked), so an ungrouped `ffill`
   there crosses station boundaries.

### The frames are long, not wide — which is why the fill bleeds

This is the load-bearing detail. A *wide* frame (date × one column per station) would have
`ffill()` fill each station's column independently and be harmless. Both affected frames are
**long**, with stations stacked:

```python
P_data = transformed_data_file[["date", "P", "code"]].copy()   # :755 control — stacked
...
P_data = P_data.ffill()                                        # :757 — no grouping
```

So the fill runs down a column in which one station's rows are followed by the next station's.

### It fires in production

`apps/logs` across 15 distinct run dates (2026-02-13 → **2026-08-17**), 31 occurrences:

| Log line | Path | Count |
|---|---|---|
| `Nan values in T data for HRU …` | **control** | 18 |
| `Nan values in P data for HRU …` | **control** | 5 |
| `Nan values in P data (ensemble) …` | ensemble | 8 |

**23 of 31 firings are on the multi-station control frame.**

### What this evidence does *not* establish

The log line proves the **fill ran**, not that a value crossed a station boundary on that run.
Bleeding requires a gap at a station block's *leading* edge; an interior gap within one station
fills correctly and harmlessly. Quantifying actual cross-station contamination needs the frames
themselves, not the logs.

That does not weaken the case for fixing it — an ungrouped fill over a 127-station stacked frame
has no correct interpretation — but the issue should not claim confirmed corruption it has not
measured. **A cheap way to close this gap:** log the NaN row positions per `code` at the fill
site before fixing, and re-read after one operational cycle.

## Why the ensemble path cannot be fixed alone

The obvious fix, `groupby(["code", "ensemble_member"])`, **does not work on the ensemble path
today**: `code` is a file-level constant there (see **PREPG-011**), so that grouping still spans
stations *in principle*. In practice ensemble files are single-feature, so grouping by
`ensemble_member` alone is currently sufficient and correct there — but it would silently become
wrong the moment PREPG-011 is armed by a multi-feature shapefile.

**Therefore: fix control + ERA5 now; group the ensemble site by member now, but leave a comment
and a test tying it to PREPG-011** so the dependency is not lost.

The control and ERA5 sites have no such problem — `code` is already the real station
(`dg_utils.py:198`) — so they can be fixed independently and immediately. **Do these first.**

## The policy question this exposes

`ffill` is currently unbounded: a single trailing observation can propagate across an arbitrary
number of days. Before implementing, decide and document:

- **Leading** gap in a group — nothing to carry forward. Fail, or leave NaN?
- **Interior** gap — fill, and up to what maximum run length?
- **Trailing** gap — fill, or truncate the series?
- **Over-limit** gap — fail loudly, or emit NaN and let the consumer decide?

Note the consumer constraint: dropping NaN rows wholesale shortens some members relative to
others and changes ensemble geometry for the conceptual model.

## Acceptance criteria

- **Control and ERA5** fills are grouped by `code`. Proven by a fixture with **two stations
  stacked in one frame**, where station 2 begins with a gap and must **not** inherit station 1's
  last value.
- **Ensemble** fill is grouped by `ensemble_member` (sufficient while files are single-feature),
  with a comment and test recording that it must become station+member if **PREPG-011** is fixed
  or armed.
- No value crosses a station boundary on any of the three paths; no value crosses a member
  boundary on the ensemble path.
- Leading, interior, trailing and over-limit gaps are tested **independently** — one combined
  fixture will not distinguish them.
- Whatever policy is chosen is logged at **WARNING**, naming variable, station, member, date range
  and gap length. The current `Take Last Observation` print is indistinguishable from routine
  output.
- Assertions are **semantic and ordering-insensitive**, not "byte-identical" — adding a grouped
  sort can change bytes on a run with no NaNs at all.
- `SAPPHIRE_TEST_ENV=True bash run_tests.sh preprocessing_gateway` green.

## Contract not to break

- Do **not** drop NaN rows wholesale (see the consumer constraint above).
- The three paths have **different keys**. Do not apply one grouping blindly to all of them —
  control and ERA5 have no member dimension.
- The replacement must stay **loud**. This is invisible today precisely because its log line looks
  benign.
