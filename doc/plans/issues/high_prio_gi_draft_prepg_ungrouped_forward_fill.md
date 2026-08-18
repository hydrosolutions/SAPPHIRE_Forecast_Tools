## Whole-frame `ffill` on three forcing paths carries values across station, and on one path ensemble-member, boundaries (PREPG-013)

**Status**: Draft (2026-08-18) — split out of PREPG-011; **scope-cut after out-of-loop review**
**Module**: `apps/preprocessing_gateway` (`Quantile_Mapping_OP.py`, `get_era5_reanalysis_data.py`)
**Priority**: **High for the control path** (*raised from Medium 2026-08-18 on measured
evidence*), Medium for ERA5, Low for the ensemble path.

The control path is the live one: its frames carry **127 stations (kyg)** / **35 (taj)** stacked
in long format, and the fill has fired on **15 distinct run dates between 2026-02-13 and
2026-08-17**. ERA5 is the same code again and shares the defect structurally, though it has not
been observed firing. The ensemble path ranks lowest because deployment files are single-feature
— an **externally measured** fact, not one this repository can confirm. See § Measured exposure.
**Labels**: `preprocessing_gateway`, `data-integrity`, `forcing`, `silent-corruption`
**Found**: 2026-08-18, out-of-loop review.
**Related**: PREPG-011 (ensemble station identity — only relevant if a multi-feature ensemble
file ever appears; not a blocker for this fix). PREPG-010 (same module, transport).

---

## The pattern, at three sites

```python
if P_ensemble.isnull().values.any():
    print(f"Nan values in P data (ensemble) for HRU {code_ens}")
    print("Take Last Observation")
    P_ensemble = P_ensemble.ffill()      # whole frame — no grouping
```

| Fill site | Path | Frame shape | Boundaries crossed **in practice** | Stations/frame |
|---|---|---|---|---|
| `Quantile_Mapping_OP.py:763`, `:768` | **control member** | long (`:754-755`) | **station** | **127** (kyg), 35 (taj) |
| `get_era5_reanalysis_data.py:183`, `:188` | ERA5 reanalysis | long (`:176-177`) | **station** | not inventoried |
| `Quantile_Mapping_OP.py:902`, `:907` | ensemble | long (`:895`) | ensemble **member** only | 1 — see below |

**The ERA5 site is character-identical to the control site** — same projection, same ungrouped
fill, same log string. Verified 2026-08-18; an earlier revision asserted its frame shape without
checking. Fix them together; they are the same code twice.

Note the cited frame lines are the `else:` (no quantile mapping) branch. When `perform_qmapping`
is true the frame comes from `dg_utils.do_quantile_mapping()` instead — **also long** — so the
fill is ungrouped on both branches.

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
| `Nan values in T data for HRU …` | control | 18 |
| `Nan values in P data for HRU …` | control | 5 |
| `Nan values in P data (ensemble) …` | ensemble | 8 |

**23 of 31 firings are on the multi-station control frame.**

> **How that attribution was established — the log string alone does not support it.**
> `Quantile_Mapping_OP.py:761/:766` and `get_era5_reanalysis_data.py:181/:186` print the **same
> string**, both interpolating `c_m_hru`, so "Nan values in P data for HRU …" is ambiguous between
> the control and ERA5 paths. The attribution above comes from the **enclosing script section** in
> each `run_locally` log, which resolves all 31 to `Quantile_Mapping_OP.py` and **zero to ERA5**.
>
> Two consequences: the ERA5 fill has **not been observed firing** (its exposure is structural, not
> demonstrated), and **the log lines should be made distinguishable** as part of this fix —
> otherwise the next person doing this analysis has to redo the section attribution.

### What this evidence does *not* establish

The log line proves the **fill ran**, not that a value crossed a station boundary on that run.
Bleeding requires a gap at a station block's *leading* edge; an interior gap within one station
fills correctly and harmlessly. Quantifying actual cross-station contamination needs the frames
themselves, not the logs.

That does not weaken the case for fixing it — an ungrouped fill over a 127-station stacked frame
has no correct interpretation — but the issue should not claim confirmed corruption it has not
measured. Quantifying it exactly is **not** a precondition for the fix (see § Deliberately out of
scope).

## Why the ensemble path cannot be fixed alone

Grouping the ensemble fill by `ensemble_member` is sufficient **only under the single-feature
assumption**, and that assumption is **not verifiable from this repository** — it rests on an
external inventory of deployment data (§ Phase-0 inventory), and the transform explicitly accepts
every feature column. So:

- Group the ensemble fill by `ensemble_member`, and **test member isolation only**.
- Do **not** claim the ensemble fix prevents station crossing. If a multi-feature ensemble file
  ever appears, `name` is discarded before the fill, so member-grouping alone would not prevent
  cross-feature bleed. That is **PREPG-011's** problem, not this one's.

Control and ERA5 have no such ambiguity — `code` is already the real station
(`dg_utils.py:198`). **Fix those two first; they are the same code twice.**

## The fix, and what it deliberately does NOT change

**Group each fill by its key. Nothing else.** *Out-of-loop review 2026-08-18 cut this issue's
scope substantially; the removed items are listed under § Deliberately out of scope.*

| Site | Group by |
|---|---|
| control (`Quantile_Mapping_OP.py:763`, `:768`) | `code` |
| ERA5 (`get_era5_reanalysis_data.py:183`, `:188`) | `code` |
| ensemble (`Quantile_Mapping_OP.py:902`, `:907`) | `ensemble_member` |

**Match today's behaviour minus the cross-group bleed** — do not open a gap-policy debate:

- **Leading** gap in a group: remains NaN (there is nothing within the group to carry forward).
  This is the only intended behaviour change.
- **Interior and trailing** gaps: keep the current unbounded `ffill` within the group.

Bounded fills, max-run limits and configurable gap policy are a **separate** question. Deferring
them costs nothing and keeps this fix reviewable.

## Acceptance criteria

- **Control and ERA5** fills are grouped by `code`. Fixture: two stations stacked in one frame,
  station 2 beginning with a gap, which must **not** inherit station 1's last value.
- **Ensemble** fill is grouped by `ensemble_member`. Fixture: two members, second beginning with
  a gap. (Member isolation only — see above.)
- A frame with **no NaNs** is unchanged, rows and order identical. The existing guards already
  skip the fill entirely in that case (`Quantile_Mapping_OP.py:760`), and a grouped assignment on
  the value column preserves row order — **no grouped sort is needed**.
- No rows are dropped.
- `SAPPHIRE_TEST_ENV=True bash run_tests.sh preprocessing_gateway` green.

## Deliberately out of scope

Cut on review as disproportionate to a three-call-site fix. Recorded so they are not lost:

- Gap-policy design (leading/interior/trailing/max-run) and its four-way test matrix.
- WARNING-level gap logging naming variable/station/member/date-range/run-length, and any
  pre-fix NaN-position instrumentation. The existing notice is adequate here.
- Making the control and ERA5 log strings distinguishable. They are currently **identical**
  (`Quantile_Mapping_OP.py:761`/`:766` vs `get_era5_reanalysis_data.py:181`/`:186`), which made
  attributing the 31 log firings need script-section context. A genuine papercut, but not this
  issue's job.

## Contract not to break

- **Do not drop NaN rows.** Shortening one station or member relative to others changes ensemble
  geometry for the conceptual model.
- The three paths have **different keys** — control and ERA5 have no member dimension.
- Do not add a sort. Row order must survive.

## Downstream effect of the fix — a real semantics change

Values that are today fabricated from another station or member will, after the fix, **remain
NaN**. Verified: they are **not dropped** anywhere downstream.

- Control: written to CSV and sent to the API as `value=None`; the schema permits null
  (`Quantile_Mapping_OP.py:770`, `:351`).
- ERA5: written to CSV; the later extension deduplicates and sorts but does not drop NaNs
  (`extend_era5_reanalysis.py:482`).
- Ensemble: CSVs are read directly by the conceptual model with no NA removal
  (`conceptual_model/functions/functions_operational.R:477`).

**So nulls can propagate into model forcing where a (wrong) number used to sit.** That is the
correct trade — an honest gap beats silent cross-station fabrication — but it is a behaviour
change the owner should sign off on, not a pure bug fix.
