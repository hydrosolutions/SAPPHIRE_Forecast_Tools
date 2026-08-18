## Forcing gaps are carried forward instead of interpolated, and the fill is ungrouped so it *could* cross station boundaries (PREPG-013)

**Status**: Draft (2026-08-18) — rewritten after measuring the actual gap shape
**Module**: `apps/preprocessing_gateway` (`Quantile_Mapping_OP.py`, `get_era5_reanalysis_data.py`)
**Priority**: **Medium.** *Lowered from High on 2026-08-18 — see § Severity history.* Fill
quality is the defect **observed in the frames inspected**; the cross-station bleed is structurally
real but was not present in those two samples. Which one dominates across all firings is unknown.
**Labels**: `preprocessing_gateway`, `data-quality`, `forcing`
**Found**: 2026-08-18, out-of-loop review; gap shape measured the same day.
**Related**: PREPG-011 (ensemble station identity — relevant only if a multi-feature ensemble file
ever appears; not a blocker here). PREPG-010 (same module, transport).

---

## Two defects; which one fires on any given run is **not** established

**(A) OBSERVED — a one-step temperature gap is carried forward instead of interpolated.**
Every gap measured on both deployments is a single *interior* step with a valid value on each
side. `ffill` copies the earlier neighbour; interpolating between the two neighbours that both
exist is the **preferred operational fallback for an isolated interior gap** — not an
unqualified improvement. See § The domain caveat.

**(B) NOT OBSERVED — the fill is ungrouped, so a *leading* gap would inherit the previous
station's last value.** Structurally real (§ Why it could bleed). **Zero leading gaps in the two
frames inspected** — which is two one-day samples, not a statement about the other 30 firings,
whose frames no longer exist. Do **not** read this as "the bleed has never happened".

Fixing both is one small change. Do not let (B)'s severity justify (A)'s scope, or vice versa.

## Measured gap shape — 2026-08-17 operational downloads

Raw control-member files from the `kyg` and `taj` data repos. Counts only; no station
identifiers reproduced.

| | kyg | taj |
|---|---|---|
| Stations (P + T columns per station) | **63** | **17** |
| Data rows | 371 | 371 |
| Empty data cells | 63 (0.134%) | 17 |
| **Leading / interior / trailing** | **0 / 63 / 0** | **0 / 17 / 0** |
| Sensor affected | **all T** | **all T** |
| Precipitation gaps | **zero** | **zero** |

On kyg all 63 gaps fall in a **single row** (row 80 of 371) — one timestamp where temperature is
missing for every station at once. That synchronisation **points at** a source-side outage rather
than per-station dropout, but the cause is inferred, not established.

> **Correction:** an earlier revision said 127 stations (kyg) and 35 (taj). Wrong — those were
> field counts. Each station contributes **two** columns (P and T), so 128 fields = 1 date +
> 63 P + 63 T + a `Source` column, which the transform drops (`dg_utils.py:202`).

**Limit of this evidence:** two files from one day. The fill has fired on 16 distinct run dates
(§ It fires in production) whose frames cannot be inspected, so "interior-only" describes **these
two samples only**. A leading gap on another date is entirely possible and is exactly what
grouping guards against.

## Why it could bleed (defect B)

All three frames are **long** — stations stacked, not one column per station. A wide frame would
have `ffill()` fill each station's column independently and be harmless.

```python
P_data = transformed_data_file[["date", "P", "code"]].copy()   # :754 — stacked
...
P_data = P_data.ffill()                                        # :763 — no grouping
```

`transform_data_file_control_member` concatenates complete station blocks one after another
(`dg_utils.py:215`) after sorting by date within each (`:194`), so station 2's first row directly
follows station 1's last. A gap at that boundary would inherit across it. `do_quantile_mapping()`
returns long frames too (`dg_utils.py:157`), so the fill is ungrouped on both branches.

## The three sites

| Fill site | Path | Frame | Group by | Stations/frame |
|---|---|---|---|---|
| `Quantile_Mapping_OP.py:763`, `:768` | control member | long (`:754-755`) | `code` | 63 (kyg), 17 (taj) |
| `get_era5_reanalysis_data.py:183`, `:188` | ERA5 reanalysis | long (`:176-177`) | `code` | not inventoried |
| `Quantile_Mapping_OP.py:902`, `:907` | ensemble | long (`:895`) | `ensemble_member` | 1 — see below |

**The ERA5 site is character-identical to the control site** — same projection, same ungrouped
fill, same log string. Fix them together; it is the same code twice. It has **not** been observed
firing.

Grouping the ensemble fill by `ensemble_member` is sufficient **only under the single-feature
assumption**, which rests on an external data inventory this repository cannot verify. Test member
isolation only; do **not** claim it prevents station crossing. If a multi-feature ensemble file
ever appears, `name` is discarded before the fill — that is **PREPG-011's** problem.

## It fires in production

`apps/logs`, **32 occurrences across 16 distinct run dates** (2026-02-13 → **2026-08-18**;
re-counted 2026-08-18, a run that day added one more control-T occurrence):

| Log line | Path | Count |
|---|---|---|
| `Nan values in T data for HRU …` | control | 19 |
| `Nan values in P data for HRU …` | control | 5 |
| `Nan values in P data (ensemble) …` | ensemble | 8 |

> **How that attribution was established.** `Quantile_Mapping_OP.py:761`/`:766` and
> `get_era5_reanalysis_data.py:181`/`:186` print the **same string**, so the line alone is
> ambiguous between control and ERA5. The split comes from the enclosing script section in each
> `run_locally` log, which resolves all 31 to `Quantile_Mapping_OP` and **none** to ERA5.

Note this counts firings, not boundary crossings. **The gap shape of these firings is unknown** —
only two frames (both 2026-08-17) were inspected, and they were interior. Attributing the other
firings to defect (A) rather than (B) would be inference, not measurement.

## The domain caveat on interpolation

Interpolation is the better default here, but it is a judgement, not a certainty:

- Temperature drives PET in the conceptual model (`functions_operational.R:467`), so the value
  matters downstream.
- Interpolation **smooths fronts and extremes**. Near 0 °C that can shift snow/rain partitioning
  or melt thresholds — precisely where a smoothed value is least welcome.
- The synchronised all-station gap points at a **source-side outage**. Filling it more smoothly
  makes the symptom less visible. The existing notice (`Quantile_Mapping_OP.py:766-767`) means it is
  not fully hidden, which is one more reason not to remove that notice.

The magnitude of the difference cannot be established from this repository. If that matters, it
needs a comparison run, not more code reading.

## The fix

**Group every fill by the key in the table above — including precipitation.** Grouping is pure
bleed-insurance and is correct for all variables. Then, for **temperature only**, interpolate
interior gaps within the group.

### The pandas details matter here — get them right

*Corrected 2026-08-18 after review; the first version of this section was wrong.*
Locked pandas is **2.3.3** (`apps/preprocessing_gateway/uv.lock:242`).

- **`SeriesGroupBy` has no `.interpolate()` method.** Use a same-indexed
  `transform(lambda s: s.interpolate(...))`.
- **Use `transform`, not `apply`.** Control concatenation preserves **duplicate index values**
  (`dg_utils.py:215`), so `groupby.apply` risks a MultiIndex/reindex hazard on assignment back.
  `transform` returns a same-indexed result and is plainly safe. **No additional sort is needed
  for control/ERA5** — they are date-sorted before station blocks are concatenated
  (`dg_utils.py:194`, concatenated at `:215`).
  For the **ensemble** path this holds only under the single-feature assumption: the sort is
  `code, name, ensemble_member, date` (`Quantile_Mapping_OP.py:226-227`), so with multiple `name`
  values the dates restart *within* each member once `name` is dropped (`:895`).
- **Default interpolation is positional linear, not datetime-aware.** With regularly spaced rows
  that is fine; do not assume it honours the date column.
- **Leading NaNs are left unfilled** by default — which is the behaviour this issue wants.
- **`limit=N` caps the number of filled cells; it does not reject a longer outage.** A two-step
  gap under `limit=1` fills one cell and leaves the other NaN — it does *not* skip the gap
  wholesale. An earlier draft implied otherwise.
- **Default interpolation also forward-fills trailing NaNs** up to the limit. If trailing
  behaviour is meant to be normative, pin it with a fixture; otherwise do not claim it.

### Precipitation: group it, but leave its semantics alone

Zero P gaps were observed on either deployment, and interpolating rainfall is not the right
default — it is intermittent and non-negative.

*Correction:* an earlier draft said `ffill` on precipitation "invents a rain event". Too strong.
It **can duplicate or extend** one: it repeats the previous amount, fabricating rainfall when that
amount is non-zero and copying zero after a dry step — the latter is not harmless either, since it
can turn an actual rain step into a dry one.

Note also that the 5 recorded P-path firings do **not** prove a P *value* was missing — the guard
tests the whole frame (`Quantile_Mapping_OP.py:760`), so any null column would trip it.

Changing precipitation fill semantics needs its own evidence. Group it now; decide later.

## Acceptance criteria

- Control and ERA5 fills are grouped by `code`; the ensemble fill by `ensemble_member`.
- Fixture: two stations stacked in one frame, station 2 starting with a gap — that leading cell
  must **remain NaN**.
  *Asserting merely "not equal to station 1's last value" is insufficient:* ungrouped
  **interpolation** would fill it with a midpoint, which is unequal to station 1's last value yet
  still crosses the boundary. The test would pass while the defect survived.
- Fixture: a single interior gap with valid neighbours is **interpolated**, not carried forward —
  assert the explicit expectation `[1, NaN, 3] → [1, 2, 3]`.
- Fixture: **two ensemble members**, the second starting with a gap — that leading cell must
  **remain NaN**, for the same reason. (Defect B is otherwise pinned only on control/ERA5.)
- Precipitation is **grouped** but its fill semantics are unchanged.
- A frame with **no NaNs** is unchanged, rows and order identical. The existing guards already
  skip the fill entirely in that case (`Quantile_Mapping_OP.py:760`), and a grouped assignment on
  the value column preserves row order — **no grouped sort is needed**.
- No rows are dropped.
- `SAPPHIRE_TEST_ENV=True bash run_tests.sh preprocessing_gateway` green.

## Deliberately out of scope

Cut on review as disproportionate to a three-call-site fix; recorded so they are not lost:

- A general gap-policy framework (configurable strategies, max-run limits, per-variable rules).
- WARNING-level gap logging naming variable/station/member/date-range/run-length, and pre-fix
  NaN-position instrumentation. The existing notice is adequate.
- Making the control and ERA5 log strings distinguishable — a genuine papercut, not this job.
- Precipitation **fill semantics** (P is still grouped — see § The fix).

## Contract not to break

- **Do not drop NaN rows.** Shortening one station or member relative to others changes ensemble
  geometry for the conceptual model.
- The three paths have **different keys** — control and ERA5 have no member dimension.
- Do not add a sort. Row order must survive.

## Downstream effect

Interpolating interior gaps keeps a value where one exists today, so the earlier concern about
nulls propagating into model forcing **largely disappears** — it now applies only to leading gaps,
of which none have been observed.

For completeness, if a leading gap does occur its NaN is **not dropped** anywhere: control writes
to CSV and sends the API `value=None` (`Quantile_Mapping_OP.py:355-357`), which the schema
permits (`sapphire/services/preprocessing/app/schemas.py:81`); ERA5's later extension deduplicates and sorts without dropping NaNs
(`extend_era5_reanalysis.py:483-484`); and the conceptual model reads ensemble CSVs with no NA removal
(`functions_operational.R:477-499`). An honest gap there beats cross-station fabrication.

## Severity history

Three changes, each on better evidence — recorded so the churn is auditable:

1. **Medium** at filing (split out of PREPG-011).
2. **→ High**, on finding the control frames are multi-station and the fill had fired on 15 run
   dates.
3. **→ Medium**, on measuring the gaps: both inspected frames are interior-only, so the bleed the
   High rating assumed was **not demonstrated**. That is not the same as "has not occurred" — 30
   of the 32 firings happened on frames that no longer exist.
