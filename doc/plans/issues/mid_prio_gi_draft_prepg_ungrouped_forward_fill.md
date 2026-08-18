## Forcing gaps are carried forward instead of interpolated, and the fill is ungrouped so it *could* cross station boundaries (PREPG-013)

**Status**: Draft (2026-08-18) — rewritten after measuring the actual gap shape
**Module**: `apps/preprocessing_gateway` (`Quantile_Mapping_OP.py`, `get_era5_reanalysis_data.py`)
**Priority**: **Medium.** *Lowered from High on 2026-08-18 — see § Severity history.* The
**observed** defect is fill quality, which happens on every affected run; the cross-station bleed
is **latent** and has not been observed.
**Labels**: `preprocessing_gateway`, `data-quality`, `forcing`
**Found**: 2026-08-18, out-of-loop review; gap shape measured the same day.
**Related**: PREPG-011 (ensemble station identity — relevant only if a multi-feature ensemble file
ever appears; not a blocker here). PREPG-010 (same module, transport).

---

## Two defects, and only one of them is happening

**(A) OBSERVED — a one-step temperature gap is carried forward instead of interpolated.**
Every gap measured on both deployments is a single *interior* step with a valid value on each
side. `ffill` copies the earlier neighbour; the obviously better answer is to interpolate between
the two neighbours that both exist. Small, systematic, and fires on every affected run.

**(B) LATENT — the fill is ungrouped, so a *leading* gap would inherit the previous station's
last value.** Structurally real (§ Why it could bleed), but **zero leading gaps were observed**,
so this has not manifested in the data inspected.

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
missing for every station at once. This is a source-side outage at one time step, not per-station
dropout.

> **Correction:** an earlier revision said 127 stations (kyg) and 35 (taj). Wrong — those were
> field counts. Each station contributes **two** columns (P and T), so 128 fields = 1 date +
> 63 P + 63 T.

**Limit of this evidence:** two files from one day. The fill has fired on 15 distinct run dates
(§ It fires in production) whose frames cannot be inspected, so "interior-only" is a strong signal
from two samples, not proof. A leading gap on some other date is possible and is exactly what
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
(`dg_utils.py:204`) after sorting by date within each (`:193`), so station 2's first row directly
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

`apps/logs`, 31 occurrences across 15 distinct run dates (2026-02-13 → **2026-08-17**):

| Log line | Path | Count |
|---|---|---|
| `Nan values in T data for HRU …` | control | 18 |
| `Nan values in P data for HRU …` | control | 5 |
| `Nan values in P data (ensemble) …` | ensemble | 8 |

> **How that attribution was established.** `Quantile_Mapping_OP.py:761`/`:766` and
> `get_era5_reanalysis_data.py:181`/`:186` print the **same string**, so the line alone is
> ambiguous between control and ERA5. The split comes from the enclosing script section in each
> `run_locally` log, which resolves all 31 to `Quantile_Mapping_OP` and **none** to ERA5.

Note this counts firings, not boundary crossings — and the measured gap shape says the control
firings were interior, i.e. defect (A), not (B).

## The fix

Per site, group by the key in the table above, then:

1. **Interpolate** short interior gaps within the group. This is what the observed data needs.
   Use a small `limit` so a long outage cannot be smeared over — matching today's practical
   behaviour without inheriting `ffill`'s unbounded reach.
2. **Leading** gaps have nothing to interpolate from and remain NaN. None observed; this is the
   only behaviour change, and it is the point of the fix.
3. **Trailing** gaps: keep current behaviour.

**Leave precipitation on its current path.** Zero P gaps were observed on either deployment, and
interpolating precipitation is *not* the right default — it is intermittent and non-negative, and
`ffill` on it invents a rain event. If P gaps ever appear they need their own answer (zero-fill or
climatology). Note the hazard; do not build for it now.

## Acceptance criteria

- Control and ERA5 fills are grouped by `code`; the ensemble fill by `ensemble_member`.
- Fixture: two stations stacked in one frame, station 2 starting with a gap, which must **not**
  inherit station 1's last value.
- Fixture: a single interior gap with valid neighbours is **interpolated**, not carried forward.
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
- Precipitation gap handling (see above).

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
to CSV and sends the API `value=None`, which the schema permits (`Quantile_Mapping_OP.py:770`,
`:351`); ERA5's later extension deduplicates and sorts without dropping NaNs
(`extend_era5_reanalysis.py:482`); and the conceptual model reads ensemble CSVs with no NA removal
(`functions_operational.R:477`). An honest gap there beats cross-station fabrication.

## Severity history

Three changes, each on better evidence — recorded so the churn is auditable:

1. **Medium** at filing (split out of PREPG-011).
2. **→ High**, on finding the control frames are multi-station and the fill had fired on 15 run
   dates.
3. **→ Medium**, on measuring the gaps: all interior, so the bleed the High rating was about has
   **not** occurred. What remains is a real but modest fill-quality defect plus a latent bleed.
