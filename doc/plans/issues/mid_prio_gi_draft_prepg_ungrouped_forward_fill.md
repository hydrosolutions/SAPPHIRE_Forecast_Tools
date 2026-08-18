## Whole-frame `ffill` on three forcing paths carries values across station, and on one path ensemble-member, boundaries (PREPG-013)

**Status**: Draft (2026-08-18) — split out of PREPG-011, which conflated it with an identity defect
**Module**: `apps/preprocessing_gateway` (`Quantile_Mapping_OP.py`, `get_era5_reanalysis_data.py`)
**Priority**: **Medium** — fabricates forcing values silently, but only where gaps occur; the log
line it emits reads like normal operation.
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

| Site | Path | Boundaries crossed |
|---|---|---|
| `Quantile_Mapping_OP.py:899`, `:904` | ensemble | station **and** ensemble member |
| `Quantile_Mapping_OP.py:757` | control member | station |
| `get_era5_reanalysis_data.py:179` | ERA5 reanalysis | station |

Rows are ordered by identity before the merge (`Quantile_Mapping_OP.py:225`), so a **leading**
gap in one group inherits the previous group's last value. Ensemble members are independent
realisations and stations are different catchments — this is fabrication, not interpolation.

## Why the ensemble path cannot be fixed alone

The obvious fix, `groupby(["code", "ensemble_member"])`, **does not work on the ensemble path
today**: `code` is a file-level constant there (see **PREPG-011**), so that grouping still spans
stations. The ensemble site must be keyed on the *resolved station identity*, which only exists
once PREPG-011 lands.

The control and ERA5 sites have no such problem — `code` is already the real station
(`dg_utils.py:198`) — so they can be fixed independently and immediately.

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

- Each fill is grouped by its path's real key: resolved station **+ member** (ensemble), station
  (control, ERA5).
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
