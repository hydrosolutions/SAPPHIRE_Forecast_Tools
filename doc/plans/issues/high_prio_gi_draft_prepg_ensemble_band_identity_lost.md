## Ensemble band identity (`name`) is dropped, making `(date, code, ensemble_member)` non-unique — and an ungrouped `ffill` then mixes bands and members (PREPG-011)

**Status**: Draft (2026-08-18) — **rewritten after out-of-loop review; the earlier framing was wrong**
**Module**: `apps/preprocessing_gateway` (`Quantile_Mapping_OP.py`, `dg_utils.py`,
`get_era5_reanalysis_data.py`)
**Priority**: **High** — silently produces forcing values that belong to a different band or
ensemble member, with no error.
**Labels**: `preprocessing_gateway`, `data-integrity`, `ensemble`, `silent-corruption`
**Found**: 2026-08-18 (out-of-loop `codex exec`); **root cause established by direct code
verification** in the same session, which corrected the first draft.
**Related**: PREPG-012 (same code path, parameter-bundle selection). PREPG-010 (same path,
transport handling — unrelated mechanism).

---

## Root cause — identity loss, not the fill

`transform_data_file_ensemble_member` emits **one row per (date, band)** and assigns a
**constant** `code` to every band in the file (`Quantile_Mapping_OP.py:167-169`):

```python
code_data["code"] = HRU_CODE     # ← the FILE's HRU code: identical for every band
code_data["name"] = name         # ← the actual per-band identity
```

The source comment is explicit: *"unique names, here they are actually the names of the
different HRU"*.

`name` is then **discarded** on both downstream paths:

```
dg_utils.py:158-159                era5_data[["date","P","code","ensemble_member"]]   (quantile-mapping path)
Quantile_Mapping_OP.py:895-896     combined_ensemble_forecast[["date","P","code","ensemble_member"]]  (bypass path)
```

**After that, `(date, code, ensemble_member)` is not a unique key** whenever a DG file contains
more than one band. This is not hypothetical: the unit-test fixture is built with **two bands
under one code** (`band_1000`, `band_2000` — `test/test_ensemble_transforms.py`), so multi-band
input is a supported, tested shape.

## The visible symptom — ungrouped forward fill

`Quantile_Mapping_OP.py:899` and `:904`:

```python
if P_ensemble.isnull().values.any():
    print(f"Nan values in P data (ensemble) for HRU {code_ens}")
    print("Take Last Observation")
    P_ensemble = P_ensemble.ffill()      # whole frame: crosses bands AND members
```

Because rows are ordered `code, name, ensemble_member, date` before the merge
(`Quantile_Mapping_OP.py:225`), a leading gap inherits the previous band's or previous member's
last value. Ensemble members are independent realisations and bands are different catchment
areas, so this is fabrication, not interpolation.

**Why the earlier draft's fix was wrong:** it proposed `groupby(["code", "ensemble_member"])`.
Since `code` is constant across bands, that grouping **still crosses band boundaries**. Any fix
that does not preserve `name` (or resolve the band contract first) does not fix this.

## The same ungrouped fill exists on two more paths

Scoping this to the ensemble path was an omission. The identical whole-frame fill appears at:

- `Quantile_Mapping_OP.py:757` — the **control-member** path (across multiple station codes)
- `get_era5_reanalysis_data.py:179` — the standalone **ERA5 reanalysis** path

Those do not have the ensemble-member dimension, but they do fill across station boundaries.
**Either cover all three or file explicit companions** — "inspect before assuming" is not an
adequate issue boundary.

## Correcting the consumer claim

The earlier draft said this "feeds ML and conceptual forecasts". **False for this path.** ML
operational code reads the control/reanalysis forcing (`machine_learning/make_forecast.py:604`);
these ensemble CSVs are consumed by the **conceptual model**
(`conceptual_model/run_operation_forecasting_CM.R:181`). It becomes an ML concern only if the
control/reanalysis fills above are included.

Also withdrawn: *"49 of 50 is a usable ensemble"*. That is **not an existing contract** — the
consumer iterates its configured member vector and builds an entry even when filtering returns
no rows (`conceptual_model/functions/functions_operational.R:492`), then iterates the full list
during execution (`:296`). **Dropping a member requires a coordinated consumer change.**

## The decision this turns on — what should bands become?

Before any fill fix, the band contract must be settled:

1. **Aggregate** — multiple bands combine (mean? area-weighted?) into one series per code. Then
   the aggregation must happen explicitly *before* `name` is dropped, and the resulting key is
   genuinely unique.
2. **Preserve** — bands stay distinct downstream, so `name` must survive into the output and the
   consumer must key on it.
3. **Single-band expected** — multi-band input is a misconfiguration, and the code should
   **fail loudly** rather than silently collapsing.

The code currently does none of these — it drops the identity and proceeds. Nothing downstream
detects a flat-lined or duplicated member: the conceptual consumer checks only that P/T files are
non-empty (`functions_operational.R:477`, `:492`).

## Acceptance criteria

- A **two-band fixture** (the production-shaped case, not two codes) proves no value crosses a
  band boundary. *The earlier draft's two-code test would have passed while the bug persisted.*
- No value crosses an `ensemble_member` boundary.
- Whatever policy is chosen for gaps is logged at **WARNING**, naming variable, band, member,
  dates and gap length — not the current benign-looking `Take Last Observation`.
- Leading, interior, trailing and over-limit gaps are tested independently.
- The output key is provably unique, or `name` is retained.
- Assertions are **semantic and ordering-based, not "byte-identical"** — any added sort can
  change bytes on a no-NaN run.
- `SAPPHIRE_TEST_ENV=True bash run_tests.sh preprocessing_gateway` green.

## Contract not to break

- Do **not** drop NaN rows wholesale — that shortens some members relative to others and changes
  ensemble geometry for the conceptual model.
- Do **not** apply one fix blindly to the control/reanalysis paths; they have a different key
  (`code`, no member dimension).
- Whatever replaces the fill must stay **loud**. This defect is invisible today precisely because
  its log line reads like normal operation.
