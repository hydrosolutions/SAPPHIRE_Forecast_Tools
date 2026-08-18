## Ensemble transform writes the file-level HRU code into `code`, discarding the per-station hydropost code the contract puts in `name` (PREPG-011)

**Status**: Draft (2026-08-18) — **third framing; the first two were wrong**
**Module**: `apps/preprocessing_gateway` (`Quantile_Mapping_OP.py`, `dg_utils.py`)
**Priority**: **Low — latent.** *Resolved 2026-08-18 by the Phase-0 inventory below:* every
ensemble file on every current deployment is **single-feature**, so nothing is mis-attributed
today. This remains a genuine contract violation that would corrupt silently the moment a
multi-feature ensemble shapefile is configured — fix it before that happens, not urgently.
**Labels**: `preprocessing_gateway`, `data-integrity`, `ensemble`, `silent-corruption`
**Found**: 2026-08-18. The first draft blamed an ungrouped `ffill`; the second called this "band
identity". Both were wrong. The correct framing comes from the **documented contract in
`doc/development.md`**, which neither earlier draft consulted.
**Related**: **PREPG-012 depends on this** — bundle selection cannot be correct until identity is.
PREPG-013 (ungrouped fills — split out of this issue). PREPG-010 (same path, transport).

---

## The documented contract

`doc/development.md:382`, on the shapefile uploaded to the Data Gateway:

> *"The `name` attribute should contain the code corresponding to the gauge station (also called
> hydropost) identifier … The shape file can have one or multiple features (polygons) and the
> code attribute should be unique for each feature."*

and `doc/development.md:521`, the transformed output format:

```
| date       | P    | code  | ensemble_member |
```

**`name` is the per-station hydropost code, and it is supposed to become the output `code`.**
Multiple features per file is explicitly supported, each carrying its own unique code. There is
no open question here about aggregation — the contract is already settled.

## Root cause — the two identifiers are swapped

`transform_data_file_ensemble_member` emits one row per (date, source column) but assigns the
**file-level** HRU code to every one of them (`Quantile_Mapping_OP.py:167-169`):

```python
code_data["code"] = HRU_CODE     # ← the FILE's identifier: identical for every feature
code_data["name"] = name         # ← the per-hydropost code the contract says belongs in `code`
```

The value that should populate `code` is parked in `name`; `code` receives a file-level constant.

## The damage starts immediately, not at the later drop

1. **Coefficient selection uses the wrong identity.** `dg_utils.py:123` groups and selects
   quantile-mapping coefficients by the **constant** `code`, ignoring `name` — so one station's
   coefficients are applied to every station in the file.
2. **The whole frame's `code` is then overwritten** from `code_ens` (`Quantile_Mapping_OP.py:865`)
   rather than each source name being mapped to its own station code.
3. **`name` is finally discarded** on both downstream paths:
   ```
   dg_utils.py:158-159             era5_data[["date","P","code","ensemble_member"]]              (quantile-mapping path)
   Quantile_Mapping_OP.py:895-896  combined_ensemble_forecast[["date","P","code","ensemble_member"]]  (bypass path)
   ```
   After which `(date, code, ensemble_member)` is **not a unique key** when a file has more than
   one feature.

A fix that only preserves `name` downstream leaves (1) and (2) intact. **Normalise the identity
at the transform**, then let coefficient selection and the projection follow from it.

## Impact — measured, not inferred

*An earlier draft asserted High severity; a later one called it conditional. The inventory has
now settled it as **latent**.* The documented shapefile supports multiple features, but no
deployment currently uses one.

> *Withdrawn:* the earlier claim that the fixture's `band_1000` / `band_2000`
> (`test/test_ensemble_transforms.py:38`) proved "multi-band input is a supported, tested shape".
> That fixture tests only the transform, uses invented names, and explicitly locks in the constant
> `code`. It is not evidence of a production shape.

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

**Verdict for this issue: dormant.** All 200 kyg ensemble files carry exactly one data column;
taj has none present. The constant `HRU_CODE` is therefore the *only* identity in each file, so
it is trivially correct today. The bug is a loaded gun, not a firing one.

**What would arm it:** subscribing a multi-feature shapefile for an *ensemble* HRU. Since
`doc/development.md:382` explicitly documents multi-feature shapefiles as supported, an operator
could do this at any time and get silently wrong forcing with no error. That is the argument for
fixing it despite the Low priority.

**Note the fill split:** the 8 ensemble-path `ffill` firings in the logs (§ PREPG-013) are on
these single-column files, so they cross **member** boundaries only, never station boundaries.

## A coordinated consumer change is required

Correcting `code` upstream is necessary but not sufficient. The conceptual consumer filters only
by `ensemble_member`, **not** by station (`conceptual_model/functions/functions_operational.R:492`).
If one output file may legitimately carry several stations, that consumer must filter by station
too — otherwise the ambiguity simply moves downstream.

Also withdrawn from the first draft: *"49 of 50 is a usable ensemble"*. Not an existing contract —
the consumer iterates its configured member vector and builds an entry even when filtering returns
no rows (`:492`), then iterates the full list during execution (`:296`). Dropping a member requires
a coordinated consumer change.

## Correcting the consumer claim

The first draft said this "feeds ML and conceptual forecasts". **False for this path.** ML
operational code reads the control/reanalysis forcing (`machine_learning/make_forecast.py:404`);
these ensemble CSVs are consumed by the **conceptual model**
(`conceptual_model/run_operation_forecasting_CM.R:181`).

## Not this issue: the ungrouped fills

The first two drafts folded the ungrouped `ffill` into this one. They are **different defects**:
the control-member transform already derives `code` from each source station column
(`dg_utils.py:198`) and so has **no** identity defect, yet still fills across station boundaries.
The three fill sites are filed separately as **PREPG-013**.

## Acceptance criteria

- The DG source `name` is mapped to the output `code` per feature; the file-level HRU code no
  longer overwrites it.
- Quantile-mapping coefficients are selected by the **mapped station identity**, not by the
  file-level code — proven by a fixture in which two stations in one file have *different*
  coefficients and receive them correctly.
- **Uniqueness is validated before writing**: `(date, code, ensemble_member)` is asserted unique,
  and a violation fails loudly rather than being silently written.
- A **two-station-in-one-file fixture** (the production-shaped case) proves no value is attributed
  to the wrong station. *The earlier draft's two-code test would have passed while the bug
  persisted.*
- Single-feature files produce **semantically equal** output to today (same rows, same values) —
  not byte-identical, since identity normalisation may change ordering.
- `SAPPHIRE_TEST_ENV=True bash run_tests.sh preprocessing_gateway` green.

## Contract not to break

- The control-member path already assigns `code` correctly (`dg_utils.py:198`) — do not "fix" it
  into the ensemble path's shape.
- Do not drop rows to force uniqueness; that changes ensemble geometry for the conceptual model.
- Any behaviour change must be **loud**. This defect is invisible today precisely because nothing
  validates the key.
